// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

//! Implementation for the `flight/types` interface.

use std::collections::HashMap;
use std::io::Write;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex};

use crate::bindings::flight::types::ErrorCode;
use crate::ctx::FlightCtx;
pub use crate::ctx::{FlightImpl, FlightView};
use crate::error::{FlightError, FlightResult};
use crate::types::{
    FlightClientState, FlightClientStateH2, FlightClientStateH3, HostFlightClient,
    HostFlightIncomingPutResponse, HostFlightIncomingResponse,
};
use arrow::datatypes::ToByteSlice;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor};
use bytes::Bytes;
use core::task::Poll;
use futures::{FutureExt, StreamExt};
use http::Uri;
use tempfile::NamedTempFile;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Request;
#[cfg(feature = "tonic-h3")]
use tonic_h3::{
    msquic_async::h3_msquic_async::msquic, msquic_async::H3MsQuicAsyncConnector, H3Channel,
};
use wasmtime::component::*;
use wasmtime_wasi::p2::{subscribe, DynPollable};
use wasmtime_wasi::{async_trait, runtime::with_ambient_tokio_runtime};

static LAZY_SHARED_CLIENTS: LazyLock<Arc<Mutex<HashMap<FlightCtx, FlightServiceClient<Channel>>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

#[async_trait]
impl<T> crate::bindings::flight::client::Host for FlightImpl<T>
where
    T: FlightView,
{
    fn create_client(&mut self) -> FlightResult<Resource<HostFlightClient>> {
        #[cfg(feature = "tonic-h3")]
        let client = if self.ctx().use_h3 {
            HostFlightClient {
                state: FlightClientState::H3(FlightClientStateH3::Default),
                token: None,
                reg: None,
            }
        } else {
            HostFlightClient {
                state: FlightClientState::H2(FlightClientStateH2::Default),
                token: None,
                reg: None,
            }
        };
        #[cfg(not(feature = "tonic-h3"))]
        let client = HostFlightClient {
            state: FlightClientState::H2(FlightClientStateH2::Default),
            token: None,
        };

        Ok(self.table().push(client)?)
    }
}

#[async_trait]
impl<T> crate::bindings::flight::types::Host for FlightImpl<T>
where
    T: FlightView,
{
    fn convert_error_code(&mut self, error: FlightError) -> anyhow::Result<ErrorCode> {
        error.downcast()
    }
}

async fn do_handshake<T>(
    flight_client: &mut FlightServiceClient<T>,
    att_config: snpguest::report::AttestationConfig,
) -> tonic::Result<String>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: http_body::Body<Data = Bytes> + std::marker::Send + 'static,
    <T::ResponseBody as http_body::Body>::Error: Into<tonic::codegen::StdError> + std::marker::Send,
{
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let mut handshake_stream = flight_client
        .handshake(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await?
        .into_inner();

    // first-round
    let req = arrow_flight::HandshakeRequest {
        protocol_version: 1,
        payload: bytes::Bytes::new(),
    };
    tx.send(req).await.map_err(|e| {
        tonic::Status::internal(format!("failed to send handshake request: {:?}", e))
    })?;
    let res = handshake_stream
        .message()
        .await?
        .ok_or(tonic::Status::internal(
            "failed to receive handshake response",
        ))?;

    // second-round, generate attestation report and response
    let mut att_req = snpguest::report::ReportDirectArgs::default();
    att_req.proc_type = att_config.proc_type;
    att_req.endorsement = att_config.endorsement;
    att_req
        .request_data
        .copy_from_slice(&res.payload.to_byte_slice()[0..64]);
    let att = snpguest::report::get_report_direct(&att_req)
        .map_err(|e| tonic::Status::internal(format!("failed to get report: {:?}", e)))?;

    let ext_att_bin = bincode::serialize(&att).unwrap();
    let req = arrow_flight::HandshakeRequest {
        protocol_version: 1,
        payload: bytes::Bytes::copy_from_slice(&ext_att_bin),
    };
    tx.send(req).await.map_err(|e| {
        tonic::Status::internal(format!("failed to send handshake request: {:?}", e))
    })?;
    let res = handshake_stream
        .message()
        .await?
        .ok_or(tonic::Status::internal(
            "failed to receive handshake response",
        ))?;
    Ok(String::from_utf8_lossy(&res.payload).to_string())
}

async fn do_get<T>(
    flight_client: &mut FlightServiceClient<T>,
    ticket: Vec<u8>,
    token: Option<String>,
    jwt: Option<String>,
) -> tonic::Result<tonic::Response<tonic::Streaming<FlightData>>>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: http_body::Body<Data = Bytes> + std::marker::Send + 'static,
    <T::ResponseBody as http_body::Body>::Error: Into<tonic::codegen::StdError> + std::marker::Send,
{
    let ticket = arrow_flight::Ticket {
        ticket: Bytes::from(ticket),
    };
    let request = if let Some(jwt) = &jwt {
        let mut request = Request::new(ticket);
        let token = format!("Bearer {}", jwt);
        match MetadataValue::from_str(&token)
            .map_err(|e| tonic::Status::internal(format!("invalid jwt: {:?}", e)))
        {
            Ok(token) => {
                request.metadata_mut().insert("authorization", token);
                Ok(request)
            }
            Err(e) => Err(e),
        }
    } else {
        Ok(Request::new(ticket))
    }
    .and_then(|mut req| {
        if let Some(token) = token {
            let token = format!("Bearer {}", token);
            match MetadataValue::from_str(&token)
                .map_err(|e| tonic::Status::internal(format!("invalid jwt: {:?}", e)))
            {
                Ok(token) => {
                    req.metadata_mut().insert("x-yak-authorization", token);
                    Ok(req)
                }
                Err(e) => Err(e),
            }
        } else {
            Ok(req)
        }
    });
    match request {
        Err(err) => Err(err),
        Ok(request) => flight_client.do_get(request).await,
    }
}

async fn do_put<T>(
    flight_client: &mut FlightServiceClient<T>,
    data_rx: tokio::sync::mpsc::Receiver<FlightData>,
    token: Option<String>,
    jwt: Option<String>,
) -> tonic::Result<tonic::Response<tonic::Streaming<arrow_flight::PutResult>>>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: http_body::Body<Data = Bytes> + std::marker::Send + 'static,
    <T::ResponseBody as http_body::Body>::Error: Into<tonic::codegen::StdError> + std::marker::Send,
{
    let stream = async_stream::stream! {
        let mut stream = ReceiverStream::new(data_rx);
        let mut flight_descriptor = Some(FlightDescriptor::new_path(vec!["save".to_string()]));
        while let Some(flight_data) = stream.next().await {
            yield match flight_descriptor.take() {
                Some(desc) => {
                    flight_data.with_descriptor(desc)
                },
                None => flight_data
            };
        }
    };
    let request = if let Some(jwt) = &jwt {
        let mut request = Request::new(stream);
        let token = format!("Bearer {}", jwt);
        match MetadataValue::from_str(&token)
            .map_err(|e| tonic::Status::internal(format!("invalid jwt: {:?}", e)))
        {
            Ok(token) => {
                request.metadata_mut().insert("authorization", token);
                Ok(request)
            }
            Err(e) => Err(e),
        }
    } else {
        Ok(Request::new(stream))
    }
    .and_then(|mut req| {
        if let Some(token) = token {
            let token = format!("Bearer {}", token);
            match MetadataValue::from_str(&token)
                .map_err(|e| tonic::Status::internal(format!("invalid jwt: {:?}", e)))
            {
                Ok(token) => {
                    req.metadata_mut().insert("x-yak-authorization", token);
                    Ok(req)
                }
                Err(e) => Err(e),
            }
        } else {
            Ok(req)
        }
    });
    match request {
        Err(err) => Err(err),
        Ok(request) => flight_client.do_put(request).await,
    }
}

#[cfg(feature = "tonic-h3")]
fn make_msquic_reg_and_config(
    client_pem: Option<(Vec<u8>, Vec<u8>)>,
    ca_cert_pem: Option<Vec<u8>>,
) -> anyhow::Result<(Arc<msquic::Registration>, Arc<msquic::Configuration>)> {
    let registration = msquic::Registration::new(&msquic::RegistrationConfig::default())?;
    let alpn = [msquic::BufferRef::from("h3")];
    let configuration = msquic::Configuration::open(
        &registration,
        &alpn,
        Some(
            &msquic::Settings::new()
                .set_IdleTimeoutMs(100000)
                .set_PeerBidiStreamCount(100)
                .set_PeerUnidiStreamCount(100)
                .set_DatagramReceiveEnabled()
                .set_StreamMultiReceiveEnabled(),
        ),
    )?;

    let cred_config = msquic::CredentialConfig::new_client();
    let (cred_config, _key_path, _cert_path) = if let Some((cert_pem, key_pem)) = client_pem {
        let mut cert_file = NamedTempFile::new()?;
        cert_file.write_all(&cert_pem)?;
        let cert_path = cert_file.into_temp_path();
        let cert_path_str = cert_path.to_string_lossy().into_owned();

        let mut key_file = NamedTempFile::new()?;
        key_file.write_all(&key_pem)?;
        let key_path = key_file.into_temp_path();
        let key_path_str = key_path.to_string_lossy().into_owned();

        (
            cred_config.set_credential(msquic::Credential::CertificateFile(
                msquic::CertificateFile::new(key_path_str, cert_path_str),
            )),
            Some(key_path),
            Some(cert_path),
        )
    } else {
        (cred_config, None, None)
    };

    let (cred_config, _ca_cert_path) = if let Some(ca_cert_pem) = ca_cert_pem {
        let mut ca_cert_file = NamedTempFile::new()?;
        ca_cert_file.write_all(&ca_cert_pem)?;
        let ca_cert_path = ca_cert_file.into_temp_path();
        let ca_cert_path_str = ca_cert_path.to_string_lossy().into_owned();
        (
            cred_config.set_ca_certificate_file(ca_cert_path_str),
            Some(ca_cert_path),
        )
    } else {
        (cred_config, None)
    };

    configuration.load_credential(&cred_config)?;
    Ok((Arc::new(registration), Arc::new(configuration)))
}

#[async_trait]
impl<T> crate::bindings::flight::types::HostClient for FlightImpl<T>
where
    T: FlightView,
{
    fn start_connect(&mut self, id: Resource<HostFlightClient>) -> FlightResult<()> {
        let ctx = self.ctx().clone();
        let client = self.table().get_mut(&id)?;
        match &client.state {
            FlightClientState::H2(FlightClientStateH2::Default) => {
                let shared_clients = LAZY_SHARED_CLIENTS.clone();
                let (tx, rx) = oneshot::channel();
                if let Some(flight_client) = shared_clients.lock().unwrap().get(&ctx) {
                    let flight_client = flight_client.clone();
                    with_ambient_tokio_runtime(move || {
                        let _ = tx.send(Ok(flight_client));
                    });
                    client.state = FlightClientState::H2(FlightClientStateH2::Connecting(rx));
                    return Ok(());
                }

                let server_url = ctx.server_url.clone();
                let use_tls = ctx.use_tls;
                let client_cert_pem = ctx.client_cert_pem.clone();
                let client_key_pem = ctx.client_key_pem.clone();
                let ca_cert_pem = ctx.ca_cert_pem.clone();
                with_ambient_tokio_runtime(move || {
                    tokio::spawn(async move {
                        let endpoint = Channel::builder(server_url.parse().unwrap());
                        let endpoint = if use_tls {
                            let tls_config = if let Some(ca_cert_pem) = ca_cert_pem {
                                ClientTlsConfig::new()
                                    .ca_certificate(Certificate::from_pem(&ca_cert_pem))
                            } else {
                                ClientTlsConfig::new().with_webpki_roots()
                            };
                            let tls_config = match (client_cert_pem, client_key_pem) {
                                (Some(cert_pem), Some(key_pem)) => {
                                    let identity =
                                        tonic::transport::Identity::from_pem(cert_pem, key_pem);
                                    tls_config.identity(identity)
                                }
                                (Some(_), None) | (None, Some(_)) => {
                                    let _ = tx.send(Err(tonic::Status::internal(
                                        "both client cert and key must be provided".to_string(),
                                    )
                                    .into()));
                                    return;
                                }
                                _ => tls_config,
                            };
                            match endpoint.tls_config(tls_config) {
                                Ok(endpoint) => endpoint,
                                Err(e) => {
                                    let _ = tx.send(Err(e.into()));
                                    return;
                                }
                            }
                        } else {
                            endpoint
                        };
                        match endpoint.connect().await {
                            Ok(channel) => {
                                let flight_client = FlightServiceClient::new(channel);
                                shared_clients
                                    .lock()
                                    .unwrap()
                                    .insert(ctx, flight_client.clone());
                                let _ = tx.send(Ok(flight_client));
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e.into()));
                            }
                        }
                    })
                });
                client.state = FlightClientState::H2(FlightClientStateH2::Connecting(rx));
                Ok(())
            }
            #[cfg(feature = "tonic-h3")]
            FlightClientState::H3(FlightClientStateH3::Default) => {
                let (tx, rx) = oneshot::channel();

                let server_url = ctx.server_url.clone();
                let client_cert_pem = ctx.client_cert_pem.clone();
                let client_key_pem = ctx.client_key_pem.clone();
                let ca_cert_pem = ctx.ca_cert_pem.clone();

                with_ambient_tokio_runtime(|| {
                    let client_pem = match (client_cert_pem, client_key_pem) {
                        (Some(cert_pem), Some(key_pem)) => Some((cert_pem, key_pem)),
                        (Some(_), None) | (None, Some(_)) => {
                            let _ = tx.send(Err(tonic::Status::internal(
                                "both client cert and key must be provided".to_string(),
                            )
                            .into()));
                            return;
                        }
                        _ => None,
                    };
                    let (reg, config) = match make_msquic_reg_and_config(client_pem, ca_cert_pem) {
                        Ok(res) => res,
                        Err(e) => {
                            let _ = tx.send(Err(tonic::Status::internal(format!(
                                "failed to create msquic config: {:?}",
                                e
                            ))
                            .into()));
                            return;
                        }
                    };
                    client.reg = Some(reg.clone());
                    let uri: Uri = match server_url.parse() {
                        Ok(uri) => uri,
                        Err(e) => {
                            let _ = tx.send(Err(tonic::Status::internal(format!(
                                "invalid server url: {:?}",
                                e
                            ))
                            .into()));
                            return;
                        }
                    };
                    let connector = H3MsQuicAsyncConnector::new(uri.clone(), config, reg);
                    let channel = H3Channel::new(connector, uri.clone());
                    let flight_client = FlightServiceClient::new(channel);
                    let _ = tx.send(Ok(flight_client));
                });
                client.state = FlightClientState::H3(FlightClientStateH3::Connecting(rx));
                Ok(())
            }
            _ => Err(ErrorCode::InvalidState.into()),
        }
    }

    fn finish_connect(&mut self, id: Resource<HostFlightClient>) -> FlightResult<()> {
        let client = self.table().get_mut(&id)?;
        match &client.state {
            FlightClientState::H2(..) => {
                let previous_state = std::mem::replace(
                    &mut client.state,
                    FlightClientState::H2(FlightClientStateH2::Closed),
                );
                let res = match previous_state {
                    FlightClientState::H2(FlightClientStateH2::ConnectReady(res)) => res,
                    FlightClientState::H2(FlightClientStateH2::Connecting(mut rx)) => {
                        let mut cx =
                            std::task::Context::from_waker(futures::task::noop_waker_ref());
                        match with_ambient_tokio_runtime(|| rx.poll_unpin(&mut cx)) {
                            Poll::Ready(res) => res,
                            Poll::Pending => {
                                client.state =
                                    FlightClientState::H2(FlightClientStateH2::Connecting(rx));
                                return Err(ErrorCode::WouldBlock.into());
                            }
                        }
                    }
                    previous_state => {
                        client.state = previous_state;
                        return Err(ErrorCode::NotInProgress.into());
                    }
                };
                match res {
                    Ok(Ok(flight_client)) => {
                        client.state =
                            FlightClientState::H2(FlightClientStateH2::Connected(flight_client));
                        Ok(())
                    }
                    Ok(Err(err)) => {
                        client.state = FlightClientState::H2(FlightClientStateH2::Closed);
                        Err(ErrorCode::ConnectionRefused(err.to_string()).into())
                    }
                    Err(err) => {
                        client.state = FlightClientState::H2(FlightClientStateH2::Closed);
                        Err(ErrorCode::InternalError(Some(format!("rx error: {:?}", err))).into())
                    }
                }
            }
            #[cfg(feature = "tonic-h3")]
            FlightClientState::H3(..) => {
                let previous_state = std::mem::replace(
                    &mut client.state,
                    FlightClientState::H3(FlightClientStateH3::Closed),
                );
                let res = match previous_state {
                    FlightClientState::H3(FlightClientStateH3::ConnectReady(res)) => res,
                    FlightClientState::H3(FlightClientStateH3::Connecting(mut rx)) => {
                        let mut cx =
                            std::task::Context::from_waker(futures::task::noop_waker_ref());
                        match with_ambient_tokio_runtime(|| rx.poll_unpin(&mut cx)) {
                            Poll::Ready(res) => res,
                            Poll::Pending => {
                                client.state =
                                    FlightClientState::H3(FlightClientStateH3::Connecting(rx));
                                return Err(ErrorCode::WouldBlock.into());
                            }
                        }
                    }
                    previous_state => {
                        client.state = previous_state;
                        return Err(ErrorCode::NotInProgress.into());
                    }
                };
                match res {
                    Ok(Ok(flight_client)) => {
                        client.state =
                            FlightClientState::H3(FlightClientStateH3::Connected(flight_client));
                        Ok(())
                    }
                    Ok(Err(err)) => {
                        client.state = FlightClientState::H3(FlightClientStateH3::Closed);
                        Err(ErrorCode::ConnectionRefused(err.to_string()).into())
                    }
                    Err(err) => {
                        client.state = FlightClientState::H3(FlightClientStateH3::Closed);
                        Err(ErrorCode::InternalError(Some(format!("rx error: {:?}", err))).into())
                    }
                }
            }
        }
    }

    fn start_handshake(&mut self, id: Resource<HostFlightClient>) -> FlightResult<()> {
        let att_config = self.ctx().attestation_config.clone();
        let client = self.table().get_mut(&id)?;
        match &client.state {
            FlightClientState::H2(state_h2) => {
                match state_h2 {
                    FlightClientStateH2::Connected(..) => {}
                    FlightClientStateH2::HandshakeProgress(..) => {
                        return Err(ErrorCode::InProgress.into());
                    }
                    _ => {
                        return Err(ErrorCode::InvalidState.into());
                    }
                }
                let FlightClientState::H2(FlightClientStateH2::Connected(mut flight_client)) =
                    std::mem::replace(
                        &mut client.state,
                        FlightClientState::H2(FlightClientStateH2::Closed),
                    )
                else {
                    unreachable!()
                };
                let (otx, orx) = oneshot::channel();
                with_ambient_tokio_runtime(|| {
                    tokio::spawn(async move {
                        let res = do_handshake(&mut flight_client, att_config).await;
                        let _ = otx.send((flight_client, res));
                    })
                });

                client.state = FlightClientState::H2(FlightClientStateH2::HandshakeProgress(orx));
                Ok(())
            }
            #[cfg(feature = "tonic-h3")]
            FlightClientState::H3(state_h3) => {
                match state_h3 {
                    FlightClientStateH3::Connected(..) => {}
                    FlightClientStateH3::HandshakeProgress(..) => {
                        return Err(ErrorCode::InProgress.into());
                    }
                    _ => {
                        return Err(ErrorCode::InvalidState.into());
                    }
                }
                let FlightClientState::H3(FlightClientStateH3::Connected(mut flight_client)) =
                    std::mem::replace(
                        &mut client.state,
                        FlightClientState::H3(FlightClientStateH3::Closed),
                    )
                else {
                    unreachable!()
                };
                let (otx, orx) = oneshot::channel();
                with_ambient_tokio_runtime(|| {
                    tokio::spawn(async move {
                        let res = do_handshake(&mut flight_client, att_config).await;
                        let _ = otx.send((flight_client, res));
                    })
                });

                client.state = FlightClientState::H3(FlightClientStateH3::HandshakeProgress(orx));

                Ok(())
            }
        }
    }

    fn finish_handshake(&mut self, id: Resource<HostFlightClient>) -> FlightResult<()> {
        let client = self.table().get_mut(&id)?;
        match &client.state {
            FlightClientState::H2(..) => {
                let previous_state = std::mem::replace(
                    &mut client.state,
                    FlightClientState::H2(FlightClientStateH2::Closed),
                );
                let res = match previous_state {
                    FlightClientState::H2(FlightClientStateH2::HandshakeReady(res)) => res,
                    FlightClientState::H2(FlightClientStateH2::HandshakeProgress(mut rx)) => {
                        let mut cx =
                            std::task::Context::from_waker(futures::task::noop_waker_ref());
                        match with_ambient_tokio_runtime(|| rx.poll_unpin(&mut cx)) {
                            Poll::Ready(res) => res,
                            Poll::Pending => {
                                client.state = FlightClientState::H2(
                                    FlightClientStateH2::HandshakeProgress(rx),
                                );
                                return Err(ErrorCode::WouldBlock.into());
                            }
                        }
                    }
                    previous_state => {
                        client.state = previous_state;
                        return Err(ErrorCode::NotInProgress.into());
                    }
                };
                match res {
                    Ok((flight_client, res)) => {
                        client.state =
                            FlightClientState::H2(FlightClientStateH2::Connected(flight_client));
                        match res {
                            Ok(token) => {
                                client.token = Some(token);
                                Ok(())
                            }
                            Err(err) => Err(ErrorCode::InternalError(Some(err.to_string())).into()),
                        }
                    }
                    Err(err) => {
                        client.state = FlightClientState::H2(FlightClientStateH2::Closed);
                        Err(ErrorCode::InternalError(Some(format!("rx error: {:?}", err))).into())
                    }
                }
            }
            #[cfg(feature = "tonic-h3")]
            FlightClientState::H3(..) => {
                let previous_state = std::mem::replace(
                    &mut client.state,
                    FlightClientState::H3(FlightClientStateH3::Closed),
                );
                let res = match previous_state {
                    FlightClientState::H3(FlightClientStateH3::HandshakeReady(res)) => res,
                    FlightClientState::H3(FlightClientStateH3::HandshakeProgress(mut rx)) => {
                        let mut cx =
                            std::task::Context::from_waker(futures::task::noop_waker_ref());
                        match with_ambient_tokio_runtime(|| rx.poll_unpin(&mut cx)) {
                            Poll::Ready(res) => res,
                            Poll::Pending => {
                                client.state = FlightClientState::H3(
                                    FlightClientStateH3::HandshakeProgress(rx),
                                );
                                return Err(ErrorCode::WouldBlock.into());
                            }
                        }
                    }
                    previous_state => {
                        client.state = previous_state;
                        return Err(ErrorCode::NotInProgress.into());
                    }
                };
                match res {
                    Ok((flight_client, res)) => {
                        client.state =
                            FlightClientState::H3(FlightClientStateH3::Connected(flight_client));
                        match res {
                            Ok(token) => {
                                client.token = Some(token);
                                Ok(())
                            }
                            Err(err) => Err(ErrorCode::InternalError(Some(err.to_string())).into()),
                        }
                    }
                    Err(err) => {
                        client.state = FlightClientState::H3(FlightClientStateH3::Closed);
                        Err(ErrorCode::InternalError(Some(format!("rx error: {:?}", err))).into())
                    }
                }
            }
        }
    }

    fn start_do_get(
        &mut self,
        id: Resource<HostFlightClient>,
        ticket: Vec<u8>,
    ) -> FlightResult<()> {
        let jwt = self.ctx().jwt.clone();
        let client = self.table().get_mut(&id)?;
        let token = client.token.clone();
        match &client.state {
            FlightClientState::H2(state_h2) => {
                match state_h2 {
                    FlightClientStateH2::Connected(..) => {}
                    FlightClientStateH2::DoGetProgress(..) => {
                        return Err(ErrorCode::InProgress.into());
                    }
                    _ => {
                        return Err(ErrorCode::InvalidState.into());
                    }
                }
                let FlightClientState::H2(FlightClientStateH2::Connected(mut flight_client)) =
                    std::mem::replace(
                        &mut client.state,
                        FlightClientState::H2(FlightClientStateH2::Closed),
                    )
                else {
                    unreachable!()
                };
                let (otx, orx) = oneshot::channel();
                with_ambient_tokio_runtime(|| {
                    tokio::spawn(async move {
                        let res = do_get(&mut flight_client, ticket, token, jwt).await;
                        let _ = otx.send((flight_client, res));
                    })
                });

                client.state = FlightClientState::H2(FlightClientStateH2::DoGetProgress(orx));
                Ok(())
            }
            #[cfg(feature = "tonic-h3")]
            FlightClientState::H3(state_h3) => {
                match state_h3 {
                    FlightClientStateH3::Connected(..) => {}
                    FlightClientStateH3::DoGetProgress(..) => {
                        return Err(ErrorCode::InProgress.into());
                    }
                    _ => {
                        return Err(ErrorCode::InvalidState.into());
                    }
                }
                let FlightClientState::H3(FlightClientStateH3::Connected(mut flight_client)) =
                    std::mem::replace(
                        &mut client.state,
                        FlightClientState::H3(FlightClientStateH3::Closed),
                    )
                else {
                    unreachable!()
                };
                let (otx, orx) = oneshot::channel();
                with_ambient_tokio_runtime(|| {
                    tokio::spawn(async move {
                        let res = do_get(&mut flight_client, ticket, token, jwt).await;
                        let _ = otx.send((flight_client, res));
                    })
                });

                client.state = FlightClientState::H3(FlightClientStateH3::DoGetProgress(orx));
                Ok(())
            }
        }
    }

    fn finish_do_get(
        &mut self,
        id: Resource<HostFlightClient>,
    ) -> FlightResult<Resource<HostFlightIncomingResponse>> {
        let client = self.table().get_mut(&id)?;
        match &client.state {
            FlightClientState::H2(..) => {
                let previous_state = std::mem::replace(
                    &mut client.state,
                    FlightClientState::H2(FlightClientStateH2::Closed),
                );
                let res = match previous_state {
                    FlightClientState::H2(FlightClientStateH2::DoGetReady(res)) => res,
                    FlightClientState::H2(FlightClientStateH2::DoGetProgress(mut rx)) => {
                        let mut cx =
                            std::task::Context::from_waker(futures::task::noop_waker_ref());
                        match with_ambient_tokio_runtime(|| rx.poll_unpin(&mut cx)) {
                            Poll::Ready(res) => res,
                            Poll::Pending => {
                                client.state =
                                    FlightClientState::H2(FlightClientStateH2::DoGetProgress(rx));
                                return Err(ErrorCode::WouldBlock.into());
                            }
                        }
                    }
                    previous_state => {
                        client.state = previous_state;
                        return Err(ErrorCode::NotInProgress.into());
                    }
                };
                match res {
                    Ok((flight_client, res)) => {
                        client.state =
                            FlightClientState::H2(FlightClientStateH2::Connected(flight_client));

                        match res {
                            Ok(response) => {
                                let stream = response.into_inner();
                                let incoming_response = HostFlightIncomingResponse {
                                    stream,
                                    pending_data: None,
                                };
                                Ok(self.table().push(incoming_response)?)
                            }
                            Err(err) => Err(ErrorCode::InternalError(Some(err.to_string())).into()),
                        }
                    }
                    Err(err) => {
                        client.state = FlightClientState::H2(FlightClientStateH2::Closed);
                        Err(ErrorCode::InternalError(Some(format!("rx error: {:?}", err))).into())
                    }
                }
            }
            #[cfg(feature = "tonic-h3")]
            FlightClientState::H3(..) => {
                let previous_state = std::mem::replace(
                    &mut client.state,
                    FlightClientState::H3(FlightClientStateH3::Closed),
                );
                let res = match previous_state {
                    FlightClientState::H3(FlightClientStateH3::DoGetReady(res)) => res,
                    FlightClientState::H3(FlightClientStateH3::DoGetProgress(mut rx)) => {
                        let mut cx =
                            std::task::Context::from_waker(futures::task::noop_waker_ref());
                        match with_ambient_tokio_runtime(|| rx.poll_unpin(&mut cx)) {
                            Poll::Ready(res) => res,
                            Poll::Pending => {
                                client.state =
                                    FlightClientState::H3(FlightClientStateH3::DoGetProgress(rx));
                                return Err(ErrorCode::WouldBlock.into());
                            }
                        }
                    }
                    previous_state => {
                        client.state = previous_state;
                        return Err(ErrorCode::NotInProgress.into());
                    }
                };
                match res {
                    Ok((flight_client, res)) => {
                        client.state =
                            FlightClientState::H3(FlightClientStateH3::Connected(flight_client));

                        match res {
                            Ok(response) => {
                                let stream = response.into_inner();
                                let incoming_response = HostFlightIncomingResponse {
                                    stream,
                                    pending_data: None,
                                };
                                Ok(self.table().push(incoming_response)?)
                            }
                            Err(err) => Err(ErrorCode::InternalError(Some(err.to_string())).into()),
                        }
                    }
                    Err(err) => {
                        client.state = FlightClientState::H3(FlightClientStateH3::Closed);
                        Err(ErrorCode::InternalError(Some(format!("rx error: {:?}", err))).into())
                    }
                }
            }
        }
    }

    fn start_do_put(&mut self, id: Resource<HostFlightClient>) -> FlightResult<()> {
        let jwt = self.ctx().jwt.clone();
        let client = self.table().get_mut(&id)?;
        let token = client.token.clone();
        match &client.state {
            FlightClientState::H2(state_h2) => {
                match state_h2 {
                    FlightClientStateH2::Connected(..) => {}
                    FlightClientStateH2::DoPutSendProgress(..) => {
                        return Err(ErrorCode::InProgress.into());
                    }
                    _ => {
                        return Err(ErrorCode::InvalidState.into());
                    }
                }
                let FlightClientState::H2(FlightClientStateH2::Connected(mut flight_client)) =
                    std::mem::replace(
                        &mut client.state,
                        FlightClientState::H2(FlightClientStateH2::Closed),
                    )
                else {
                    unreachable!()
                };
                let (data_tx, data_rx) = mpsc::channel::<FlightData>(100);
                let (otx, orx) = oneshot::channel();
                with_ambient_tokio_runtime(|| {
                    tokio::spawn(async move {
                        let res = do_put(&mut flight_client, data_rx, token, jwt).await;
                        let _ = otx.send((flight_client, res));
                    })
                });

                client.state =
                    FlightClientState::H2(FlightClientStateH2::DoPutSendProgress(data_tx, orx));
                Ok(())
            }
            #[cfg(feature = "tonic-h3")]
            FlightClientState::H3(state_h3) => {
                match state_h3 {
                    FlightClientStateH3::Connected(..) => {}
                    FlightClientStateH3::DoPutSendProgress(..) => {
                        return Err(ErrorCode::InProgress.into());
                    }
                    _ => {
                        return Err(ErrorCode::InvalidState.into());
                    }
                }
                let FlightClientState::H3(FlightClientStateH3::Connected(mut flight_client)) =
                    std::mem::replace(
                        &mut client.state,
                        FlightClientState::H3(FlightClientStateH3::Closed),
                    )
                else {
                    unreachable!()
                };
                let (data_tx, data_rx) = mpsc::channel::<FlightData>(100);
                let (otx, orx) = oneshot::channel();
                with_ambient_tokio_runtime(|| {
                    tokio::spawn(async move {
                        let res = do_put(&mut flight_client, data_rx, token, jwt).await;
                        let _ = otx.send((flight_client, res));
                    })
                });

                client.state =
                    FlightClientState::H3(FlightClientStateH3::DoPutSendProgress(data_tx, orx));
                Ok(())
            }
        }
    }

    fn do_put(
        &mut self,
        id: Resource<HostFlightClient>,
        data: Option<crate::bindings::flight::types::FlightData>,
        fin: bool,
    ) -> FlightResult<()> {
        if data.is_none() && !fin {
            return Err(ErrorCode::InvalidParameter.into());
        }
        let client = self.table().get_mut(&id)?;
        match &client.state {
            FlightClientState::H2(state_h2) => {
                match state_h2 {
                    FlightClientStateH2::DoPutSendProgress(..) => {}
                    _ => {
                        return Err(ErrorCode::InvalidState.into());
                    }
                }
                let FlightClientState::H2(FlightClientStateH2::DoPutSendProgress(data_tx, rx)) =
                    std::mem::replace(
                        &mut client.state,
                        FlightClientState::H2(FlightClientStateH2::Closed),
                    )
                else {
                    unreachable!()
                };

                if let Some(data) = data {
                    let flight_data = FlightData::new()
                        .with_data_header(data.data_header)
                        .with_app_metadata(data.app_metadata)
                        .with_data_body(data.data_body);
                    let res: FlightResult<()> =
                        data_tx.try_send(flight_data).map_err(|e| match e {
                            mpsc::error::TrySendError::Full(_) => ErrorCode::WouldBlock.into(),
                            mpsc::error::TrySendError::Closed(_) => {
                                ErrorCode::InternalError(Some("rx closed".to_string())).into()
                            }
                        });
                    res?;
                }
                if fin {
                    drop(data_tx);
                    client.state =
                        FlightClientState::H2(FlightClientStateH2::DoPutRecvProgress(rx));
                } else {
                    client.state =
                        FlightClientState::H2(FlightClientStateH2::DoPutSendProgress(data_tx, rx));
                }
                Ok(())
            }
            #[cfg(feature = "tonic-h3")]
            FlightClientState::H3(state_h3) => {
                match state_h3 {
                    FlightClientStateH3::DoPutSendProgress(..) => {}
                    _ => {
                        return Err(ErrorCode::InvalidState.into());
                    }
                }
                let FlightClientState::H3(FlightClientStateH3::DoPutSendProgress(data_tx, rx)) =
                    std::mem::replace(
                        &mut client.state,
                        FlightClientState::H3(FlightClientStateH3::Closed),
                    )
                else {
                    unreachable!()
                };
                if let Some(data) = data {
                    let flight_data = FlightData::new()
                        .with_data_header(data.data_header)
                        .with_app_metadata(data.app_metadata)
                        .with_data_body(data.data_body);
                    let res: FlightResult<()> =
                        data_tx.try_send(flight_data).map_err(|e| match e {
                            mpsc::error::TrySendError::Full(_) => ErrorCode::WouldBlock.into(),
                            mpsc::error::TrySendError::Closed(_) => {
                                ErrorCode::InternalError(Some("rx closed".to_string())).into()
                            }
                        });
                    res?;
                }
                if fin {
                    drop(data_tx);
                    client.state =
                        FlightClientState::H3(FlightClientStateH3::DoPutRecvProgress(rx));
                } else {
                    client.state =
                        FlightClientState::H3(FlightClientStateH3::DoPutSendProgress(data_tx, rx));
                }
                Ok(())
            }
        }
    }

    fn finish_do_put(
        &mut self,
        id: Resource<HostFlightClient>,
    ) -> FlightResult<Resource<HostFlightIncomingPutResponse>> {
        let client = self.table().get_mut(&id)?;
        match &client.state {
            FlightClientState::H2(..) => {
                let previous_state = std::mem::replace(
                    &mut client.state,
                    FlightClientState::H2(FlightClientStateH2::Closed),
                );
                let res = match previous_state {
                    FlightClientState::H2(FlightClientStateH2::DoPutReady(res)) => res,
                    FlightClientState::H2(FlightClientStateH2::DoPutRecvProgress(mut rx)) => {
                        let mut cx =
                            std::task::Context::from_waker(futures::task::noop_waker_ref());
                        match with_ambient_tokio_runtime(|| rx.poll_unpin(&mut cx)) {
                            Poll::Ready(res) => res,
                            Poll::Pending => {
                                client.state = FlightClientState::H2(
                                    FlightClientStateH2::DoPutRecvProgress(rx),
                                );
                                return Err(ErrorCode::WouldBlock.into());
                            }
                        }
                    }
                    previous_state => {
                        client.state = previous_state;
                        return Err(ErrorCode::NotInProgress.into());
                    }
                };
                match res {
                    Ok((flight_client, res)) => {
                        client.state =
                            FlightClientState::H2(FlightClientStateH2::Connected(flight_client));

                        match res {
                            Ok(response) => {
                                let stream = response.into_inner();
                                let incoming_put_response = HostFlightIncomingPutResponse {
                                    stream,
                                    pending_data: None,
                                };
                                Ok(self.table().push(incoming_put_response)?)
                            }
                            Err(err) => Err(ErrorCode::InternalError(Some(err.to_string())).into()),
                        }
                    }
                    Err(err) => {
                        client.state = FlightClientState::H2(FlightClientStateH2::Closed);
                        Err(ErrorCode::InternalError(Some(format!("rx error: {:?}", err))).into())
                    }
                }
            }
            #[cfg(feature = "tonic-h3")]
            FlightClientState::H3(..) => {
                let previous_state = std::mem::replace(
                    &mut client.state,
                    FlightClientState::H3(FlightClientStateH3::Closed),
                );
                let res = match previous_state {
                    FlightClientState::H3(FlightClientStateH3::DoPutReady(res)) => res,
                    FlightClientState::H3(FlightClientStateH3::DoPutRecvProgress(mut rx)) => {
                        let mut cx =
                            std::task::Context::from_waker(futures::task::noop_waker_ref());
                        match with_ambient_tokio_runtime(|| rx.poll_unpin(&mut cx)) {
                            Poll::Ready(res) => res,
                            Poll::Pending => {
                                client.state = FlightClientState::H3(
                                    FlightClientStateH3::DoPutRecvProgress(rx),
                                );
                                return Err(ErrorCode::WouldBlock.into());
                            }
                        }
                    }
                    previous_state => {
                        client.state = previous_state;
                        return Err(ErrorCode::NotInProgress.into());
                    }
                };
                match res {
                    Ok((flight_client, res)) => {
                        client.state =
                            FlightClientState::H3(FlightClientStateH3::Connected(flight_client));

                        match res {
                            Ok(response) => {
                                let stream = response.into_inner();
                                let incoming_put_response = HostFlightIncomingPutResponse {
                                    stream,
                                    pending_data: None,
                                };
                                Ok(self.table().push(incoming_put_response)?)
                            }
                            Err(err) => Err(ErrorCode::InternalError(Some(err.to_string())).into()),
                        }
                    }
                    Err(err) => {
                        client.state = FlightClientState::H3(FlightClientStateH3::Closed);
                        Err(ErrorCode::InternalError(Some(format!("rx error: {:?}", err))).into())
                    }
                }
            }
        }
    }

    fn subscribe(
        &mut self,
        id: Resource<HostFlightClient>,
    ) -> wasmtime::Result<Resource<DynPollable>> {
        subscribe(self.table(), id)
    }

    fn drop(&mut self, id: Resource<HostFlightClient>) -> wasmtime::Result<()> {
        let mut client = self.table().delete(id)?;
        if let Some(reg) = client.reg.take() {
            std::mem::drop(client);
            std::mem::drop(reg);
        } else {
            std::mem::drop(client);
        }
        Ok(())
    }
}

#[async_trait]
impl<T> crate::bindings::flight::types::HostIncomingResponse for FlightImpl<T>
where
    T: FlightView,
{
    fn get(
        &mut self,
        id: Resource<HostFlightIncomingResponse>,
    ) -> FlightResult<Option<crate::bindings::flight::types::FlightData>> {
        let incoming_response = self.table().get_mut(&id)?;
        let res = match incoming_response.pending_data.take() {
            Some(res) => res,
            None => {
                let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
                match with_ambient_tokio_runtime(|| {
                    incoming_response.stream.poll_next_unpin(&mut cx)
                }) {
                    Poll::Ready(res) => res,
                    Poll::Pending => {
                        return Err(ErrorCode::WouldBlock.into());
                    }
                }
            }
        };
        match res {
            Some(Ok(data)) => {
                let flight_descriptor = if let Some(flight_descriptor) = &data.flight_descriptor {
                    Some(crate::bindings::flight::types::FlightDescriptor {
                        descriptor_type: flight_descriptor.r#type,
                        cmd: flight_descriptor.cmd.to_vec(),
                        path: flight_descriptor.path.clone(),
                    })
                } else {
                    None
                };
                let flight_data = crate::bindings::flight::types::FlightData {
                    flight_descriptor,
                    data_header: data.data_header.to_vec(),
                    app_metadata: data.app_metadata.to_vec(),
                    data_body: data.data_body.to_vec(),
                };
                Ok(Some(flight_data))
            }
            Some(Err(err)) => Err(ErrorCode::InternalError(Some(err.to_string())).into()),
            None => Ok(None),
        }
    }

    fn subscribe(
        &mut self,
        id: Resource<HostFlightIncomingResponse>,
    ) -> wasmtime::Result<Resource<DynPollable>> {
        subscribe(self.table(), id)
    }

    fn drop(&mut self, id: Resource<HostFlightIncomingResponse>) -> wasmtime::Result<()> {
        let _ = self.table().delete(id)?;
        Ok(())
    }
}

#[async_trait]
impl<T> crate::bindings::flight::types::HostIncomingPutResponse for FlightImpl<T>
where
    T: FlightView,
{
    fn get(
        &mut self,
        id: Resource<HostFlightIncomingPutResponse>,
    ) -> FlightResult<Option<crate::bindings::flight::types::PutResult>> {
        let incoming_put_response = self.table().get_mut(&id)?;
        let res = match incoming_put_response.pending_data.take() {
            Some(res) => res,
            None => {
                let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
                match with_ambient_tokio_runtime(|| {
                    incoming_put_response.stream.poll_next_unpin(&mut cx)
                }) {
                    Poll::Ready(res) => res,
                    Poll::Pending => {
                        return Err(ErrorCode::WouldBlock.into());
                    }
                }
            }
        };
        match res {
            Some(Ok(data)) => {
                let put_result = crate::bindings::flight::types::PutResult {
                    app_metadata: data.app_metadata.to_vec(),
                };
                Ok(Some(put_result))
            }
            Some(Err(err)) => Err(ErrorCode::InternalError(Some(err.to_string())).into()),
            None => Ok(None),
        }
    }

    fn subscribe(
        &mut self,
        id: Resource<HostFlightIncomingPutResponse>,
    ) -> wasmtime::Result<Resource<DynPollable>> {
        subscribe(self.table(), id)
    }

    fn drop(&mut self, id: Resource<HostFlightIncomingPutResponse>) -> wasmtime::Result<()> {
        let _ = self.table().delete(id)?;
        Ok(())
    }
}
