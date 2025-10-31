// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

//! Implementation for the `flight/types` interface.
use crate::bindings::flight::types::ErrorCode;
pub use crate::ctx::{FlightImpl, FlightView};
use crate::error::{FlightError, FlightResult};
use crate::types::{
    FlightClientState, HostFlightClient, HostFlightIncomingPutResponse, HostFlightIncomingResponse,
};
use arrow::datatypes::ToByteSlice;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor};
use bytes::Bytes;
use core::task::Poll;
use futures::{FutureExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Request;
use wasmtime::component::*;
use wasmtime_wasi::p2::{subscribe, DynPollable};
use wasmtime_wasi::{async_trait, runtime::with_ambient_tokio_runtime};

use std::str::FromStr;

#[async_trait]
impl<T> crate::bindings::flight::client::Host for FlightImpl<T>
where
    T: FlightView,
{
    fn create_client(&mut self) -> FlightResult<Resource<HostFlightClient>> {
        let client = HostFlightClient {
            state: FlightClientState::Default,
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

#[async_trait]
impl<T> crate::bindings::flight::types::HostClient for FlightImpl<T>
where
    T: FlightView,
{
    fn start_connect(&mut self, id: Resource<HostFlightClient>) -> FlightResult<()> {
        let server_url = self.ctx().server_url.clone();
        let use_tls = self.ctx().use_tls;
        let client_cert_pem = self.ctx().client_cert_pem.clone();
        let client_key_pem = self.ctx().client_key_pem.clone();
        let ca_cert_pem = self.ctx().ca_cert_pem.clone();
        let client = self.table().get_mut(&id)?;
        match &client.state {
            FlightClientState::Default => {}
            _ => {
                return Err(ErrorCode::AlreadyConnected.into());
            }
        }
        let (tx, rx) = oneshot::channel::<anyhow::Result<Channel>>();

        with_ambient_tokio_runtime(|| {
            tokio::spawn(async move {
                let endpoint = Channel::builder(server_url.parse().unwrap());
                let endpoint = if use_tls {
                    let tls_config = if let Some(ca_cert_pem) = ca_cert_pem {
                        ClientTlsConfig::new().ca_certificate(Certificate::from_pem(&ca_cert_pem))
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
                            ).into()));
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
                let res = endpoint.connect().await;
                let _ = tx.send(res.map_err(|e| e.into()));
            })
        });
        client.state = FlightClientState::Connecting(rx);
        Ok(())
    }

    fn finish_connect(&mut self, id: Resource<HostFlightClient>) -> FlightResult<()> {
        let client = self.table().get_mut(&id)?;
        let previous_state = std::mem::replace(&mut client.state, FlightClientState::Closed);
        let res = match previous_state {
            FlightClientState::ConnectReady(res) => res,
            FlightClientState::Connecting(mut rx) => {
                let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
                match with_ambient_tokio_runtime(|| rx.poll_unpin(&mut cx)) {
                    Poll::Ready(res) => res,
                    Poll::Pending => {
                        client.state = FlightClientState::Connecting(rx);
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
            Ok(Ok(channel)) => {
                client.state = FlightClientState::Connected(FlightServiceClient::new(channel));
                Ok(())
            }
            Ok(Err(err)) => {
                client.state = FlightClientState::Closed;
                Err(ErrorCode::ConnectionRefused(err.to_string()).into())
            }
            Err(err) => {
                client.state = FlightClientState::Closed;
                Err(ErrorCode::InternalError(Some(format!("rx error: {:?}", err))).into())
            }
        }
    }

    fn start_handshake(&mut self, id: Resource<HostFlightClient>) -> FlightResult<()> {
        let att_config = self.ctx().attestation_config.clone();
        let client = self.table().get_mut(&id)?;
        match &client.state {
            FlightClientState::Connected(..) => {}
            FlightClientState::HandshakeProgress(..) => {
                return Err(ErrorCode::InProgress.into());
            }
            _ => {
                return Err(ErrorCode::InvalidState.into());
            }
        };

        let FlightClientState::Connected(mut flight_client) =
            std::mem::replace(&mut client.state, FlightClientState::Closed)
        else {
            unreachable!()
        };

        let (otx, orx) = oneshot::channel();
        with_ambient_tokio_runtime(|| {
            tokio::spawn(async move {
                async fn do_handshake(
                    flight_client: &mut FlightServiceClient<Channel>,
                    att_config: snpguest::report::AttestationConfig,
                ) -> tonic::Result<String> {
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
                        tonic::Status::internal(format!(
                            "failed to send handshake request: {:?}",
                            e
                        ))
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
                    let att = snpguest::report::get_report_direct(&att_req).map_err(|e| {
                        tonic::Status::internal(format!("failed to get report: {:?}", e))
                    })?;

                    let ext_att_bin = bincode::serialize(&att).unwrap();
                    let req = arrow_flight::HandshakeRequest {
                        protocol_version: 1,
                        payload: bytes::Bytes::copy_from_slice(&ext_att_bin),
                    };
                    tx.send(req).await.map_err(|e| {
                        tonic::Status::internal(format!(
                            "failed to send handshake request: {:?}",
                            e
                        ))
                    })?;
                    let res = handshake_stream
                        .message()
                        .await?
                        .ok_or(tonic::Status::internal(
                            "failed to receive handshake response",
                        ))?;
                    Ok(String::from_utf8_lossy(&res.payload).to_string())
                }

                let res = do_handshake(&mut flight_client, att_config).await;
                let _ = otx.send((flight_client, res));
            })
        });

        client.state = FlightClientState::HandshakeProgress(orx);
        Ok(())
    }

    fn finish_handshake(&mut self, id: Resource<HostFlightClient>) -> FlightResult<()> {
        let client = self.table().get_mut(&id)?;
        let previous_state = std::mem::replace(&mut client.state, FlightClientState::Closed);
        let res = match previous_state {
            FlightClientState::HandshakeReady(res) => res,
            FlightClientState::HandshakeProgress(mut rx) => {
                let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
                let res = match with_ambient_tokio_runtime(|| rx.poll_unpin(&mut cx)) {
                    Poll::Ready(res) => res,
                    Poll::Pending => {
                        client.state = FlightClientState::HandshakeProgress(rx);
                        return Err(ErrorCode::WouldBlock.into());
                    }
                };
                res
            }
            previous_state => {
                client.state = previous_state;
                return Err(ErrorCode::NotInProgress.into());
            }
        };

        match res {
            Ok((flight_client, res)) => {
                client.state = FlightClientState::Connected(flight_client);
                match res {
                    Ok(token) => {
                        client.token = Some(token);
                        Ok(())
                    }
                    Err(err) => Err(ErrorCode::InternalError(Some(err.to_string())).into()),
                }
            }
            Err(err) => {
                client.state = FlightClientState::Closed;
                Err(ErrorCode::InternalError(Some(format!("rx error: {:?}", err))).into())
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
            FlightClientState::Connected(..) => {}
            FlightClientState::DoGetProgress(..) => {
                return Err(ErrorCode::InProgress.into());
            }
            _ => {
                return Err(ErrorCode::InvalidState.into());
            }
        };

        let FlightClientState::Connected(mut flight_client) =
            std::mem::replace(&mut client.state, FlightClientState::Closed)
        else {
            unreachable!()
        };

        let (tx, rx) = oneshot::channel();
        with_ambient_tokio_runtime(|| {
            tokio::spawn(async move {
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
                    Err(err) => {
                        let _ = tx.send((flight_client, Err(err)));
                    }
                    Ok(request) => {
                        let res = flight_client.do_get(request).await;
                        let _ = tx.send((flight_client, res));
                    }
                }
            })
        });

        client.state = FlightClientState::DoGetProgress(rx);
        Ok(())
    }

    fn finish_do_get(
        &mut self,
        id: Resource<HostFlightClient>,
    ) -> FlightResult<Resource<HostFlightIncomingResponse>> {
        let client = self.table().get_mut(&id)?;
        let previous_state = std::mem::replace(&mut client.state, FlightClientState::Closed);
        let res = match previous_state {
            FlightClientState::DoGetReady(res) => res,
            FlightClientState::DoGetProgress(mut rx) => {
                let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
                match with_ambient_tokio_runtime(|| rx.poll_unpin(&mut cx)) {
                    Poll::Ready(res) => res,
                    Poll::Pending => {
                        client.state = FlightClientState::DoGetProgress(rx);
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
                client.state = FlightClientState::Connected(flight_client);
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
                client.state = FlightClientState::Closed;
                Err(ErrorCode::InternalError(Some(format!("rx error: {:?}", err))).into())
            }
        }
    }

    fn start_do_put(&mut self, id: Resource<HostFlightClient>) -> FlightResult<()> {
        let jwt = self.ctx().jwt.clone();
        let client = self.table().get_mut(&id)?;
        let token = client.token.clone();
        match &client.state {
            FlightClientState::Connected(..) => {}
            FlightClientState::DoPutSendProgress(..) | FlightClientState::DoPutRecvProgress(..) => {
                return Err(ErrorCode::InProgress.into());
            }
            _ => {
                return Err(ErrorCode::InvalidState.into());
            }
        };

        let FlightClientState::Connected(mut flight_client) =
            std::mem::replace(&mut client.state, FlightClientState::Closed)
        else {
            unreachable!()
        };

        let (data_tx, data_rx) = mpsc::channel::<FlightData>(100);
        let (tx, rx) = oneshot::channel();
        with_ambient_tokio_runtime(|| {
            tokio::spawn(async move {
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
                    Err(err) => {
                        let _ = tx.send((flight_client, Err(err)));
                    }
                    Ok(request) => {
                        let res = flight_client.do_put(request).await;
                        let _ = tx.send((flight_client, res));
                    }
                }
            })
        });

        client.state = FlightClientState::DoPutSendProgress(data_tx, rx);
        Ok(())
    }

    fn do_put(
        &mut self,
        id: Resource<HostFlightClient>,
        data: Option<crate::bindings::flight::types::FlightData>,
        fin: bool,
    ) -> FlightResult<()> {
        let client = self.table().get_mut(&id)?;
        match &client.state {
            FlightClientState::DoPutSendProgress(..) => {}
            _ => {
                return Err(ErrorCode::InvalidState.into());
            }
        };

        if data.is_none() && !fin {
            return Err(ErrorCode::InvalidParameter.into());
        }

        let FlightClientState::DoPutSendProgress(data_tx, rx) =
            std::mem::replace(&mut client.state, FlightClientState::Closed)
        else {
            unreachable!()
        };
        if let Some(data) = data {
            let flight_data = FlightData::new()
                .with_data_header(data.data_header)
                .with_app_metadata(data.app_metadata)
                .with_data_body(data.data_body);
            let res: FlightResult<()> = data_tx.try_send(flight_data).map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => ErrorCode::WouldBlock.into(),
                mpsc::error::TrySendError::Closed(_) => {
                    ErrorCode::InternalError(Some("rx closed".to_string())).into()
                }
            });
            res?;
        }
        if fin {
            drop(data_tx);
            client.state = FlightClientState::DoPutRecvProgress(rx);
        } else {
            client.state = FlightClientState::DoPutSendProgress(data_tx, rx);
        }
        Ok(())
    }

    fn finish_do_put(
        &mut self,
        id: Resource<HostFlightClient>,
    ) -> FlightResult<Resource<HostFlightIncomingPutResponse>> {
        let client = self.table().get_mut(&id)?;
        let previous_state = std::mem::replace(&mut client.state, FlightClientState::Closed);
        let res = match previous_state {
            FlightClientState::DoPutReady(res) => res,
            FlightClientState::DoPutRecvProgress(mut rx) => {
                let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
                match with_ambient_tokio_runtime(|| rx.poll_unpin(&mut cx)) {
                    Poll::Ready(res) => res,
                    Poll::Pending => {
                        client.state = FlightClientState::DoPutRecvProgress(rx);
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
                client.state = FlightClientState::Connected(flight_client);
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
                client.state = FlightClientState::Closed;
                Err(ErrorCode::InternalError(Some(format!("rx error: {:?}", err))).into())
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
        let _ = self.table().delete(id)?;
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
