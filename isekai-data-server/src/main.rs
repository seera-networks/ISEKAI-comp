// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use anyhow::anyhow;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::FlightService, flight_service_server::FlightServiceServer,
};
use base64::Engine;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use isekai_utils::module::GetTicket;
use jsonwebtoken::{Algorithm, TokenData, Validation, decode, decode_header};
use jwks::Jwks;
use rand::RngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::io::Write;
#[cfg(feature = "tonic-h3")]
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
#[cfg(feature = "tonic-h3")]
use tonic_h3::msquic_async::h3_msquic_async::{msquic, msquic_async};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    iss: String,      // Issuer
    sub: String,      // Subject (whom token refers to)
    aud: Vec<String>, // Audience
    iat: usize,       // Issued at (as UTC timestamp)
    exp: usize,       // Expiration time (as UTC timestamp)
    scope: String,
    azp: String,
}

mod auth;
mod csv;
mod edinet;
mod storage;

#[derive(Clone)]
pub struct FlightServiceImpl {
    cmd_opts: CmdOptions,
    jwks: Jwks,
    valid_tokens: Arc<Mutex<Vec<String>>>,
    server_ld: Option<[u8; 48]>,
}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let cmd_opts = self.cmd_opts.clone();
        if request.peer_certs().is_some() {
            println!("Client certificate presented");
        } else {
            println!("No client certificate presented");
        }
        let mut inbound = request.into_inner();

        let mut cnt = 0;
        // Process incoming HandshakeRequests
        let valid_tokens = self.valid_tokens.clone();
        let mut challenge = [0u8; 64];
        let server_ld = self.server_ld.clone();
        let output_stream = async_stream::try_stream! {
            while let Some(handshake_request) = inbound.next().await {
                let req = handshake_request?;
                let resp = if cnt == 0 {
                    println!("handshake rquest1");
                    cnt += 1;
                    if cmd_opts.use_test_challenge {
                        challenge.copy_from_slice(&snpguest::report::TEST_REQ_DATA[0..64]);
                    } else {
                        // challenge must be 64 bytes
                        OsRng.fill_bytes(&mut challenge);
                    }
                    HandshakeResponse {
                        protocol_version: 1,
                        payload: bytes::Bytes::copy_from_slice(&challenge),
                    }
                } else if cnt == 1{
                    println!("handshake rquest2");
                    cnt += 1;
                    let mut att_file = tempfile::NamedTempFile::new()?;
                    att_file.write(&req.payload)?;
                    let att_path = att_file.path().to_path_buf();
                    let certs_dir = std::path::PathBuf::from("/tmp/ext-grpc-server/snpguest/certs");
                    let att_res = snpguest::verify2::fetch_and_verify_async(certs_dir, att_path, true).await;
                    println!("handshake rquest2 done");
                    let att_report = match att_res {
                        Err(e) => {
                            println!("failed to verify attestation report: {:#?}", e);
                            Err(Status::unauthenticated(format!("failed to verify attestation report: {:#?}", e)))
                        },
                        Ok(r) => {
                            println!("successfully verified attestation report");
                            Ok(r)
                        }
                    }?;
                    if att_report.report_data != challenge {
                        println!("attestation report data does not match challenge");
                        return Err(Status::unauthenticated("attestation report data does not match challenge"))?
                    }
                    if let Some(ld) = server_ld {
                        if att_report.measurement != ld {
                            println!("launch digest does not match expected value");
                            return Err(Status::unauthenticated("launch digest does not match expected value"))?
                        }
                        println!("successfully verified launch digest");
                    } else {
                        println!("no server-ld configured, skipping");
                    }
                    let mut token = [0u8; 64];
                    OsRng.fill_bytes(&mut token);
                    let token = token.into_iter()
                        .map(|x| format!("{:02x}", x))
                        .collect::<String>();
                    valid_tokens.lock().unwrap().push(token.clone());
                    HandshakeResponse {
                        protocol_version: 1,
                        payload: bytes::Bytes::from(token),
                    }
                } else {
                    Err(Status::internal("too many handshake requests"))?
                };

                yield resp;
            }
        };

        let boxed_stream: Self::HandshakeStream = Box::pin(output_stream);
        Ok(Response::new(boxed_stream))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        println!("do_get");

        let token = request
            .metadata()
            .get("x-yak-authorization")
            .ok_or_else(|| Status::unauthenticated("No token"))
            .and_then(|value| {
                if value.len() >= 7 {
                    let value = value
                        .to_str()
                        .map_err(|_| Status::unauthenticated("invalid char"))?;
                    if &value[0..7] == "Bearer " {
                        Ok(value[7..].to_string())
                    } else {
                        Err(Status::unauthenticated("Invalid format"))
                    }
                } else {
                    Err(Status::unauthenticated("too short"))
                }
            })?;
        if !self.valid_tokens.lock().unwrap().contains(&token) {
            return Err(Status::unauthenticated(format!("Invalid token: {}", token)));
        }

        let jwt = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("No token"))
            .and_then(|value| {
                if value.len() >= 7 {
                    let value = value
                        .to_str()
                        .map_err(|_| Status::unauthenticated("invalid char"))?;
                    if &value[0..7] == "Bearer " {
                        Ok(value[7..].to_string())
                    } else {
                        Err(Status::unauthenticated("Invalid format"))
                    }
                } else {
                    Err(Status::unauthenticated("too short"))
                }
            });
        let subject = if let Ok(jwt) = jwt {
            let header = decode_header(&jwt)
                .map_err(|_| Status::unauthenticated("jwt header should be decoded"))?;
            let kid = header
                .kid
                .as_ref()
                .ok_or_else(|| Status::unauthenticated("jwt header should have a kid"))?;
            let jwk = self
                .jwks
                .keys
                .get(kid)
                .ok_or_else(|| Status::unauthenticated("jwt refer to a unknown key id"))?;
            let mut validation = Validation::new(Algorithm::RS256);
            validation.set_audience(&[
                "https://yakserv.seera-networks.com",
                "https://seera-networks.jp.auth0.com/userinfo",
            ]);
            let decoded_token: TokenData<Claims> =
                decode::<Claims>(&jwt, &jwk.decoding_key, &validation).map_err(|x| {
                    Status::unauthenticated(format!("jwt should be valid: {:?}", x))
                })?;
            decoded_token.claims.sub.replace("|", "_")
        } else {
            "test".to_string()
        };

        if !auth::authenticate_subject(&self.cmd_opts, &subject) {
            return Err(Status::unauthenticated(format!(
                "Unauthorized subject: {}",
                subject
            )));
        }

        let ticket = GetTicket::from_json(
            &String::from_utf8_lossy(&request.into_inner().ticket).to_string(),
        );
        let (input_stream, policy) = if ticket.target == "system" {
            let batches = if self.cmd_opts.csv_file.is_some() {
                csv::get_data(&self.cmd_opts, &ticket.column_name)?
            } else if self.cmd_opts.edinet_db.is_some() {
                edinet::get_data(&self.cmd_opts, &ticket.column_name)?
            } else {
                return Err(Status::internal("no data source"));
            };
            let input_stream = futures::stream::iter(batches.into_iter().map(Ok));

            let res = if self.cmd_opts.csv_file.is_some() {
                csv::get_policy(&self.cmd_opts, &subject, &ticket.column_name)
            } else {
                edinet::get_policy(&self.cmd_opts, &subject, &ticket.column_name)
            };

            let policy = match res {
                Ok(policy) => {
                    println!("subject: {}, policy: {}", subject, policy);
                    policy
                }
                Err(e) => {
                    return Err(Status::internal(format!("failed to get policy: {:?}", e)));
                }
            };
            (input_stream, policy)
        } else {
            let batches = storage::get_data(
                &self.cmd_opts,
                &subject,
                &ticket.target,
                &ticket.column_name,
            )
            .map_err(|e| Status::internal(format!("failed to get data: {:?}", e)))?;
            let input_stream = futures::stream::iter(batches.into_iter().map(Ok));
            let policy = storage::get_policy(
                &self.cmd_opts,
                &subject,
                &ticket.target,
                &ticket.column_name,
            )
            .map_err(|e| Status::internal(format!("failed to get policy: {:?}", e)))?;
            (input_stream, policy)
        };
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_metadata(policy.as_bytes().to_vec().into())
            .build(input_stream)
            .map_err(Status::from);

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        println!("do_put");

        let token = request
            .metadata()
            .get("x-yak-authorization")
            .ok_or_else(|| Status::unauthenticated("No token"))
            .and_then(|value| {
                if value.len() >= 7 {
                    let value = value
                        .to_str()
                        .map_err(|_| Status::unauthenticated("invalid char"))?;
                    if &value[0..7] == "Bearer " {
                        Ok(value[7..].to_string())
                    } else {
                        Err(Status::unauthenticated("Invalid format"))
                    }
                } else {
                    Err(Status::unauthenticated("too short"))
                }
            })?;
        if !self.valid_tokens.lock().unwrap().contains(&token) {
            return Err(Status::unauthenticated(format!("Invalid token: {}", token)));
        }

        let jwt = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("No token"))
            .and_then(|value| {
                if value.len() >= 7 {
                    let value = value
                        .to_str()
                        .map_err(|_| Status::unauthenticated("invalid char"))?;
                    if &value[0..7] == "Bearer " {
                        Ok(value[7..].to_string())
                    } else {
                        Err(Status::unauthenticated("Invalid format"))
                    }
                } else {
                    Err(Status::unauthenticated("too short"))
                }
            });

        let subject = if let Ok(jwt) = jwt {
            let header = decode_header(&jwt)
                .map_err(|_| Status::unauthenticated("jwt header should be decoded"))?;
            let kid = header
                .kid
                .as_ref()
                .ok_or_else(|| Status::unauthenticated("jwt header should have a kid"))?;
            let jwk = self
                .jwks
                .keys
                .get(kid)
                .ok_or_else(|| Status::unauthenticated("jwt refer to a unknown key id"))?;
            let mut validation = Validation::new(Algorithm::RS256);
            validation.set_audience(&[
                "https://yakserv.seera-networks.com",
                "https://seera-networks.jp.auth0.com/userinfo",
            ]);
            let decoded_token: TokenData<Claims> =
                decode::<Claims>(&jwt, &jwk.decoding_key, &validation).map_err(|x| {
                    Status::unauthenticated(format!("jwt should be valid: {:?}", x))
                })?;
            decoded_token.claims.sub.replace("|", "_")
        } else {
            "test".to_string()
        };

        if !auth::authenticate_subject(&self.cmd_opts, &subject) {
            return Err(Status::unauthenticated(format!(
                "Unauthorized subject: {}",
                subject
            )));
        }

        let mut target = None;
        let mut policy = None;
        let mut stream = FlightDataDecoder::new(request.into_inner().map_err(FlightError::from));
        while let Some(data) = stream.next().await {
            match data.map(|x| {
                if !x.inner.app_metadata.is_empty() {
                    policy = Some(String::from_utf8_lossy(&x.inner.app_metadata).to_string());
                }
                x.payload
            }) {
                Err(e) => {
                    println!("error: {:?}", e);
                    continue;
                }
                Ok(DecodedPayload::None) => {
                    println!("none");
                }
                Ok(DecodedPayload::Schema(schema)) => {
                    println!("schema: {:?}", schema);
                    target = Some(
                        storage::create_storage(&self.cmd_opts, &subject, schema, policy.take())
                            .map_err(|e| {
                                Status::internal(format!("Failed to create table: {:?}", e))
                            })?,
                    );
                }
                Ok(DecodedPayload::RecordBatch(batch)) => {
                    storage::insert_data(
                        &self.cmd_opts,
                        &subject,
                        target.as_ref().expect("target not created yet"),
                        batch,
                    )
                    .map_err(|e| Status::internal(format!("Failed to update table: {:?}", e)))?;
                }
            }
        }

        let results = vec![Ok(PutResult {
            app_metadata: bytes::Bytes::from(
                target.as_ref().expect("target not created yet").clone(),
            ),
        })];
        let result_stream = futures::stream::iter(results.into_iter());
        Ok(Response::new(Box::pin(result_stream)))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}

use argh::FromArgs;
#[derive(FromArgs, Clone)]
/// grpc-server args
pub struct CmdOptions {
    /// use h2
    #[argh(switch)]
    use_h2: bool,
    /// use h3
    #[cfg(feature = "tonic-h3")]
    #[argh(switch)]
    use_h3: bool,
    /// disable TLS
    #[argh(switch)]
    h2_no_tls: bool,
    /// authorized subject
    #[argh(option)]
    authorized_subject: Option<String>,
    /// CSV file path
    #[argh(option)]
    csv_file: Option<String>,
    /// EDINET db path
    #[argh(option)]
    edinet_db: Option<String>,
    /// default parquet path
    #[argh(option, default = "String::from(\"./parquet\")")]
    parquet_path: String,
    /// storage db path
    #[argh(option, default = "String::from(\"./storage.db\")")]
    storage_db: String,
    /// policy db path
    #[argh(option, default = "String::from(\"./policy.db\")")]
    policy_db: String,
    /// TLS certificate
    #[argh(option, default = "String::from(\"./certs/server.crt\")")]
    cert: String,
    /// TLS key
    #[argh(option, default = "String::from(\"./certs/server.key\")")]
    key: String,
    /// port
    #[argh(option, default = "50053")]
    port: u16,
    /// use test challenge for non SEV-SNP environment
    #[argh(switch)]
    use_test_challenge: bool,

    #[argh(option)]
    /// expected server launch digest in base64 format
    server_ld: Option<String>,
}

#[cfg(feature = "tonic-h3")]
fn make_msquic_async_listner(
    addr: Option<SocketAddr>,
    cert_path: &str,
    key_path: &str,
) -> anyhow::Result<(Arc<msquic::Registration>, msquic_async::Listener)> {
    let registration = msquic::Registration::new(&msquic::RegistrationConfig::default())?;
    let alpn = [msquic::BufferRef::from("h3")];
    let configuration = msquic::Configuration::open(
        &registration,
        &alpn,
        Some(
            &msquic::Settings::new()
                .set_IdleTimeoutMs(10000)
                .set_PeerBidiStreamCount(100)
                .set_PeerUnidiStreamCount(100)
                .set_DatagramReceiveEnabled()
                .set_StreamMultiReceiveEnabled()
                .set_ServerMigrationEnabled(),
        ),
    )?;

    #[cfg(not(windows))]
    {
        use std::io::Write;
        use tempfile::NamedTempFile;
        let cert_path = cert_path.to_owned();
        let key_path = key_path.to_owned();

        let ca_cert = include_bytes!("../../certs/yakCA.crt");
        let mut ca_cert_file = NamedTempFile::new()?;
        ca_cert_file.write_all(ca_cert)?;
        let ca_cert_path = ca_cert_file.into_temp_path();
        let ca_cert_path = ca_cert_path.to_string_lossy().into_owned();

        let cred_config = msquic::CredentialConfig::new()
            .set_credential_flags(msquic::CredentialFlags::REQUIRE_CLIENT_AUTHENTICATION)
            .set_credential(msquic::Credential::CertificateFile(
                msquic::CertificateFile::new(key_path, cert_path),
            ))
            .set_ca_certificate_file(ca_cert_path);

        configuration.load_credential(&cred_config)?;
    }

    #[cfg(windows)]
    {
        use schannel::RawPointer;
        use schannel::cert_context::{CertContext, KeySpec};
        use schannel::cert_store::{CertAdd, Memory};
        use schannel::crypt_prov::{AcquireOptions, ProviderType};

        let cert = std::fs::read(cert_path)?;
        let key = std::fs::read(key_path)?;

        let mut store = Memory::new().unwrap().into_store();

        let name = String::from("isekai-data-server");

        let cert_ctx = CertContext::from_pem(cert).unwrap();

        let mut options = AcquireOptions::new();
        options.container(&name);

        let type_ = ProviderType::rsa_full();

        let mut container = match options.acquire(type_) {
            Ok(container) => container,
            Err(_) => options.new_keyset(true).acquire(type_).unwrap(),
        };
        container.import().import_pkcs8_pem(key).unwrap();

        cert_ctx
            .set_key_prov_info()
            .container(&name)
            .type_(type_)
            .keep_open(true)
            .key_spec(KeySpec::key_exchange())
            .set()
            .unwrap();

        let context = store.add_cert(&cert_ctx, CertAdd::Always).unwrap();

        let cred_config = msquic::CredentialConfig::new()
            .set_credential_flags(msquic::CredentialFlags::REQUIRE_CLIENT_AUTHENTICATION)
            .set_credential(msquic::Credential::CertificateContext(unsafe {
                context.as_ptr()
            }));

        configuration.load_credential(&cred_config)?;
    }

    let listner = msquic_async::Listener::new(&registration, configuration)?;
    listner.start(&alpn, addr)?;
    Ok((Arc::new(registration), listner))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        .with_writer(std::io::stderr)
        .init();

    let cmd_opts: CmdOptions = argh::from_env();

    let addr = format!("0.0.0.0:{}", cmd_opts.port).parse()?;

    let jwks_url = "https://seera-networks.jp.auth0.com/.well-known/jwks.json";
    let jwks = Jwks::from_jwks_url(jwks_url).await?;

    let server_ld = if let Some(server_ld) = &cmd_opts.server_ld {
        let base64_engine = base64::engine::general_purpose::STANDARD;
        let res = base64_engine.decode(server_ld.as_bytes())?;
        if res.len() != 48 {
            return Err(anyhow!(
                "server_ld must be 48 bytes when decoded, but got {}",
                res.len()
            ));
        }
        Some(res[0..48].try_into().unwrap())
    } else {
        None
    };

    let mut join_set = JoinSet::new();
    let token = CancellationToken::new();

    if cmd_opts.use_h2 {
        let cmd_opts = cmd_opts.clone();
        let jwks = jwks.clone();
        let server_ld = server_ld.clone();
        let token = token.clone();
        join_set.spawn(async move {
            println!("Starting gRPC server with h2");

            let mut server = if cmd_opts.h2_no_tls {
                Server::builder()
            } else {
                println!("TLS enabled");
                // Load server's identity (certificate and private key)
                let cert = std::fs::read(&cmd_opts.cert)?;
                let key = std::fs::read(&cmd_opts.key)?;
                let server_identity = tonic::transport::Identity::from_pem(cert, key);
                let client_ca_cert = tonic::transport::Certificate::from_pem(include_bytes!(
                    "../../certs/yakCA.crt"
                ));

                let tls_config = tonic::transport::ServerTlsConfig::new()
                    .identity(server_identity)
                    .client_ca_root(client_ca_cert);

                Server::builder().tls_config(tls_config)?
            };

            let service = FlightServiceImpl {
                cmd_opts,
                jwks,
                valid_tokens: Arc::new(Mutex::new(Vec::new())),
                server_ld,
            };

            let svc = FlightServiceServer::new(service);

            server
                .add_service(svc)
                .serve_with_shutdown(addr, async move { token.cancelled().await })
                .await?;
            anyhow::Ok(())
        });
    }

    #[cfg(feature = "tonic-h3")]
    if cmd_opts.use_h3 {
        let token = token.clone();
        join_set.spawn(async move {
            println!("Starting gRPC server with h3");

            let (_registration, listener) =
                make_msquic_async_listner(Some(addr), &cmd_opts.cert, &cmd_opts.key)?;
            let acceptor = tonic_h3::msquic_async::H3MsQuicAsyncAcceptor::new(listener);

            let service = FlightServiceImpl {
                cmd_opts,
                jwks,
                valid_tokens: Arc::new(Mutex::new(Vec::new())),
                server_ld,
            };

            let svc = FlightServiceServer::new(service);

            let router = tonic::service::Routes::builder()
                .add_service(svc)
                .clone()
                .routes();
            let _ = tonic_h3::server::H3Router::new(router)
                .serve_with_shutdown(acceptor, async move { token.cancelled().await })
                .await;
            anyhow::Ok(())
        });
    }

    if join_set.is_empty() {
        println!("No server is configured to start. Please check the command line options.");
        return Ok(());
    }
    join_set.spawn(async move {
        println!("Press Ctrl+C to stop the server...");
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");
        println!("Shutting down...");
        token.cancel();
        anyhow::Ok(())
    });

    join_set.join_all().await;
    Ok(())
}
