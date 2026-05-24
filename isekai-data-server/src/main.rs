// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use anyhow::anyhow;
use base64::Engine;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use http::header::HeaderName;
use jsonwebtoken::{decode, decode_header, Algorithm, TokenData, Validation};
use jwks::Jwks;
use serde::{Deserialize, Serialize};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tonic_web::GrpcWebLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};

use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};

use isekai_utils::module::GetTicket;
use rand::rngs::OsRng;
use rand::RngCore;
use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::{debug, error, info};
use tracing_subscriber::{fmt, EnvFilter};

const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_EXPOSED_HEADERS: [&str; 3] =
    ["grpc-status", "grpc-message", "grpc-status-details-bin"];
const DEFAULT_ALLOW_HEADERS: [&str; 4] =
    ["x-grpc-web", "content-type", "x-user-agent", "grpc-timeout"];
const HANDSHAKE_TOKEN_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_VALID_TOKENS: usize = 1024;

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

#[derive(Default)]
struct ValidTokenStore {
    tokens: HashMap<String, Instant>,
}

impl ValidTokenStore {
    fn insert(&mut self, token: String, now: Instant) {
        self.prune(now);
        self.tokens.insert(token, now + HANDSHAKE_TOKEN_TTL);
        while self.tokens.len() > MAX_VALID_TOKENS {
            if let Some(expired_token) = self
                .tokens
                .iter()
                .min_by_key(|(_, expires_at)| *expires_at)
                .map(|(token, _)| token.clone())
            {
                self.tokens.remove(&expired_token);
            } else {
                break;
            }
        }
    }

    fn contains(&mut self, token: &str, now: Instant) -> bool {
        self.prune(now);
        self.tokens.contains_key(token)
    }

    fn prune(&mut self, now: Instant) {
        self.tokens.retain(|_, expires_at| *expires_at > now);
    }
}

#[derive(Clone)]
pub struct FlightServiceImpl {
    cmd_opts: CmdOptions,
    jwks: Jwks,
    valid_tokens: Arc<Mutex<ValidTokenStore>>,
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
        // if request.peer_certs().is_some() {
        //     println!("Client certificate presented");
        // } else {
        //     println!("No client certificate presented");
        // }
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
                    debug!("handshake rquest1");
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
                    debug!("handshake rquest2");
                    cnt += 1;
                    let mut att_file = tempfile::NamedTempFile::new()?;
                    att_file.write(&req.payload)?;
                    let att_path = att_file.path().to_path_buf();
                    let certs_dir = std::path::PathBuf::from("/tmp/ext-grpc-server/snpguest/certs");
                    let att_res = snpguest::verify2::fetch_and_verify_async(certs_dir, att_path, true).await;
                    debug!("handshake rquest2 done");
                    let att_report = match att_res {
                        Err(e) => {
                            error!("failed to verify attestation report: {:#?}", e);
                            Err(Status::unauthenticated(format!("failed to verify attestation report: {:#?}", e)))
                        },
                        Ok(r) => {
                            info!("successfully verified attestation report");
                            Ok(r)
                        }
                    }?;
                    if att_report.report_data != challenge {
                        error!("attestation report data does not match challenge");
                        return Err(Status::unauthenticated("attestation report data does not match challenge"))?
                    }
                    if let Some(ld) = server_ld {
                        if att_report.measurement != ld {
                            error!("launch digest does not match expected value");
                            return Err(Status::unauthenticated("launch digest does not match expected value"))?
                        }
                        info!("successfully verified launch digest");
                    } else {
                        info!("no server-ld configured, skipping");
                    }
                    let mut token = [0u8; 64];
                    OsRng.fill_bytes(&mut token);
                    let token = token.into_iter()
                        .map(|x| format!("{:02x}", x))
                        .collect::<String>();
                    valid_tokens
                        .lock()
                        .unwrap()
                        .insert(token.clone(), Instant::now());
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
        debug!("do_get");

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
        if !self
            .valid_tokens
            .lock()
            .unwrap()
            .contains(&token, Instant::now())
        {
            error!("Invalid token");
            return Err(Status::unauthenticated("Invalid token"));
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
        } else if self.cmd_opts.allow_test_subject {
            "test".to_string()
        } else {
            return Err(Status::unauthenticated("No JWT provided"));
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
                    info!("subject: {}, policy: {}", subject, policy);
                    policy
                }
                Err(e) => {
                    error!("failed to get policy: {:?}", e);
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
        debug!("do_put");

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
        if !self
            .valid_tokens
            .lock()
            .unwrap()
            .contains(&token, Instant::now())
        {
            return Err(Status::unauthenticated("Invalid token"));
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
        } else if self.cmd_opts.allow_test_subject {
            "test".to_string()
        } else {
            return Err(Status::unauthenticated("No JWT provided"));
        };

        if !auth::authenticate_subject(&self.cmd_opts, &subject) {
            return Err(Status::unauthenticated(format!(
                "Unauthorized subject: {}",
                subject
            )));
        }

        let mut policy = None;
        let mut schema = None;
        let mut batches = Vec::new();
        let mut stream = FlightDataDecoder::new(request.into_inner().map_err(FlightError::from));
        while let Some(data) = stream.next().await {
            let data = data.map_err(|e| {
                error!("Failed to decode FlightData: {:?}", e);
                Status::invalid_argument(format!("Failed to decode FlightData: {:?}", e))
            })?;
            if !data.inner.app_metadata.is_empty() {
                policy = Some(String::from_utf8_lossy(&data.inner.app_metadata).to_string());
            }
            match data.payload {
                DecodedPayload::None => {
                    error!("Received empty payload");
                    return Err(Status::invalid_argument("Received empty payload"));
                }
                DecodedPayload::Schema(decoded_schema) => {
                    if schema.is_some() {
                        return Err(Status::invalid_argument(
                            "Multiple schemas are not supported",
                        ));
                    }
                    schema = Some(decoded_schema);
                }
                DecodedPayload::RecordBatch(batch) => {
                    if schema.is_none() {
                        return Err(Status::invalid_argument(
                            "Schema must be sent before RecordBatch",
                        ));
                    }
                    batches.push(batch);
                }
            }
        }

        let schema = schema.ok_or_else(|| Status::invalid_argument("No schema was provided"))?;
        let target_name = storage::store_data(&self.cmd_opts, &subject, schema, policy, batches)
            .map_err(|e| Status::internal(format!("Failed to store data: {:?}", e)))?;
        let results = vec![Ok(PutResult {
            app_metadata: bytes::Bytes::from(target_name),
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
    /// enable mTLS
    #[argh(switch)]
    no_tls: bool,
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

    /// allow requests without JWT to be treated as subject "test"
    #[argh(switch)]
    allow_test_subject: bool,

    #[argh(option)]
    /// expected server launch digest in base64 format
    server_ld: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file_appender = tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("isekai-data-server")
        .filename_suffix("log")
        .build("./logs")
        .expect("Failed to create log file appender");

    let filter = EnvFilter::try_from_env("ISEKAI_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_writer(file_appender)
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
    let service = FlightServiceImpl {
        cmd_opts: cmd_opts.clone(),
        jwks,
        valid_tokens: Arc::new(Mutex::new(ValidTokenStore::default())),
        server_ld,
    };

    let svc = FlightServiceServer::new(service);

    let server = if cmd_opts.no_tls {
        Server::builder()
    } else {
        info!("TLS enabled");
        // Load server's identity (certificate and private key)
        let cert = std::fs::read(&cmd_opts.cert)?;
        let key = std::fs::read(&cmd_opts.key)?;
        let server_identity = tonic::transport::Identity::from_pem(cert, key);
        let client_ca_cert =
            tonic::transport::Certificate::from_pem(include_bytes!("../../certs/yakCA.crt"));

        let tls_config = tonic::transport::ServerTlsConfig::new()
            .identity(server_identity)
            .client_ca_root(client_ca_cert);

        Server::builder().tls_config(tls_config)?
    };

    server
        .accept_http1(true)
        .layer(
            CorsLayer::new()
                .allow_origin(AllowOrigin::mirror_request())
                .allow_credentials(true)
                .max_age(DEFAULT_MAX_AGE)
                .expose_headers(
                    DEFAULT_EXPOSED_HEADERS
                        .iter()
                        .cloned()
                        .map(HeaderName::from_static)
                        .collect::<Vec<HeaderName>>(),
                )
                .allow_headers(
                    DEFAULT_ALLOW_HEADERS
                        .iter()
                        .cloned()
                        .map(HeaderName::from_static)
                        .collect::<Vec<HeaderName>>(),
                ),
        )
        .layer(GrpcWebLayer::new())
        .add_service(svc)
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ValidTokenStore, HANDSHAKE_TOKEN_TTL, MAX_VALID_TOKENS};
    use std::time::{Duration, Instant};

    #[test]
    fn token_store_prunes_expired_tokens() {
        let mut store = ValidTokenStore::default();
        let now = Instant::now();

        store.insert("valid".to_string(), now);
        assert!(store.contains("valid", now + Duration::from_secs(1)));
        assert!(!store.contains("valid", now + HANDSHAKE_TOKEN_TTL + Duration::from_secs(1)));
    }

    #[test]
    fn token_store_keeps_size_bounded() {
        let mut store = ValidTokenStore::default();
        let now = Instant::now();

        for i in 0..=MAX_VALID_TOKENS {
            store.insert(format!("token-{i}"), now + Duration::from_millis(i as u64));
        }

        assert_eq!(store.tokens.len(), MAX_VALID_TOKENS);
        assert!(!store.contains(
            "token-0",
            now + Duration::from_millis(MAX_VALID_TOKENS as u64),
        ));
        assert!(store.contains(
            &format!("token-{MAX_VALID_TOKENS}"),
            now + Duration::from_millis(MAX_VALID_TOKENS as u64),
        ));
    }
}
