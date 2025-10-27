// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

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

use rand::rngs::OsRng;
use rand::RngCore;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use isekai_utils::module::GetTicket;

const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_EXPOSED_HEADERS: [&str; 3] =
    ["grpc-status", "grpc-message", "grpc-status-details-bin"];
const DEFAULT_ALLOW_HEADERS: [&str; 4] =
    ["x-grpc-web", "content-type", "x-user-agent", "grpc-timeout"];

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

mod csv;
mod edinet;
mod storage;

#[derive(Clone)]
pub struct FlightServiceImpl {
    cmd_opts: CmdOptions,
    jwks: Jwks,
    valid_tokens: Arc<Mutex<Vec<String>>>,
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
        let mut inbound = request.into_inner();

        let mut cnt = 0;
        // Process incoming HandshakeRequests
        let valid_tokens = self.valid_tokens.clone();
        let output_stream = async_stream::try_stream! {
            while let Some(handshake_request) = inbound.next().await {
                let req = handshake_request?;
                let resp = if cnt == 0 {
                    println!("handshake rquest1");
                    cnt += 1;
                    let payload = if cmd_opts.use_test_challenge {
                        bytes::Bytes::copy_from_slice(snpguest::report::TEST_REQ_DATA)
                    } else {
                        // challenge must be 64 bytes
                        let mut challenge = [0u8; 64];
                        OsRng.fill_bytes(&mut challenge);
                        bytes::Bytes::copy_from_slice(&challenge)
                    };
                    HandshakeResponse {
                        protocol_version: 1,
                        payload: payload,
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
                    match att_res {
                        Err(e) => {
                            println!("failed to verify attestation report: {:#?}", e);
                            Err(Status::unauthenticated(format!("failed to verify attestation report: {:#?}", e)))
                        },
                        Ok(_) => {
                            println!("successfully verified attestation report");
                            Ok(())
                        }
                    }?;
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
    /// enable mTLS
    #[argh(switch)]
    no_tls: bool,
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cmd_opts: CmdOptions = argh::from_env();

    let addr = format!("0.0.0.0:{}", cmd_opts.port).parse()?;

    let jwks_url = "https://seera-networks.jp.auth0.com/.well-known/jwks.json";
    let jwks = Jwks::from_jwks_url(jwks_url).await?;

    let service = FlightServiceImpl {
        cmd_opts: cmd_opts.clone(),
        jwks,
        valid_tokens: Arc::new(Mutex::new(Vec::new())),
    };

    let svc = FlightServiceServer::new(service);

    let server = if cmd_opts.no_tls {
        Server::builder()
    } else {
        println!("TLS enabled");
        // Load server's identity (certificate and private key)
        let cert = std::fs::read(&cmd_opts.cert)?;
        let key = std::fs::read(&cmd_opts.key)?;
        let server_identity = tonic::transport::Identity::from_pem(cert, key);

        let tls_config = tonic::transport::ServerTlsConfig::new().identity(server_identity);

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
