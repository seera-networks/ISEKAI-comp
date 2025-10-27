// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::{FlightData, FlightDescriptor};
use core::panic;
use futures::{Stream, StreamExt, TryStreamExt, ready};
use std::pin::Pin;
use std::task::{Context, Poll};
use isekai_utils::{FlightDataStream, module::GetTicket};
use isekai_utils_mod_http::{BodyDataStream, write_record};
use wstd::http::body::{BodyForthcoming, IncomingBody};
use wstd::http::server::{Finished, Responder};
use wstd::http::{Request, Response};
use wstd::runtime::AsyncPollable;

mod generated {
    wit_bindgen::generate!({
        world: "mywasi:flight/api",
        path: "../../crates/mywasi-flight/wit",
        with: {
            "wasi:io/poll@0.2.0": wasi::io::poll,
        },
        generate_all,
    });
}
use generated::mywasi::flight::client;
use generated::mywasi::flight::types::{ErrorCode, FlightData as MyFlightData, IncomingResponse};

struct MyWasiFlightDataStream {
    incoming: IncomingResponse,
    fut: Option<Pin<Box<dyn Future<Output = ()>>>>,
}
unsafe impl Send for MyWasiFlightDataStream {}
unsafe impl Sync for MyWasiFlightDataStream {}

impl MyWasiFlightDataStream {
    pub fn new(incoming: IncomingResponse) -> Self {
        Self {
            incoming,
            fut: None,
        }
    }
}

impl Stream for MyWasiFlightDataStream {
    type Item = arrow_flight::error::Result<FlightData>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let res = if this.fut.is_none() {
            match this.incoming.get() {
                Ok(Some(data)) => Ok(Some(data)),
                Ok(None) => Ok(None),
                Err(err) => Err(err),
            }
        } else {
            Err(ErrorCode::WouldBlock)
        };
        let res = match res {
            Err(ErrorCode::WouldBlock) => {
                this.fut.get_or_insert_with(|| {
                    let pollable = this.incoming.subscribe();
                    let subscription = AsyncPollable::new(pollable);
                    let fut = subscription.wait_for();
                    let fut_pin: Pin<Box<dyn Future<Output = ()>>> = Box::pin(fut);
                    fut_pin
                });
                ready!(this.fut.as_mut().unwrap().as_mut().poll(cx));
                this.fut = None;
                match this.incoming.get() {
                    Ok(Some(data)) => Ok(Some(data)),
                    Ok(None) => Ok(None),
                    Err(err) => Err(err),
                }
            }
            res => res,
        };
        match res {
            Ok(Some(data)) => {
                let flight_data = FlightData::new()
                    .with_data_header(data.data_header)
                    .with_app_metadata(data.app_metadata)
                    .with_data_body(data.data_body);
                let flight_data = if let Some(flight_descriptor) = data.flight_descriptor {
                    flight_data.with_descriptor(FlightDescriptor {
                        r#type: flight_descriptor.descriptor_type,
                        cmd: flight_descriptor.cmd.into(),
                        path: flight_descriptor.path,
                    })
                } else {
                    flight_data
                };
                Poll::Ready(Some(Ok(flight_data)))
            }
            Ok(None) => Poll::Ready(None),
            Err(err) => Poll::Ready(Some(Err(arrow_flight::error::FlightError::protocol(
                format!("error: {:?}", err),
            )))),
        }
    }
}

async fn handle_load_data(request: Request<IncomingBody>, responder: Responder) -> Finished {
    let target = match request
        .headers()
        .get("X-Yak-Target")
        .and_then(|v| v.to_str().ok())
    {
        Some(v) if !v.is_empty() => v.to_string(),
        _ => {
            panic!("X-Yak-Target header is required");
        }
    };
    let col_name = match request
        .headers()
        .get("X-Yak-Column-Name")
        .and_then(|v| v.to_str().ok())
    {
        Some(v) if !v.is_empty() => v.to_string(),
        _ => {
            panic!("X-Yak-Column-Name header is required");
        }
    };

    let ticket = GetTicket {
        target: target.clone(),
        column_name: col_name.clone(),
    }
    .to_json();

    let client = client::create_client().unwrap();
    client.start_connect().unwrap();
    let subscription = AsyncPollable::new(client.subscribe());
    subscription.wait_for().await;
    client.finish_connect().unwrap();

    client.start_handshake().unwrap();
    let subscription = AsyncPollable::new(client.subscribe());
    subscription.wait_for().await;
    client.finish_handshake().unwrap();

    client.start_do_get(&ticket.into_bytes()).unwrap();
    let subscription = AsyncPollable::new(client.subscribe());
    subscription.wait_for().await;
    let incoming_response = client.finish_do_get().unwrap();
    let flight_data_stream = MyWasiFlightDataStream::new(incoming_response);

    let mut outgoing_body = None;

    let mut responder = Some(responder);
    let mut stream = FlightDataDecoder::new(flight_data_stream);
    let mut policy = Some("{}".to_string());
    while let Some(data) = stream.next().await {
        match data.map(|x| {
            if !x.inner.app_metadata.is_empty() {
                policy = Some(String::from_utf8_lossy(&x.inner.app_metadata).to_string());
            }
            x.payload
        }) {
            Ok(DecodedPayload::Schema(schema)) => {
                eprintln!("Schema: {:?}", schema);
            }
            Ok(DecodedPayload::None) => {
                eprintln!("None");
            }
            Ok(DecodedPayload::RecordBatch(batch)) => {
                let outgoing = outgoing_body.get_or_insert_with(|| {
                    let response = Response::builder()
                        .status(200)
                        .header(
                            "X-Yak-Policy",
                            policy.take().expect("Policy should not be None"),
                        )
                        .body(BodyForthcoming)
                        .unwrap();
                    responder
                        .take()
                        .expect("Responder should not be None at this point")
                        .start_response(response)
                });
                write_record(outgoing, vec![batch]).await.unwrap();
            }
            Err(err) => {
                eprintln!("Error receiving FlightData: {:?}", err);
            }
        }
    }
    Finished::finish(outgoing_body.unwrap(), Ok(()), None)
}

async fn handle_save_data(request: Request<IncomingBody>, responder: Responder) -> Finished {
    let mut policy = match request
        .headers()
        .get("X-Yak-Policy")
        .and_then(|v| v.to_str().ok())
    {
        Some(v) if !v.is_empty() => Some(v.to_string()),
        _ => {
            panic!("X-Yak-Policy header is required");
        }
    };

    let client = client::create_client().unwrap();
    client.start_connect().unwrap();
    let subscription = AsyncPollable::new(client.subscribe());
    subscription.wait_for().await;
    client.finish_connect().unwrap();

    client.start_handshake().unwrap();
    let subscription = AsyncPollable::new(client.subscribe());
    subscription.wait_for().await;
    client.finish_handshake().unwrap();

    client.start_do_put().unwrap();

    let incoming_body = request.into_body();

    let mut flight_data_stream = FlightDataStream::new(
        BodyDataStream::new(incoming_body)
            .map_err(|e| arrow_flight::error::FlightError::protocol(format!("error: {:?}", e))),
    );

    while let Some(data) = flight_data_stream.next().await {
        match data {
            Ok(flight_data) => {
                let flight_data = if let Some(policy) = policy.take() {
                    eprintln!("Using policy: {}", policy);
                    flight_data.with_app_metadata(policy.into_bytes())
                } else {
                    flight_data
                };
                let my_flight_data = MyFlightData {
                    data_header: flight_data.data_header.to_vec(),
                    app_metadata: flight_data.app_metadata.to_vec(),
                    data_body: flight_data.data_body.to_vec(),
                    flight_descriptor: None,
                };
                client.do_put(Some(&my_flight_data), false).unwrap();
            }
            Err(err) => {
                eprintln!("Error receiving FlightData: {:?}", err);
            }
        }
    }
    client.do_put(None, true).unwrap();

    let subscription = AsyncPollable::new(client.subscribe());
    subscription.wait_for().await;
    let incoming_put_response = client.finish_do_put().unwrap();
    let subscription = AsyncPollable::new(incoming_put_response.subscribe());
    subscription.wait_for().await;
    let tbl_name = match incoming_put_response.get() {
        Ok(Some(data)) => {
            eprintln!("Received PutResult: {:?}", data);
            String::from_utf8_lossy(&data.app_metadata).to_string()
        }
        Ok(None) => {
            panic!("No data received in PutResult");
        }
        Err(err) => {
            panic!("Error receiving PutResult: {:?}", err);
        }
    };

    let response = Response::builder()
        .status(200)
        .header("X-Yak-Table-Name", tbl_name)
        .body(BodyForthcoming)
        .unwrap();
    Finished::finish(responder.start_response(response), Ok(()), None)
}

#[wstd::http_server]
async fn main(request: Request<IncomingBody>, responder: Responder) -> Finished {
    let path = request.uri().path().strip_prefix("/").unwrap_or("");

    match path {
        "load_data" => handle_load_data(request, responder).await,
        "save_data" => handle_save_data(request, responder).await,
        _ => {
            eprintln!("Unknown path: {}", path);
            let response = Response::builder()
                .status(404)
                .body(BodyForthcoming)
                .unwrap();
            Finished::finish(responder.start_response(response), Ok(()), None)
        }
    }
}
