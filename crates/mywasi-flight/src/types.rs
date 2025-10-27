// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

//! Implements the base structure (i.e. [FlightCtx]) that will provide the
//! implementation of the flight API.

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, PutResult};
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tonic::codec::Streaming;
use tonic::transport::Channel;
use tonic::Response;
use wasmtime_wasi::async_trait;
use wasmtime_wasi::p2::Pollable;

pub enum FlightClientState {
    Default,
    Connecting(oneshot::Receiver<Result<Channel, tonic::transport::Error>>),
    ConnectReady(Result<Result<Channel, tonic::transport::Error>, RecvError>),
    Connected(FlightServiceClient<Channel>),
    HandshakeProgress(oneshot::Receiver<(FlightServiceClient<Channel>, tonic::Result<String>)>),
    HandshakeReady(Result<(FlightServiceClient<Channel>, tonic::Result<String>), RecvError>),
    DoGetProgress(
        oneshot::Receiver<(
            FlightServiceClient<Channel>,
            tonic::Result<Response<Streaming<FlightData>>>,
        )>,
    ),
    DoGetReady(
        Result<
            (
                FlightServiceClient<Channel>,
                tonic::Result<Response<Streaming<FlightData>>>,
            ),
            RecvError,
        >,
    ),
    DoPutSendProgress(
        mpsc::Sender<FlightData>,
        oneshot::Receiver<(
            FlightServiceClient<Channel>,
            tonic::Result<Response<Streaming<PutResult>>>,
        )>,
    ),
    DoPutRecvProgress(
        oneshot::Receiver<(
            FlightServiceClient<Channel>,
            tonic::Result<Response<Streaming<PutResult>>>,
        )>,
    ),
    DoPutReady(
        Result<
            (
                FlightServiceClient<Channel>,
                tonic::Result<Response<Streaming<PutResult>>>,
            ),
            RecvError,
        >,
    ),
    Closed,
}

/// The concrete type behind a `flight/flight-client` resource.
pub struct HostFlightClient {
    pub state: FlightClientState,
    pub token: Option<String>,
}

#[async_trait]
impl Pollable for HostFlightClient {
    async fn ready(&mut self) {
        match &mut self.state {
            FlightClientState::Connecting(rx) => {
                self.state = FlightClientState::ConnectReady(rx.await);
            }
            FlightClientState::HandshakeProgress(rx) => {
                self.state = FlightClientState::HandshakeReady(rx.await);
            }
            FlightClientState::DoGetProgress(rx) => {
                self.state = FlightClientState::DoGetReady(rx.await);
            }
            FlightClientState::DoPutRecvProgress(rx) => {
                self.state = FlightClientState::DoPutReady(rx.await);
            }
            _ => {}
        }
    }
}

/// The concrete type behind a `flight/flight-incoming-response` resource.
pub struct HostFlightIncomingResponse {
    pub stream: Streaming<FlightData>,
    pub pending_data: Option<Option<tonic::Result<FlightData>>>,
}

#[async_trait]
impl Pollable for HostFlightIncomingResponse {
    async fn ready(&mut self) {
        if self.pending_data.is_some() {
            return;
        }
        self.pending_data = Some(self.stream.next().await);
    }
}

/// The concrete type behind a `flight/flight-incoming-response` resource.
pub struct HostFlightIncomingPutResponse {
    pub stream: Streaming<PutResult>,
    pub pending_data: Option<Option<tonic::Result<PutResult>>>,
}

#[async_trait]
impl Pollable for HostFlightIncomingPutResponse {
    async fn ready(&mut self) {
        if self.pending_data.is_some() {
            return;
        }
        self.pending_data = Some(self.stream.next().await);
    }
}
