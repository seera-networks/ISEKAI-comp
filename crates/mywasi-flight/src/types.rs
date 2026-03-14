// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

//! Implements the base structure (i.e. [FlightCtx]) that will provide the
//! implementation of the flight API.

use anyhow::Result;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, PutResult};
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tonic::codec::Streaming;
use tonic::transport::Channel;
use tonic::Response;
#[cfg(feature = "tonic-h3")]
use tonic_h3::{msquic_async::H3MsQuicAsyncConnector, H3Channel};
use wasmtime_wasi::async_trait;
use wasmtime_wasi::p2::Pollable;

pub enum FlightClientStateH2 {
    Default,
    Connecting(oneshot::Receiver<anyhow::Result<FlightServiceClient<Channel>>>),
    ConnectReady(Result<anyhow::Result<FlightServiceClient<Channel>>, RecvError>),
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

#[cfg(feature = "tonic-h3")]
pub enum FlightClientStateH3 {
    Default,
    Connecting(
        oneshot::Receiver<anyhow::Result<FlightServiceClient<H3Channel<H3MsQuicAsyncConnector>>>>,
    ),
    ConnectReady(
        Result<anyhow::Result<FlightServiceClient<H3Channel<H3MsQuicAsyncConnector>>>, RecvError>,
    ),
    Connected(FlightServiceClient<H3Channel<H3MsQuicAsyncConnector>>),
    HandshakeProgress(
        oneshot::Receiver<(
            FlightServiceClient<H3Channel<H3MsQuicAsyncConnector>>,
            tonic::Result<String>,
        )>,
    ),
    HandshakeReady(
        Result<
            (
                FlightServiceClient<H3Channel<H3MsQuicAsyncConnector>>,
                tonic::Result<String>,
            ),
            RecvError,
        >,
    ),
    DoGetProgress(
        oneshot::Receiver<(
            FlightServiceClient<H3Channel<H3MsQuicAsyncConnector>>,
            tonic::Result<Response<Streaming<FlightData>>>,
        )>,
    ),
    DoGetReady(
        Result<
            (
                FlightServiceClient<H3Channel<H3MsQuicAsyncConnector>>,
                tonic::Result<Response<Streaming<FlightData>>>,
            ),
            RecvError,
        >,
    ),
    DoPutSendProgress(
        mpsc::Sender<FlightData>,
        oneshot::Receiver<(
            FlightServiceClient<H3Channel<H3MsQuicAsyncConnector>>,
            tonic::Result<Response<Streaming<PutResult>>>,
        )>,
    ),
    DoPutRecvProgress(
        oneshot::Receiver<(
            FlightServiceClient<H3Channel<H3MsQuicAsyncConnector>>,
            tonic::Result<Response<Streaming<PutResult>>>,
        )>,
    ),
    DoPutReady(
        Result<
            (
                FlightServiceClient<H3Channel<H3MsQuicAsyncConnector>>,
                tonic::Result<Response<Streaming<PutResult>>>,
            ),
            RecvError,
        >,
    ),
    Closed,
}

pub enum FlightClientState {
    H2(FlightClientStateH2),
    #[cfg(feature = "tonic-h3")]
    H3(FlightClientStateH3),
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
            FlightClientState::H2(FlightClientStateH2::Connecting(rx)) => {
                self.state = FlightClientState::H2(FlightClientStateH2::ConnectReady(rx.await));
            }
            #[cfg(feature = "tonic-h3")]
            FlightClientState::H3(FlightClientStateH3::Connecting(rx)) => {
                self.state = FlightClientState::H3(FlightClientStateH3::ConnectReady(rx.await));
            }
            FlightClientState::H2(FlightClientStateH2::HandshakeProgress(rx)) => {
                self.state = FlightClientState::H2(FlightClientStateH2::HandshakeReady(rx.await));
            }
            #[cfg(feature = "tonic-h3")]
            FlightClientState::H3(FlightClientStateH3::HandshakeProgress(rx)) => {
                self.state = FlightClientState::H3(FlightClientStateH3::HandshakeReady(rx.await));
            }
            FlightClientState::H2(FlightClientStateH2::DoGetProgress(rx)) => {
                self.state = FlightClientState::H2(FlightClientStateH2::DoGetReady(rx.await));
            }
            #[cfg(feature = "tonic-h3")]
            FlightClientState::H3(FlightClientStateH3::DoGetProgress(rx)) => {
                self.state = FlightClientState::H3(FlightClientStateH3::DoGetReady(rx.await));
            }
            FlightClientState::H2(FlightClientStateH2::DoPutRecvProgress(rx)) => {
                self.state = FlightClientState::H2(FlightClientStateH2::DoPutReady(rx.await));
            }
            #[cfg(feature = "tonic-h3")]
            FlightClientState::H3(FlightClientStateH3::DoPutRecvProgress(rx)) => {
                self.state = FlightClientState::H3(FlightClientStateH3::DoPutReady(rx.await));
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
