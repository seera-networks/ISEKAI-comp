// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, ready};
use prost::Message;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use wstd::http::body::{IncomingBody, OutgoingBody};
use wstd::io::{AsyncRead, AsyncWrite};

struct BodyDataStreamItemState {
    body: IncomingBody,
}
pub struct BodyDataStream {
    fut: Option<
        Pin<Box<dyn Future<Output = (BodyDataStreamItemState, Option<wstd::http::Result<Bytes>>)>>>,
    >,
}
unsafe impl Send for BodyDataStream {}
unsafe impl Sync for BodyDataStream {}

impl BodyDataStream {
    async fn compute_item(
        mut state: BodyDataStreamItemState,
    ) -> (BodyDataStreamItemState, Option<wstd::http::Result<Bytes>>) {
        let mut buf = vec![0; 4096];
        let len = match state.body.read(&mut buf).await {
            Ok(len) => len,
            Err(err) => {
                return (state, Some(Err(err.into())));
            }
        };
        if len == 0 {
            return (state, None);
        }
        let bytes = Bytes::copy_from_slice(&buf[0..len]);
        (state, Some(Ok(bytes)))
    }

    pub fn new(body: IncomingBody) -> Self {
        let init_state = BodyDataStreamItemState { body };
        Self {
            fut: Some(Box::pin(Self::compute_item(init_state))),
        }
    }
}

impl Stream for BodyDataStream {
    type Item = wstd::http::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.fut.is_none() {
            return Poll::Ready(None);
        }
        let (state, res) = ready!(this.fut.as_mut().unwrap().as_mut().poll(cx));
        match res {
            Some(Ok(bytes)) => {
                this.fut = Some(Box::pin(Self::compute_item(state)));
                Poll::Ready(Some(Ok(bytes)))
            }
            Some(Err(err)) => {
                this.fut = None;
                Poll::Ready(Some(Err(err.into())))
            }
            None => {
                this.fut = None;
                Poll::Ready(None)
            }
        }
    }
}

pub async fn write_record(
    body: &mut OutgoingBody,
    batches: Vec<RecordBatch>,
) -> anyhow::Result<()> {
    let input_stream = futures::stream::iter(batches.into_iter().map(Ok));
    let mut flight_data_stream = FlightDataEncoderBuilder::new().build(input_stream);
    while let Some(flight_data) = flight_data_stream.next().await {
        let flight_data = flight_data?;
        let mut buf = BytesMut::with_capacity(flight_data.encoded_len());
        flight_data.encode_length_delimited(&mut buf)?;
        let _written = body.write_all(&buf[0..]).await?;
        body.flush().await?;
    }
    Ok(())
}
