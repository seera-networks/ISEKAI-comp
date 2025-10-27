// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

//! Raw bindings to the `flight` package.
#[allow(missing_docs)]
mod generated {
    use crate::types;

    wasmtime::component::bindgen!({
        path: "wit",
        world: "mywasi:flight/api",
        imports: { default: trappable },
        require_store_data_send: true,
        with: {
            "wasi:io": wasmtime_wasi::p2::bindings::io,
            "mywasi:flight/types/client": types::HostFlightClient,
            "mywasi:flight/types/incoming-response": types::HostFlightIncomingResponse,
            "mywasi:flight/types/incoming-put-response": types::HostFlightIncomingPutResponse,
        },
        trappable_error_type: {
            "mywasi:flight/types/error-code" => crate::error::FlightError,
        },
    });
}

pub use self::generated::mywasi::*;
