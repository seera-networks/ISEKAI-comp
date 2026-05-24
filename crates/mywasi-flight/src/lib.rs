// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

//! # Wasmtime's MyWASI Flight Implementation
//!
//! This crate is Wasmtime's host implementation of the `mywasi:flight` package.
//! This crate's implementation is primarily built on top of [`tonic`] and [`tokio`].

mod ctx;
mod error;
mod types_impl;

pub mod types;

pub mod bindings;

pub use crate::error::{FlightError, FlightResult};

pub use crate::ctx::{FlightCtx, FlightImpl, FlightView};
use wasmtime::component::HasData;

pub fn add_only_flight_to_linker_async<T>(
    l: &mut wasmtime::component::Linker<T>,
) -> anyhow::Result<()>
where
    T: FlightView + 'static,
{
    crate::bindings::flight::client::add_to_linker::<_, Flight<T>>(l, |x| FlightImpl(x))?;
    crate::bindings::flight::types::add_to_linker::<_, Flight<T>>(l, |x| FlightImpl(x))?;

    Ok(())
}

struct Flight<T>(T);

impl<T: 'static> HasData for Flight<T> {
    type Data<'a> = FlightImpl<&'a mut T>;
}