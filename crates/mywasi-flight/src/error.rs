// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use crate::bindings::flight::types::ErrorCode;
use std::fmt;
use wasmtime_wasi::ResourceTableError;

pub type FlightResult<T> = Result<T, FlightError>;

#[repr(transparent)]
pub struct FlightError {
    err: anyhow::Error,
}

impl FlightError {
    /// Create a new `FlightError` that represents a trap.
    pub fn trap(err: impl Into<anyhow::Error>) -> FlightError {
        FlightError { err: err.into() }
    }

    /// Downcast this error to an [`ErrorCode`].
    pub fn downcast(self) -> anyhow::Result<ErrorCode> {
        self.err.downcast()
    }
}

impl From<ErrorCode> for FlightError {
    fn from(error: ErrorCode) -> Self {
        Self { err: error.into() }
    }
}

impl From<ResourceTableError> for FlightError {
    fn from(error: ResourceTableError) -> Self {
        FlightError::trap(error)
    }
}

impl fmt::Debug for FlightError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.err.fmt(f)
    }
}

impl fmt::Display for FlightError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.err.fmt(f)
    }
}

impl std::error::Error for FlightError {}
