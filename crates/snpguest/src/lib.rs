// SPDX-License-Identifier: Apache-2.0
// This is the main entry point of the snpguest utility. The CLI includes subcommands for requesting and managing certificates, displaying information, fetching derived keys, and verifying certificates and attestation reports.

mod certs;
pub mod fetch;
pub mod report;
mod verify;
pub mod verify2;

use anyhow::{Context, Result};
use clap::{arg, Parser, Subcommand, ValueEnum};
