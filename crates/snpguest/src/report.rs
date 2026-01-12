// SPDX-License-Identifier: Apache-2.0
// This file defines the CLI for requesting attestation reports. It contains functions for requesting attestation reports and saving them to files. Additionally, it includes code for reading and parsing attestation reports.

use super::*;

use std::{
    fs::{self, File},
    io::Write,
    path::PathBuf,
};
#[cfg(target_os = "linux")]
use std::{
    fs::OpenOptions,
    io::Read,
};

use anyhow::{anyhow, Result};
use rand::{thread_rng, RngCore};
use serde::{Deserialize, Serialize};
use sev::firmware::guest::AttestationReport;
#[cfg(target_os = "linux")]
use sev::firmware::guest::Firmware;

// Read a bin-formatted attestation report.
pub fn read_report(att_report_path: PathBuf) -> Result<AttestationReport, anyhow::Error> {
    let attestation_file = fs::File::open(att_report_path)?;

    let attestation_report = bincode::deserialize_from(attestation_file)
        .context("Could not parse attestation report.")?;

    Ok(attestation_report)
}

// Create 64 random bytes of data for attestation report request
pub fn create_random_request() -> [u8; 64] {
    let mut data = [0u8; 64];
    thread_rng().fill_bytes(&mut data);
    data
}

/// Report command to request an attestation report.
#[derive(Parser)]
pub struct ReportArgs {
    /// File to write the attestation report to.
    #[arg(value_name = "att-report-path", required = true)]
    pub att_report_path: PathBuf,

    /// Use random data for attestation report request. Writes data
    /// to ./random-request-file.txt by default, use --request to specify
    /// where to write data.
    #[arg(short, long, default_value_t = false, conflicts_with = "platform")]
    pub random: bool,

    /// Specify an integer VMPL level between 0 and 3 that the Guest is running on.
    #[arg(short, long, default_value = "1", value_name = "vmpl")]
    pub vmpl: Option<u32>,

    /// Provide file with data for attestation-report request. If provided
    /// with random flag, then the random data will be written in the
    /// provided path.
    #[arg(value_name = "request-file", required = true)]
    pub request_file: PathBuf,

    /// Expect that the 64-byte report data will already be provided by the platform provider.
    #[arg(short, long, conflicts_with = "random")]
    pub platform: bool,

    /// Test flag to generate attestation report
    #[arg(short, long, default_value_t = false)]
    pub dry: bool,
}

pub const TEST_ATT_REPORT: &'static [u8] = include_bytes!("../test/att.bin");
pub const TEST_REQ_DATA: &'static [u8] = include_bytes!("../test/req.bin");

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttestationConfig {
    pub proc_type: fetch::ProcType,
    pub endorsement: fetch::Endorsement,
}

pub struct ReportDirectArgs {
    pub proc_type: fetch::ProcType,
    pub endorsement: fetch::Endorsement,
    pub request_data: [u8; 64],
}

impl ReportDirectArgs {
    pub fn default() -> Self {
        let mut ret = ReportDirectArgs {
            proc_type: fetch::ProcType::Milan,
            endorsement: fetch::Endorsement::Vcek,
            request_data: [0u8; 64],
        };

        ret.request_data.copy_from_slice(&TEST_REQ_DATA[0..64]);

        return ret;
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExtendedAttestationReport {
    pub proc_type: fetch::ProcType,
    pub endorsement: fetch::Endorsement,
    pub vlek_pem: Option<Vec<u8>>,
    pub report: AttestationReport,
}

impl ReportArgs {
    pub fn verify(&self, hyperv: bool) -> Result<()> {
        if self.random && self.platform {
            return Err(anyhow!(
                "--random and --platform both enabled (not allowed). Consult man page."
            ));
        }

        if self.random && hyperv {
            return Err(anyhow!(
                "--random enabled yet Hyper-V guest detected (not allowed). Consult man page."
            ));
        }

        if self.platform && !hyperv {
            return Err(anyhow!("--platform enabled yet Hyper-V guest not detected (not allowed). Consult man page."));
        }

        Ok(())
    }
}

#[cfg(feature = "hyperv")]
fn request_hardware_report(
    _data: Option<[u8; 64]>,
    vmpl: Option<u32>,
) -> Result<AttestationReport> {
    hyperv::report::get(vmpl.unwrap_or(0))
}

#[cfg(target_os = "linux")]
#[cfg(not(feature = "hyperv"))]
fn request_hardware_report(data: Option<[u8; 64]>, vmpl: Option<u32>) -> Result<AttestationReport> {
    let mut fw = Firmware::open().context("unable to open /dev/sev-guest")?;
    fw.get_report(None, data, vmpl)
        .context("unable to fetch attestation report")
}

#[cfg(target_os = "linux")]
// Request attestation report and write it into a file
pub fn get_report(args: ReportArgs, hv: bool) -> Result<()> {
    args.verify(hv)?;

    let data: Option<[u8; 64]> = if args.random {
        Some(create_random_request())
    } else if args.platform {
        None
    } else {
        /*
         * Read from the request file.
         */
        let mut bytes = [0u8; 64];
        let mut file = File::open(&args.request_file)?;
        file.read_exact(&mut bytes)
            .context("unable to read 64 bytes from REQUEST_FILE")?;

        Some(bytes)
    };

    let report = if args.dry {
        let mut report = AttestationReport::default();
        if let Some(d) = data {
            report.report_data = d;
        }
        report
    } else {
        request_hardware_report(data, args.vmpl)?
    };

    /*
     * Serialize and write attestation report.
     */
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&args.att_report_path)?;

    bincode::serialize_into(&mut file, &report)
        .context("Could not serialize attestation report into file.")?;

    /*
     * Write reports report data (only for --random or --platform).
     */
    if args.random {
        reqdata_write(args.request_file, &report).context("unable to write random request data")?;
    } else if args.platform {
        reqdata_write(args.request_file, &report)
            .context("unable to write platform request data")?;
    }

    Ok(())
}

#[cfg(target_os = "linux")]
pub fn get_report_direct(direct_args: &ReportDirectArgs) -> Result<ExtendedAttestationReport> {
    let att = if direct_args.request_data.eq(&TEST_REQ_DATA[0..64]) {
        let att = bincode::deserialize(&TEST_ATT_REPORT)
            .context("Could not parse attestation report.")
            .unwrap();
        Ok(att)
    } else {
        request_hardware_report(Some(direct_args.request_data), None)
    }?;

    let mut ext_report = ExtendedAttestationReport {
        proc_type: direct_args.proc_type,
        endorsement: direct_args.endorsement,
        vlek_pem: None,
        report: att,
    };

    if direct_args.endorsement == fetch::Endorsement::Vlek {
        ext_report.vlek_pem = Some(certs::get_vlek_cert_pem()?);
    }

    Ok(ext_report)
    //Ok(bincode::serialize(&ext_report).unwrap())
}

#[cfg(target_os = "linux")]
fn reqdata_write(name: PathBuf, report: &AttestationReport) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(name)
        .context("unable to create or write to request data file")?;

    write_hex(&mut file, &report.report_data).context("unable to write report data to REQUEST_FILE")
}

pub fn write_hex(file: &mut File, data: &[u8]) -> Result<()> {
    let mut line_counter = 0;
    for val in data {
        // Make it blocks for easier read
        if line_counter.eq(&16) {
            writeln!(file)?;
            line_counter = 0;
        }

        write!(file, "{:02x}", val)?;
        line_counter += 1;
    }
    Ok(())
}
