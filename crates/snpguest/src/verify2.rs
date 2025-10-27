use super::*;
use sev::firmware::host::CertType;
use std::io::Write;
use std::path::PathBuf;

#[derive(Parser)]
pub struct Args {
    /// Path to directory containing VCEK.
    #[arg(value_name = "certs-dir", required = true)]
    pub certs_dir: PathBuf,

    /// Path to attestation report to use for validation.
    #[arg(value_name = "ext-att-report-path", required = true)]
    pub ext_att_report_path: PathBuf,
}

pub async fn fetch_and_verify(args: Args) -> Result<()> {
    fetch_and_verify_async(args.certs_dir, args.ext_att_report_path, false).await
}

pub async fn fetch_and_verify_async(
    cert_dir: PathBuf,
    ext_att_path: PathBuf,
    quiet: bool,
) -> Result<()> {
    let ext_att_bin =
        std::fs::read(&ext_att_path).context("failed to read extended attestation reoprt")?;
    let ext_att: report::ExtendedAttestationReport =
        bincode::deserialize(&ext_att_bin).context("failed to decode ExtendedAttestationReport")?;
    let mut att_file = tempfile::NamedTempFile::new()?;
    att_file.write(&bincode::serialize(&ext_att.report)?)?;

    let (ark_path, _) = certs::get_cert_path(
        &cert_dir,
        &CertType::ARK,
        certs::CertFormat::Pem,
        &ext_att.endorsement,
    );
    let (ask_path, _) = certs::get_cert_path(
        &cert_dir,
        &CertType::ASK,
        certs::CertFormat::Pem,
        &ext_att.endorsement,
    );

    if !ark_path.exists() || !ask_path.exists() {
        if cert_dir.exists() {
            std::fs::remove_dir_all(&cert_dir).context("failed to remove certs dir")?;
        }
        std::fs::create_dir_all(&cert_dir).context("failed to create certs dir")?;

        let fetch_args = fetch::cert_authority::Args {
            encoding: certs::CertFormat::Pem,
            processor_model: ext_att.proc_type,
            certs_dir: cert_dir.clone(),
            endorser: ext_att.endorsement,
        };
        fetch::cert_authority::fetch_ca_async(fetch_args)
            .await
            .context("failed to fetch ark and ask certs")?;
    }

    match ext_att.endorsement {
        fetch::Endorsement::Vcek => {
            let (vcek_path, _) = certs::get_cert_path(
                &cert_dir,
                &CertType::VCEK,
                certs::CertFormat::Pem,
                &ext_att.endorsement,
            );
            if !vcek_path.exists() {
                let fetch_args = fetch::vcek::Args {
                    encoding: certs::CertFormat::Pem,
                    processor_model: ext_att.proc_type,
                    certs_dir: cert_dir.clone(),
                    att_report_path: att_file.path().to_path_buf(),
                };
                fetch::vcek::fetch_vcek_async(fetch_args)
                    .await
                    .context("failed to fetch vcek cert")?;
            }
        }
        fetch::Endorsement::Vlek => {
            let (vlek_path, _) = certs::get_cert_path(
                &cert_dir,
                &CertType::VLEK,
                certs::CertFormat::Pem,
                &ext_att.endorsement,
            );
            let mut vlek_file =
                std::fs::File::create(vlek_path).context("failed to create VLEK file")?;
            vlek_file
                .write(&ext_att.vlek_pem.context("VLEK is not specified")?)
                .context("failed to write VLEK certificate")?;
        }
    }

    // always validate certificate chain
    let verify_args = verify::certificate_chain::Args {
        certs_dir: cert_dir.clone(),
    };
    verify::certificate_chain::validate_cc(verify_args, true)
        .context("failed to validate certificate chain")?;

    // validate attestation report
    let verify_args = verify::attestation::Args {
        certs_dir: cert_dir.clone(),
        att_report_path: att_file.path().to_path_buf(),
        tcb: false,
        signature: false,
        dry: false,
        output_report_data: !quiet,
    };
    verify::attestation::verify_attestation(verify_args, true)
        .context("failed to validate attestation report")?;

    return Ok(());
}
