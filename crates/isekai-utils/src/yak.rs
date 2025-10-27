// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use super::jsonize_bytes;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct YakModule {
    pub header: YakModuleHeader,
    module: Vec<u8>,
    module_js: Vec<u8>,
    pub bundle_file: Vec<u8>,
    pub digest: Vec<u8>,
    pub signature: YakModuleSignature,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq, Clone)]
pub enum YakModuleType {
    #[default]
    WASI,
    Emscripten,
    Pyodide,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq, Clone)]
pub enum YakModuleKind {
    #[default]
    Function,
    Execution,
    Procedure,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq, Clone)]
pub enum BundleFileFormat {
    #[default]
    TarGz,
    Binary,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq, Clone)]
pub struct YakModuleHeader {
    pub module_type: YakModuleType,
    pub module_kind: YakModuleKind,
    pub module_file_name: String,
    pub module_js_file_name: String,
    module_length: usize,
    module_js_length: usize,
    bundle_file_format: BundleFileFormat,
    bundle_file_length: usize,

    pub ext_com: Option<ExternalCommunication>,
}
jsonize_bytes!(YakModuleHeader);

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq, Clone)]
pub struct ExternalCommunication {
    pub endpoint: String,
    pub use_tls: bool,
    pub ca_cert_pem: Option<Vec<u8>>,
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct YakModuleSignature {
    // secp256k1 with sha1
    pub signature: Vec<u8>,
}

impl YakModule {
    pub fn new(module: &Vec<u8>) -> YakModule {
        return YakModule {
            header: YakModuleHeader::default(),
            module: module.clone(),
            module_js: vec![],
            bundle_file: vec![],
            digest: vec![],
            signature: YakModuleSignature::default(),
        };
    }

    pub fn new_with_js(module: &Vec<u8>, module_js: &Vec<u8>) -> YakModule {
        let mut m = YakModule {
            header: YakModuleHeader::default(),
            module: module.clone(),
            module_js: module_js.clone(),
            bundle_file: vec![],
            digest: vec![],
            signature: YakModuleSignature::default(),
        };
        m.header.module_type = YakModuleType::Emscripten;
        return m;
    }

    pub fn new_pyodide() -> YakModule {
        let mut m = YakModule {
            header: YakModuleHeader::default(),
            module: vec![],
            module_js: vec![],
            bundle_file: vec![],
            digest: vec![],
            signature: YakModuleSignature::default(),
        };
        m.header.module_type = YakModuleType::Pyodide;
        return m;
    }

    pub fn module(&self) -> &[u8] {
        return &self.module;
    }

    pub fn module_js(&self) -> &[u8] {
        return &self.module_js;
    }

    pub fn verify(&self, pubkey: &str) -> Result<(), anyhow::Error> {
        let mut m = self.clone();
        let sig = m.signature.signature;

        let mut sig_file = tempfile::NamedTempFile::new().unwrap();
        sig_file.write(&sig).unwrap();
        sig_file.flush().unwrap();

        m.signature.signature = vec![];

        let mut cmd = std::process::Command::new("openssl")
            .arg("dgst")
            .arg("-signature")
            .arg(sig_file.path())
            .arg("-verify")
            .arg(pubkey)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        {
            let pipe_out = cmd.stdin.as_mut().unwrap();
            m.write(pipe_out).unwrap();
            pipe_out.flush().unwrap();
        }
        cmd.wait().unwrap();
        return Ok(());
    }

    pub fn load_from_file<P: AsRef<Path> + std::fmt::Debug + Copy>(
        path: P,
    ) -> Result<YakModule, anyhow::Error> {
        let mut f = File::open(path)
            .context(format!("failed to open {:?}", path))
            .unwrap();
        return Self::read(&mut f);
    }

    pub fn save_to_file<P: AsRef<Path>>(&mut self, path: P) -> Result<(), anyhow::Error> {
        let mut file = File::create(path)?;
        self.write(&mut file)
    }

    pub fn set_bundle_binary(&mut self, b: &[u8]) {
        self.bundle_file = b.to_vec();
        self.header.bundle_file_format = BundleFileFormat::Binary;
    }

    pub fn pack_dir_as_bundle_targz<P: AsRef<std::path::Path>>(
        &mut self,
        dir_path: P,
    ) -> Result<(), anyhow::Error> {
        let mut cmd = std::process::Command::new("tar")
            .arg("czf")
            .arg("/dev/fd/1")
            .arg("-C")
            .arg(dir_path.as_ref())
            .arg(".")
            .stdout(std::process::Stdio::piped())
            .spawn()?;
        let mut buf = [0u8; 4096];
        while let Ok(n) = cmd.stdout.as_mut().unwrap().read(&mut buf) {
            if n == 0 {
                break;
            }
            self.bundle_file.extend_from_slice(&buf[0..n]);
        }
        cmd.wait()?;

        Ok(())
    }

    pub fn unpack_bundle_targz<P: AsRef<std::path::Path>>(
        &self,
        dir_path: P,
    ) -> Result<(), anyhow::Error> {
        if self.header.bundle_file_length == 0 {
            return Ok(());
        }

        let mut cmd = std::process::Command::new("tar")
            .arg("xzf")
            .arg("-")
            .arg("-C")
            .arg(dir_path.as_ref())
            .stdin(std::process::Stdio::piped())
            .spawn()?;

        cmd.stdin.as_mut().unwrap().write(&self.bundle_file)?;
        cmd.wait()?;

        Ok(())
    }

    fn read<T: std::io::Read>(reader: &mut T) -> Result<YakModule, anyhow::Error> {
        let mut header_len_bytes: [u8; 4] = [0; 4];

        reader.read(&mut header_len_bytes).unwrap();
        let header_len = u32::from_le_bytes(header_len_bytes);
        let mut header_bytes = vec![0; header_len as usize];

        reader.read(&mut header_bytes).unwrap();
        let header = YakModuleHeader::from_json(&header_bytes);

        let mut module_bytes = vec![0; header.module_length];
        reader.read(&mut module_bytes).unwrap();

        let mut module_js_bytes = vec![0; header.module_js_length];
        reader.read(&mut module_js_bytes).unwrap();

        let mut bundle_file_bytes = vec![0; header.bundle_file_length];
        reader.read(&mut bundle_file_bytes).unwrap();

        let mut digest = vec![0; 32];
        reader.read(&mut digest).unwrap();

        let mut signature = vec![];
        reader.read_to_end(&mut signature).unwrap();

        let m = YakModule {
            header: header,
            module: module_bytes,
            module_js: module_js_bytes,
            bundle_file: bundle_file_bytes,
            digest: digest,
            signature: YakModuleSignature {
                signature: signature,
            },
        };

        return Ok(m);
    }

    fn write_content<T: std::io::Write>(&mut self, writer: &mut T) -> Result<(), anyhow::Error> {
        self.header.module_length = self.module.len();
        self.header.module_js_length = self.module_js.len();
        self.header.bundle_file_length = self.bundle_file.len();
        let header_bytes = self.header.to_json();
        let header_len_bytes = (header_bytes.len() as u32).to_le_bytes();

        writer.write(&header_len_bytes).unwrap();
        writer.write(&header_bytes).unwrap();
        writer.write(&self.module).unwrap();
        writer.write(&self.module_js).unwrap();
        writer.write(&self.bundle_file).unwrap();

        Ok(())
    }

    pub fn write<T: std::io::Write>(&mut self, writer: &mut T) -> Result<(), anyhow::Error> {
        let mut buf = vec![];
        self.write_content(&mut buf).unwrap();

        let mut hasher = Sha256::new();
        hasher.update(&buf);
        let res = hasher.finalize();
        self.digest = res.to_vec();

        writer.write(&buf).unwrap();
        writer.write(&self.digest).unwrap();
        writer.write(&self.signature.signature).unwrap();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::{YakModule, YakModuleHeader, YakModuleSignature};
    use bytes::Buf;

    #[test]
    fn write_and_read() {
        let mut m = YakModule {
            header: YakModuleHeader::default(),
            module: vec![0, 1, 2, 3],
            module_js: vec![4, 5, 6],
            bundle_file: vec![],
            digest: vec![],
            signature: YakModuleSignature::default(),
        };
        m.set_bundle_binary(&vec![7, 8, 9]);

        let mut buf = Vec::new();
        m.write(&mut buf).unwrap();
        let mut reader = buf.reader();
        let m2 = YakModule::read(&mut reader).unwrap();
        assert_eq!(&m, &m2);
    }

    #[test]
    fn save_and_load() {
        let dir = tempdir::TempDir::new("test-yak").unwrap();
        let fname = dir.path().join("test");

        let mut m = YakModule {
            header: YakModuleHeader::default(),
            module: vec![0, 1, 2, 3],
            module_js: vec![],
            bundle_file: vec![],
            digest: vec![],
            signature: YakModuleSignature::default(),
        };
        m.signature.signature = vec![4, 5, 6];
        m.set_bundle_binary(&vec![7, 8, 9]);

        m.save_to_file(&fname).unwrap();
        let m2 = YakModule::load_from_file(&fname).unwrap();
        assert_eq!(&m, &m2);
    }

    #[test]
    fn save_and_load_bundle_targz() {
        let dir = tempdir::TempDir::new("test-yak").unwrap();
        let fname = dir.path().join("test");

        let mut m = YakModule {
            header: YakModuleHeader::default(),
            module: vec![0, 1, 2, 3],
            module_js: vec![],
            bundle_file: vec![],
            digest: vec![],
            signature: YakModuleSignature::default(),
        };

        let test_body = vec![4, 5, 6, 7];

        let src_dir = tempfile::tempdir().unwrap();
        {
            let mut test_file = std::fs::File::create(src_dir.path().join("test.txt")).unwrap();
            test_file.write(&test_body).unwrap();
        }

        m.signature.signature = vec![4, 5, 6];
        m.pack_dir_as_bundle_targz(src_dir.path()).unwrap();

        m.save_to_file(&fname).unwrap();
        let m2 = YakModule::load_from_file(&fname).unwrap();
        assert_eq!(&m, &m2);

        let dst_dir = tempfile::tempdir().unwrap();
        m2.unpack_bundle_targz(dst_dir.path()).unwrap();
        let unpack_body = std::fs::read(dst_dir.path().join("test.txt")).unwrap();
        assert_eq!(&test_body, &unpack_body);
    }
}
