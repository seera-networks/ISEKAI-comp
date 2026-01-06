// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use super::jsonize;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::BufReader, path::Path};

#[derive(Serialize, Deserialize)]
pub struct Modules {
    pub procedure: Module,
    #[serde(default)]
    pub executions: Vec<Module>,
    #[serde(default)]
    pub http_executions: Vec<Module>,
    pub functions: Vec<Module>,
    #[serde(default)]
    pub http_functions: Vec<Module>,
    #[serde(default)]
    pub table_verifiers: Vec<Module>,
}
jsonize!(Modules);

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Module {
    pub name: String,
    pub path: String,
    #[serde(default)]
    pub verifier: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub home_path: Option<String>,
}

impl Modules {
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Modules> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let m: Modules = serde_json::from_reader(reader)?;
        Ok(m)
    }
}

#[derive(Serialize, Deserialize)]
pub struct ExecutionModules {
    pub executions: HashMap<String, ExecutionModule>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct ExecutionModule {
    pub kind: String,
    pub path: String,
}

impl ExecutionModules {
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<ExecutionModules> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let m: ExecutionModules = serde_json::from_reader(reader)?;
        Ok(m)
    }
}
