// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

macro_rules! jsonize {
    ($structname: ident) => {
        impl $structname {
            #[allow(dead_code)]
            pub fn to_json(self: &Self) -> String {
                serde_json::to_string(self).unwrap()
            }

            #[allow(dead_code)]
            pub fn from_json(json: &str) -> Self {
                serde_json::from_str(json).unwrap()
            }
        }
    };
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PolicyFile {
    #[serde(default)]
    pub rules: HashMap<String, PolicyRule>,
    #[serde(default)]
    pub func_policy: HashMap<String, FunctionPolicy>,
    #[serde(default)]
    pub default_table_verifier: String,
    #[serde(default)]
    pub table_verifiers: HashMap<String, TableVerifier>,
}
jsonize!(PolicyFile);

impl PolicyFile {
    pub fn new() -> Self {
        return Self {
            rules: HashMap::new(),
            func_policy: HashMap::new(),
            default_table_verifier: "".to_owned(),
            table_verifiers: HashMap::new(),
        };
    }

    pub fn get_rules_from_column_name(self: &Self, column_name: &str) -> Vec<(String, PolicyRule)> {
        let mut rules = vec![];

        for (k, v) in &self.rules {
            if v.column_name == column_name {
                rules.push((k.clone(), v.clone()));
            }
        }

        return rules;
    }

    pub fn get_function_policy(self: &Self, func_name: &str) -> HashMap<String, FunctionPolicy> {
        let mut res = HashMap::new();
        self.update_function_policy(&mut res, func_name);
        return res;
    }

    pub fn update_function_policy(
        self: &Self,
        hash_map: &mut HashMap<String, FunctionPolicy>,
        func_name: &str,
    ) {
        for (k, v) in &self.func_policy {
            if func_name == v.func {
                hash_map.insert(k.clone(), v.clone());
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct PolicyRule {
    pub column_name: String,
    pub requires: Vec<String>,
    pub rejects: Vec<String>,
    pub table_verifier: Option<String>,
}
jsonize!(PolicyRule);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionPolicy {
    pub func: String,
    #[serde(default)]
    pub reject: String,
    #[serde(default)]
    pub require: String,
}
jsonize!(FunctionPolicy);

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
pub struct TableVerifier {
    pub verifier: String,
    #[serde(default)]
    pub arg: String,
}
jsonize!(TableVerifier);
