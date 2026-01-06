// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use super::jsonize;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct YakGrpcInput {
    pub executions: Vec<String>,
    pub dag: String,
}
jsonize!(YakGrpcInput);
