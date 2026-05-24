// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use arrow::array::{Float32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use isekai_utils::policy::PolicyFile;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex};
use tonic::{Result, Status};
use tracing::info;

use crate::CmdOptions;

struct State {
    data: DataStore,
}

enum Data {
    Float32Array(Float32Array),
    StringArray(StringArray),
}
struct DataStore {
    columns: HashMap<String, (Field, Data)>,
    loaded: bool,
}

impl DataStore {
    fn new() -> Self {
        Self {
            columns: HashMap::new(),
            loaded: false,
        }
    }
}

static STATE: LazyLock<Mutex<State>> = LazyLock::new(|| {
    Mutex::new(State {
        data: DataStore::new(),
    })
});

enum Value {
    Float32(f32),
    String(String),
}

pub fn get_data(cmd_opts: &CmdOptions, column_name: &str) -> Result<Vec<RecordBatch>> {
    let mut state = STATE.lock().unwrap();

    if !state.data.loaded {
        let csv_data = std::fs::read_to_string(cmd_opts.csv_file.as_ref().unwrap())
            .map_err(|e| Status::internal(format!("open csv: {:?}", e)))?;
        let mut data = Vec::<(String, Vec<Option<Value>>)>::new();
        let mut reader = csv::Reader::from_reader(csv_data.as_bytes());
        for r in reader
            .headers()
            .map_err(|e| Status::internal(format!("read header: {:?}", e)))?
        {
            data.push((r.to_string(), vec![]));
        }
        for r in reader.records() {
            let val =
                r.map_err(|e| Status::invalid_argument(format!("invalid csv row: {:?}", e)))?;
            if val.len() != data.len() {
                return Err(Status::invalid_argument(format!(
                    "invalid csv row: expected {} columns but got {}",
                    data.len(),
                    val.len()
                )));
            }
            for idx in 0..data.len() {
                let str = val.get(idx).ok_or_else(|| {
                    Status::invalid_argument(format!("missing column {} in csv row", idx))
                })?;
                let v = f32::from_str(str)
                    .map(|v| Some(Value::Float32(v)))
                    .unwrap_or_else(|_| {
                        if !str.is_empty() {
                            Some(Value::String(str.to_string()))
                        } else {
                            None
                        }
                    });
                data.get_mut(idx)
                    .ok_or_else(|| Status::internal(format!("missing destination column {}", idx)))?
                    .1
                    .push(v);
            }
        }

        for (col_name, value) in data {
            let is_float_column = value
                .iter()
                .any(|v| matches!(v.as_ref(), Some(Value::Float32(_))));
            if is_float_column {
                let field = Field::new(col_name.clone(), DataType::Float32, true);
                let new_value: Vec<Option<f32>> = value
                    .into_iter()
                    .map(|v| match v {
                        Some(Value::Float32(v)) => Some(v),
                        _ => None,
                    })
                    .collect();
                let array = Float32Array::from(new_value);
                state
                    .data
                    .columns
                    .insert(col_name, (field, Data::Float32Array(array)));
            } else {
                let field = Field::new(col_name.clone(), DataType::Utf8, true);
                let new_value: Vec<Option<String>> = value
                    .into_iter()
                    .map(|v| match v {
                        Some(Value::String(v)) => Some(v),
                        _ => None,
                    })
                    .collect();
                let array = StringArray::from(new_value);
                state
                    .data
                    .columns
                    .insert(col_name, (field, Data::StringArray(array)));
            }
        }

        info!("data loaded");
        state.data.loaded = true
    }

    let column = state
        .data
        .columns
        .get(column_name)
        .ok_or(Status::internal(format!(
            "column {} not found",
            column_name
        )))?;

    Ok(vec![RecordBatch::try_new(
        Arc::new(Schema::new(vec![column.0.clone()])),
        match &column.1 {
            Data::Float32Array(v) => vec![Arc::new(v.clone())],
            Data::StringArray(v) => vec![Arc::new(v.clone())],
        },
    )
    .map_err(|e| {
        Status::internal(format!("failed to new RecordBatch: {:?}", e))
    })?])
}

pub fn get_policy(
    cmd_opts: &CmdOptions,
    subject: &str,
    column_name: &str,
) -> rusqlite::Result<String> {
    let conn = rusqlite::Connection::open_with_flags(
        &cmd_opts.policy_db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let sql = r#"
        SELECT json FROM policy
            WHERE subject = ?;
        "#;
    let mut stmt = conn.prepare(sql)?;
    let entry_iter = stmt.query_map(rusqlite::params![subject], |row| {
        let val: String = row.get(0)?;
        Ok(val)
    })?;
    if let Some(policy) = entry_iter.last().transpose()? {
        Ok(policy)
    } else {
        Ok(default_policy(column_name))
    }
}

pub fn default_policy(_column_name: &str) -> String {
    let policy = PolicyFile::new();
    serde_json::to_string(&policy).unwrap()
}

#[cfg(test)]
mod tests {
    use super::get_data;
    use crate::CmdOptions;

    fn test_cmd_opts(csv_file: &str) -> CmdOptions {
        CmdOptions {
            no_tls: true,
            authorized_subject: None,
            csv_file: Some(csv_file.to_string()),
            edinet_db: None,
            parquet_path: "./parquet".to_string(),
            storage_db: "./storage.db".to_string(),
            policy_db: "./policy.db".to_string(),
            cert: "./certs/server.crt".to_string(),
            key: "./certs/server.key".to_string(),
            port: 50053,
            use_test_challenge: false,
            allow_test_subject: true,
            server_ld: None,
        }
    }

    #[test]
    fn get_data_returns_error_for_malformed_rows() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("bad.csv");
        std::fs::write(&csv_path, "a,b\n1\n").unwrap();
        let cmd_opts = test_cmd_opts(csv_path.to_str().unwrap());

        let err = get_data(&cmd_opts, "a").unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("invalid csv row"));
    }
}
