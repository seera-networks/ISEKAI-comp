// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use arrow::array::{Float32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tonic::{Result, Status};
use isekai_utils::policy::PolicyFile;

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
        return Self {
            columns: HashMap::new(),
            loaded: false,
        };
    }
}

static STATE: Lazy<Mutex<State>> = Lazy::new(|| {
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
            let val = r.map_err(|e| Status::internal(format!("read record: {:?}", e)))?;
            for idx in 0..data.len() {
                let str = val.get(idx).unwrap();
                let v = f32::from_str(str)
                    .map(|v| Some(Value::Float32(v)))
                    .unwrap_or({
                        if !str.is_empty() {
                            Some(Value::String(val.get(idx).unwrap().to_string()))
                        } else {
                            None
                        }
                    });
                data.get_mut(idx).unwrap().1.push(v);
            }
        }

        for (col_name, value) in data {
            let is_float_column = value.iter().any({
                |v| match v.as_ref() {
                    Some(Value::Float32(_)) => true,
                    _ => false,
                }
            });
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
        println!("data loaded");
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
    let sql = format!(
        r#"
        SELECT json FROM policy
            WHERE subject = ?;
        "#
    );
    let mut stmt = conn.prepare(&sql)?;
    let entry_iter = stmt.query_map(rusqlite::params![subject], |row| {
        let val: String = row.get(0)?;
        Ok(val)
    })?;
    if let Some(policy) = entry_iter.last().map(|x| x.unwrap()) {
        Ok(policy)
    } else {
        Ok(default_policy(column_name))
    }
}

pub fn default_policy(_column_name: &str) -> String {
    let policy = PolicyFile::new();
    serde_json::to_string(&policy).unwrap()
}
