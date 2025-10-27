// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use arrow::array::{Float32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rusqlite::{params, Connection, OpenFlags};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::OpenOptions;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Result, Status};
use isekai_utils::policy::{FunctionPolicy, PolicyFile, PolicyRule};

use crate::CmdOptions;

enum Value {
    Float32(f32),
    String(String),
}

pub fn get_data(cmd_opts: &CmdOptions, column_name: &str) -> Result<Vec<RecordBatch>> {
    // let mut state = STATE.lock().unwrap();
    let parts: Vec<&str> = column_name.split('/').collect();
    let (item, context, year) = match parts.len() {
        3 => (
            parts[0].to_string(),
            parts[1].to_string(),
            parts[2].to_string(),
        ),
        _ => {
            return Err(Status::invalid_argument(format!(
                "invalid column name: {}",
                column_name
            )));
        }
    };

    let year = if year.find('_').is_none() {
        format!("{}_0", year)
    } else {
        year
    };

    let filename = format!(
        "{}/{}-{}-{}.parquet",
        cmd_opts.parquet_path, item, context, year
    );

    let file = if let Ok(file) = OpenOptions::new().read(true).open(&filename) {
        file
    } else {
        let conn = Connection::open_with_flags(
            cmd_opts.edinet_db.as_deref().unwrap(),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .map_err(|e| Status::internal(format!("open_with_flags: {:?}", e)))?;

        let sql = format!(
            r#"
        SELECT ids.edinet_id, entries.closing_date, entries.value FROM ids
            LEFT JOIN entries
                ON ids.edinet_id = entries.edinet_id AND
                    entries.item = ? AND
                    entries.context = ?
        "#
        );

        let mut datas: VecDeque<(String, HashMap<String, Option<Value>>)> = VecDeque::new();

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| Status::internal(format!("prepare: {:?}", e)))?;
        let entry_iter = stmt
            .query_map(params![item.clone(), context.clone()], |row| {
                let edinet_id = String::from_utf8(row.get(0)?).expect("from_utf8");
                let closing_date =
                    String::from_utf8(row.get(1).unwrap_or(Vec::new())).expect("from_utf8");
                let value = row.get(2).unwrap_or("".to_string());
                Ok((edinet_id, closing_date, value))
            })
            .map_err(|e| Status::internal(format!("query_map: {:?}", e)))?;

        for entry in entry_iter {
            let (edinet_id, closing_date, value) =
                entry.map_err(|e| Status::internal(format!("Invalid entry: {:?}", e)))?;
            if closing_date.is_empty() || value.is_empty() {
                datas.push_back((edinet_id, HashMap::new()));
                continue;
            }
            let date = NaiveDate::parse_from_str(&closing_date, "%Y-%m-%d")
                .map_err(|e| Status::internal(format!("parse_from_str: {:?}", e)))?;
            let value = f32::from_str(&value)
                .map(|v| Some(Value::Float32(v)))
                .unwrap_or({
                    if !value.is_empty() {
                        Some(Value::String(value))
                    } else {
                        None
                    }
                });

            if datas.len() > 0 && &datas.back().unwrap().0 == &edinet_id {
                let mut idx = 0;
                let mut key = format!("{}_{}", date.year(), idx);
                loop {
                    if !datas.back().unwrap().1.contains_key(&key) {
                        break;
                    }
                    idx += 1;
                    key = format!("{}_{}", date.year(), idx);
                }
                datas.back_mut().unwrap().1.insert(key, value);
            } else {
                let mut new_value = HashMap::new();
                new_value.insert(format!("{}_0", date.year()), value);
                datas.push_back((edinet_id, new_value));
            }
        }

        let mut years = HashSet::new();
        for data in datas.iter() {
            for year in data.1.keys() {
                years.insert(year.clone());
            }
        }

        let is_float_column = datas.iter().any({
            |v| {
                v.1.values().any(|v2| match v2.as_ref() {
                    Some(Value::Float32(_)) => true,
                    _ => false,
                })
            }
        });

        for year in years {
            let title = format!("{}/{}/{}", item, context, year);
            let batch = if is_float_column {
                let field = Field::new(&title, DataType::Float32, true);
                let values: Vec<Option<f32>> = datas
                    .iter()
                    .map(|(_, values)| match values.get(&year) {
                        Some(values) => match values {
                            Some(Value::Float32(v)) => Some(*v),
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect();
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![field.clone()])),
                    vec![Arc::new(Float32Array::from(values))],
                )
                .map_err(|e| Status::internal(format!("failed to new RecordBatch: {:?}", e)))?
            } else {
                let field = Field::new(&title, DataType::Utf8, true);
                let values: Vec<Option<String>> = datas
                    .iter()
                    .map(|(_, values)| match values.get(&year) {
                        Some(values) => match values {
                            Some(Value::String(v)) => Some(v.clone()),
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect();
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![field.clone()])),
                    vec![Arc::new(StringArray::from(values))],
                )
                .map_err(|e| Status::internal(format!("failed to new RecordBatch: {:?}", e)))?
            };

            let filename = format!(
                "{}/{}-{}-{}.parquet",
                cmd_opts.parquet_path, item, context, year
            );
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(filename)
                .map_err(|e| Status::internal(format!("failed to open file: {:?}", e)))?;

            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();

            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

            writer
                .write(&batch)
                .map_err(|e| Status::internal(format!("failed to write: {:?}", e)))?;

            writer
                .close()
                .map_err(|e| Status::internal(format!("failed to close: {:?}", e)))?;
        }

        OpenOptions::new()
            .read(true)
            .open(&filename)
            .map_err(|e| Status::internal(format!("failed to open file: {:?}", e)))?
    };

    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Status::internal(format!("failed to new ParquetRecordBatchReader: {:?}", e)))?
        .build()
        .map_err(|e| {
            Status::internal(format!("failed to build ParquetRecordBatchReader: {:?}", e))
        })?;

    let mut batches = Vec::new();
    for batch in reader.by_ref() {
        batches
            .push(batch.map_err(|e| Status::internal(format!("failed to read batch: {:?}", e)))?);
    }
    Ok(batches)
}

pub fn get_policy(
    cmd_opts: &CmdOptions,
    subject: &str,
    column_name: &str,
) -> rusqlite::Result<String> {
    let parts: Vec<&str> = column_name.split('/').collect();
    let item = match parts.len() {
        3 => parts[0].to_string(),
        _ => column_name.to_string(),
    };

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
        return Ok(policy);
    }

    let sql = format!(
        r#"
        SELECT json FROM policy_by_item
            WHERE item = ?;
        "#
    );
    let mut stmt = conn.prepare(&sql)?;
    let entry_iter = stmt.query_map(rusqlite::params![item], |row| {
        let val: String = row.get(0)?;
        Ok(val)
    })?;
    if let Some(policy) = entry_iter.last().map(|x| x.unwrap()) {
        let mut policy = PolicyFile::from_json(&policy);
        policy.rules.get_mut(&item).map(|rule| {
            rule.column_name = column_name.to_owned();
        });
        Ok(serde_json::to_string(&policy).unwrap())
    } else {
        Ok(default_policy(column_name))
    }
}

pub fn default_policy(column_name: &str) -> String {
    let mut policy = PolicyFile::new();
    let rule = PolicyRule {
        column_name: column_name.to_owned(),
        rejects: Vec::new(),
        requires: vec!["mean_minimum_100".to_string()],
        table_verifier: None,
    };
    let func_policy = FunctionPolicy {
        func: "mean".to_string(),
        reject: "".to_string(),
        require: "{\"checkpoint\": \"input\", \"minimum_count\": 100}".to_string(),
    };
    policy
        .rules
        .insert("rule_mean_minimum_100".to_string(), rule);
    policy
        .func_policy
        .insert("mean_minimum_100".to_string(), func_policy);
    serde_json::to_string(&policy).unwrap()
}
