// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use crate::CmdOptions;
use arrow::array::{self, AsArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use rusqlite::{Connection, OpenFlags};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use isekai_utils::policy::PolicyFile;

fn is_valid_sqlid(name: &str) -> bool {
    // A valid SQL identifier must start with a letter and can contain letters, digits, and underscores
    let re = regex::Regex::new(r"^[a-zA-Z][a-zA-Z0-9_]*$").unwrap();
    re.is_match(name)
}

pub fn create_storage(
    cmd_opts: &CmdOptions,
    subject: &str,
    schema: SchemaRef,
    policy: Option<String>,
) -> anyhow::Result<String> {
    let conn = Connection::open_with_flags(
        cmd_opts.storage_db.as_str(),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let target = format!("{}", now.as_secs());
    let tbl_name = format!("{}_{}", subject, target);
    if !is_valid_sqlid(&tbl_name) {
        return Err(anyhow::anyhow!("Invalid table name: {}", tbl_name));
    }
    let mut sql = format!("CREATE TABLE {} (", tbl_name);

    for (i, field) in schema.fields.iter().enumerate() {
        let field_name = field.name();
        if !is_valid_sqlid(&field_name) {
            return Err(anyhow::anyhow!("Invalid field name: {}", field_name));
        }
        let field_type = match field.data_type() {
            arrow_schema::DataType::Int32 => "INTEGER",
            arrow_schema::DataType::Float32 => "REAL",
            arrow_schema::DataType::Utf8 => "TEXT",
            _ => "BLOB", // Default to BLOB for unsupported types
        };
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("{} {}", field_name, field_type));
    }
    sql.push_str(")");

    conn.execute(&sql, [])?;

    // Update the policy table if a policy is provided
    if let Some(policy) = policy {
        let sql = "INSERT INTO policy (table_name, json) VALUES (?, ?)".to_string();
        let mut stmt = conn.prepare(&sql)?;
        stmt.execute(rusqlite::params![&tbl_name, &policy])?;
    }

    Ok(target)
}

enum StorageValue {
    Int32(i32),
    Float32(f32),
    Utf8(String),
    Blob(Vec<u8>),
}

impl rusqlite::types::ToSql for StorageValue {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            StorageValue::Int32(v) => Ok(rusqlite::types::ToSqlOutput::from(*v)),
            StorageValue::Float32(v) => Ok(rusqlite::types::ToSqlOutput::from(*v)),
            StorageValue::Utf8(v) => Ok(rusqlite::types::ToSqlOutput::from(v.as_str())),
            StorageValue::Blob(v) => Ok(rusqlite::types::ToSqlOutput::from(v.as_slice())),
        }
    }
}

pub fn insert_data(
    cmd_opts: &CmdOptions,
    subject: &str,
    target: &str,
    batch: RecordBatch,
) -> anyhow::Result<()> {
    let conn = Connection::open_with_flags(
        cmd_opts.storage_db.as_str(),
        OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;

    let tbl_name = format!("{}_{}", subject, target);
    if !is_valid_sqlid(&tbl_name) {
        return Err(anyhow::anyhow!("Invalid table name"));
    }

    for i in 0..batch.num_rows() {
        let mut params = Vec::new();

        let mut sql = format!("INSERT INTO {} (", tbl_name);
        for (field_idx, field) in batch.schema_ref().fields.iter().enumerate() {
            let field_name = field.name();
            if !is_valid_sqlid(&field_name) {
                return Err(anyhow::anyhow!("Invalid field name: {}", field_name));
            }
            if field_idx > 0 {
                sql.push_str(", ");
            }
            sql.push_str(field_name);
        }
        sql.push_str(") VALUES (");

        for (col_idx, column) in batch.columns().iter().enumerate() {
            let field = batch.schema_ref().field(col_idx);
            let field_type = field.data_type();

            if col_idx > 0 {
                sql.push_str(", ");
            }
            sql.push('?');
            match field_type {
                arrow_schema::DataType::Boolean => {
                    let value = column.as_boolean().value(i);
                    params.push(StorageValue::Int32(if value { 1 } else { 0 }));
                }
                arrow_schema::DataType::Int32 => {
                    let value = column
                        .as_primitive::<arrow::datatypes::Int32Type>()
                        .value(i);
                    params.push(StorageValue::Int32(value));
                }
                arrow_schema::DataType::Float32 => {
                    let value = column
                        .as_primitive::<arrow::datatypes::Float32Type>()
                        .value(i);
                    params.push(StorageValue::Float32(value));
                }
                arrow_schema::DataType::Utf8 => {
                    let value = array::as_string_array(column).value(i);
                    params.push(StorageValue::Utf8(value.to_string()));
                }
                arrow_schema::DataType::Binary => {
                    let value = column
                        .as_any()
                        .downcast_ref::<arrow::array::BinaryArray>()
                        .unwrap()
                        .value(i);
                    params.push(StorageValue::Blob(value.to_vec()));
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsupported column type: {:?} for field: {}",
                        field_type,
                        field.name()
                    ));
                }
            }
        }
        sql.push(')');

        let mut stmt = conn.prepare(&sql)?;
        stmt.execute(rusqlite::params_from_iter(params.iter()))?;
    }
    Ok(())
}

pub fn get_data(
    cmd_opts: &CmdOptions,
    subject: &str,
    target: &str,
    column_name: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    let conn = Connection::open_with_flags(
        cmd_opts.storage_db.as_str(),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;

    let tbl_name = format!("{}_{}", subject, target);
    if !is_valid_sqlid(&tbl_name) {
        return Err(anyhow::anyhow!("Invalid table name: {}", tbl_name));
    }
    if !is_valid_sqlid(column_name) {
        return Err(anyhow::anyhow!("Invalid column name: {}", column_name));
    }

    let sql = format!("SELECT {} FROM {}", column_name, tbl_name);

    let mut stmt = conn.prepare(&sql)?;
    let value_iter = stmt.query_map([], |row| {
        let value: Result<f32, rusqlite::Error> = row.get(0);
        match value {
            Ok(v) => Ok(StorageValue::Float32(v)),
            Err(_) => {
                let value: Result<String, rusqlite::Error> = row.get(0);
                match value {
                    Ok(v) => Ok(StorageValue::Utf8(v)),
                    Err(_) => {
                        let value: Result<Vec<u8>, rusqlite::Error> = row.get(0);
                        value.map(|v| StorageValue::Blob(v))
                    }
                }
            }
        }
    })?;

    let values = value_iter
        .filter_map(|v| v.ok())
        .collect::<Vec<StorageValue>>();
    if values.iter().any(|v| matches!(v, StorageValue::Float32(_))) {
        let new_field =
            arrow_schema::Field::new(column_name, arrow_schema::DataType::Float32, true);
        let mut new_values = Vec::new();

        for value in values {
            if let StorageValue::Float32(v) = value {
                new_values.push(Some(v));
            } else {
                new_values.push(None);
            }
        }

        let new_array = Arc::new(arrow::array::Float32Array::from(new_values)) as _;

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![new_field])),
            vec![new_array],
        )?;
        Ok(vec![batch])
    } else if values.iter().any(|v| matches!(v, StorageValue::Utf8(_))) {
        let new_field = arrow_schema::Field::new(column_name, arrow_schema::DataType::Utf8, true);
        let mut new_values = Vec::new();

        for value in values {
            if let StorageValue::Utf8(v) = value {
                new_values.push(Some(v));
            } else {
                new_values.push(None);
            }
        }

        let new_array = Arc::new(arrow::array::StringArray::from(new_values)) as _;

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![new_field])),
            vec![new_array],
        )?;
        Ok(vec![batch])
    } else {
        let new_field = arrow_schema::Field::new(column_name, arrow_schema::DataType::Binary, true);
        let mut new_values = Vec::new();

        for value in values.iter() {
            if let StorageValue::Blob(v) = value {
                new_values.push(Some(&v[..]));
            } else {
                new_values.push(None);
            }
        }

        let new_array = Arc::new(arrow::array::BinaryArray::from(new_values)) as _;

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![new_field])),
            vec![new_array],
        )?;
        Ok(vec![batch])
    }
}

pub fn get_policy(
    cmd_opts: &CmdOptions,
    subject: &str,
    target: &str,
    column_name: &str,
) -> anyhow::Result<String> {
    let tbl_name = format!("{}_{}", subject, target);

    let conn = rusqlite::Connection::open_with_flags(
        &cmd_opts.storage_db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;

    let sql = "SELECT json FROM policy WHERE table_name = ?".to_string();
    let mut stmt = conn.prepare(&sql)?;
    let entry_iter = stmt.query_map(rusqlite::params![tbl_name], |row| {
        let val: String = row.get(0)?;
        Ok(val)
    })?;
    if let Some(policy) = entry_iter.last() {
        let policy = policy?;
        let mut policy_file = PolicyFile::from_json(&policy);
        for (_, rule) in policy_file.rules.iter_mut() {
            rule.column_name = column_name.to_owned();
        }
        Ok(serde_json::to_string(&policy_file).unwrap())
    } else {
        Err(anyhow::anyhow!("No policy found for table: {}", tbl_name))
    }
}
