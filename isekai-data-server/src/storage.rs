// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use crate::CmdOptions;
use anyhow::Context;
use arrow::array::{self, AsArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use isekai_utils::policy::PolicyFile;
use rand::rngs::OsRng;
use rand::RngCore;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn is_valid_sqlid(name: &str) -> bool {
    // A valid SQL identifier must start with a letter and can contain letters, digits, and underscores
    let re = regex::Regex::new(r"^[a-zA-Z][a-zA-Z0-9_]*$").unwrap();
    re.is_match(name)
}

const MAX_TARGET_GENERATION_ATTEMPTS: usize = 16;

fn ensure_storage_metadata_tables(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS policy (
            table_name TEXT NOT NULL,
            json TEXT NOT NULL
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS storage_schema (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            arrow_type TEXT NOT NULL,
            PRIMARY KEY (table_name, column_name)
        )",
        [],
    )?;
    Ok(())
}

fn generate_target() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let mut random_bytes = [0u8; 8];
    OsRng.fill_bytes(&mut random_bytes);
    let random = random_bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{}_{}_{}", now.as_secs(), now.subsec_nanos(), random)
}

fn table_exists(conn: &Connection, table_name: &str) -> anyhow::Result<bool> {
    let mut stmt =
        conn.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1")?;
    let exists = stmt
        .query_row(rusqlite::params![table_name], |_| Ok(()))
        .optional()?
        .is_some();
    Ok(exists)
}

fn arrow_to_sql_type(field_type: &DataType) -> &'static str {
    match field_type {
        DataType::Boolean | DataType::Int32 => "INTEGER",
        DataType::Float32 => "REAL",
        DataType::Utf8 => "TEXT",
        _ => "BLOB",
    }
}

fn arrow_type_name(field_type: &DataType) -> anyhow::Result<&'static str> {
    match field_type {
        DataType::Boolean => Ok("bool"),
        DataType::Int32 => Ok("int32"),
        DataType::Float32 => Ok("float32"),
        DataType::Utf8 => Ok("utf8"),
        DataType::Binary => Ok("binary"),
        _ => Err(anyhow::anyhow!("Unsupported column type: {:?}", field_type)),
    }
}

fn create_storage_in_tx(
    tx: &Transaction<'_>,
    subject: &str,
    schema: SchemaRef,
    policy: Option<String>,
) -> anyhow::Result<String> {
    ensure_storage_metadata_tables(tx)?;
    if !is_valid_sqlid(subject) {
        return Err(anyhow::anyhow!("Invalid subject name: {}", subject));
    }
    for _ in 0..MAX_TARGET_GENERATION_ATTEMPTS {
        let target = generate_target();
        let tbl_name = format!("{}_{}", subject, target);
        if table_exists(tx, &tbl_name)? {
            continue;
        }

        let mut sql = format!("CREATE TABLE {} (", tbl_name);
        for (i, field) in schema.fields.iter().enumerate() {
            let field_name = field.name();
            if !is_valid_sqlid(field_name) {
                return Err(anyhow::anyhow!("Invalid field name: {}", field_name));
            }
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!(
                "{} {}",
                field_name,
                arrow_to_sql_type(field.data_type())
            ));
        }
        sql.push(')');

        tx.execute(&sql, [])
            .with_context(|| format!("failed to create storage table {}", tbl_name))?;
        for field in schema.fields.iter() {
            let arrow_type = arrow_type_name(field.data_type())?;
            tx.execute(
                "INSERT INTO storage_schema (table_name, column_name, arrow_type) VALUES (?, ?, ?)",
                rusqlite::params![&tbl_name, field.name(), arrow_type],
            )
            .with_context(|| {
                format!(
                    "failed to persist schema metadata for {}.{}",
                    tbl_name,
                    field.name()
                )
            })?;
        }
        if let Some(policy) = policy {
            tx.execute(
                "INSERT INTO policy (table_name, json) VALUES (?, ?)",
                rusqlite::params![&tbl_name, &policy],
            )
            .with_context(|| format!("failed to persist policy for table {}", tbl_name))?;
        }
        return Ok(target);
    }

    Err(anyhow::anyhow!(
        "Failed to generate a unique storage target after {} attempts",
        MAX_TARGET_GENERATION_ATTEMPTS
    ))
}

pub fn create_storage(
    cmd_opts: &CmdOptions,
    subject: &str,
    schema: SchemaRef,
    policy: Option<String>,
) -> anyhow::Result<String> {
    let mut conn = Connection::open_with_flags(
        cmd_opts.storage_db.as_str(),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )?;
    let tx = conn.transaction()?;
    let target = create_storage_in_tx(&tx, subject, schema, policy)?;
    tx.commit()?;
    Ok(target)
}

enum StorageValue {
    Null,
    Boolean(bool),
    Int32(i32),
    Float32(f32),
    Utf8(String),
    Blob(Vec<u8>),
}

impl rusqlite::types::ToSql for StorageValue {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            StorageValue::Null => Ok(rusqlite::types::ToSqlOutput::from(rusqlite::types::Null)),
            StorageValue::Boolean(v) => {
                Ok(rusqlite::types::ToSqlOutput::from(if *v { 1 } else { 0 }))
            }
            StorageValue::Int32(v) => Ok(rusqlite::types::ToSqlOutput::from(*v)),
            StorageValue::Float32(v) => Ok(rusqlite::types::ToSqlOutput::from(*v)),
            StorageValue::Utf8(v) => Ok(rusqlite::types::ToSqlOutput::from(v.as_str())),
            StorageValue::Blob(v) => Ok(rusqlite::types::ToSqlOutput::from(v.as_slice())),
        }
    }
}

fn insert_batch_in_tx(
    tx: &Transaction<'_>,
    subject: &str,
    target: &str,
    batch: RecordBatch,
) -> anyhow::Result<()> {
    let tbl_name = format!("{}_{}", subject, target);
    if !is_valid_sqlid(&tbl_name) {
        return Err(anyhow::anyhow!("Invalid table name"));
    }

    for i in 0..batch.num_rows() {
        let mut params = Vec::new();

        let mut sql = format!("INSERT INTO {} (", tbl_name);
        for (field_idx, field) in batch.schema_ref().fields.iter().enumerate() {
            let field_name = field.name();
            if !is_valid_sqlid(field_name) {
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

            if column.is_null(i) {
                params.push(StorageValue::Null);
                continue;
            }

            match field_type {
                DataType::Boolean => {
                    let value = column.as_boolean().value(i);
                    params.push(StorageValue::Boolean(value));
                }
                DataType::Int32 => {
                    let value = column
                        .as_primitive::<arrow::datatypes::Int32Type>()
                        .value(i);
                    params.push(StorageValue::Int32(value));
                }
                DataType::Float32 => {
                    let value = column
                        .as_primitive::<arrow::datatypes::Float32Type>()
                        .value(i);
                    params.push(StorageValue::Float32(value));
                }
                DataType::Utf8 => {
                    let value = array::as_string_array(column).value(i);
                    params.push(StorageValue::Utf8(value.to_string()));
                }
                DataType::Binary => {
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

        let mut stmt = tx.prepare(&sql)?;
        stmt.execute(rusqlite::params_from_iter(params.iter()))?;
    }
    Ok(())
}

pub fn store_data(
    cmd_opts: &CmdOptions,
    subject: &str,
    schema: SchemaRef,
    policy: Option<String>,
    batches: Vec<RecordBatch>,
) -> anyhow::Result<String> {
    let mut conn = Connection::open_with_flags(
        cmd_opts.storage_db.as_str(),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )?;
    let tx = conn.transaction()?;
    let target = create_storage_in_tx(&tx, subject, schema, policy)?;
    for batch in batches {
        insert_batch_in_tx(&tx, subject, &target, batch)?;
    }
    tx.commit()?;
    Ok(target)
}

pub fn insert_data(
    cmd_opts: &CmdOptions,
    subject: &str,
    target: &str,
    batch: RecordBatch,
) -> anyhow::Result<()> {
    let mut conn = Connection::open_with_flags(
        cmd_opts.storage_db.as_str(),
        OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;
    let tx = conn.transaction()?;
    insert_batch_in_tx(&tx, subject, target, batch)?;
    tx.commit()?;
    Ok(())
}

fn get_column_type(
    conn: &Connection,
    tbl_name: &str,
    column_name: &str,
) -> anyhow::Result<DataType> {
    let mut stmt = conn.prepare(
        "SELECT arrow_type FROM storage_schema WHERE table_name = ? AND column_name = ?",
    )?;
    let arrow_type = stmt
        .query_row(rusqlite::params![tbl_name, column_name], |row| {
            row.get::<_, String>(0)
        })
        .optional()?;
    if let Some(arrow_type) = arrow_type {
        return match arrow_type.as_str() {
            "bool" => Ok(DataType::Boolean),
            "int32" => Ok(DataType::Int32),
            "float32" => Ok(DataType::Float32),
            "utf8" => Ok(DataType::Utf8),
            "binary" => Ok(DataType::Binary),
            _ => Err(anyhow::anyhow!(
                "Unsupported stored column type: {}",
                arrow_type
            )),
        };
    }

    let pragma = format!("PRAGMA table_info({})", tbl_name);
    let mut stmt = conn.prepare(&pragma)?;
    let declared_type = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })?
        .find_map(|row| match row {
            Ok((name, ty)) if name == column_name => Some(Ok(ty)),
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        })
        .transpose()?
        .ok_or_else(|| anyhow::anyhow!("Column {} not found in {}", column_name, tbl_name))?;
    match declared_type.as_str() {
        "INTEGER" => Ok(DataType::Int32),
        "REAL" => Ok(DataType::Float32),
        "TEXT" => Ok(DataType::Utf8),
        "BLOB" => Ok(DataType::Binary),
        _ => Err(anyhow::anyhow!(
            "Unsupported declared SQLite type {} for column {}",
            declared_type,
            column_name
        )),
    }
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
    let column_type = get_column_type(&conn, &tbl_name, column_name)?;

    let mut stmt = conn.prepare(&sql)?;
    let batch = match column_type {
        DataType::Boolean => {
            let values = stmt
                .query_map([], |row| row.get::<_, Option<i64>>(0))?
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|value| value.map(|value| value != 0))
                .collect::<Vec<_>>();
            let new_field = arrow_schema::Field::new(column_name, DataType::Boolean, true);
            let new_array = Arc::new(arrow::array::BooleanArray::from(values)) as _;
            RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(vec![new_field])),
                vec![new_array],
            )?
        }
        DataType::Int32 => {
            let values = stmt
                .query_map([], |row| row.get::<_, Option<i32>>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            let new_field = arrow_schema::Field::new(column_name, DataType::Int32, true);
            let new_array = Arc::new(arrow::array::Int32Array::from(values)) as _;
            RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(vec![new_field])),
                vec![new_array],
            )?
        }
        DataType::Float32 => {
            let values = stmt
                .query_map([], |row| row.get::<_, Option<f32>>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            let new_field = arrow_schema::Field::new(column_name, DataType::Float32, true);
            let new_array = Arc::new(arrow::array::Float32Array::from(values)) as _;
            RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(vec![new_field])),
                vec![new_array],
            )?
        }
        DataType::Utf8 => {
            let values = stmt
                .query_map([], |row| row.get::<_, Option<String>>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            let new_field = arrow_schema::Field::new(column_name, DataType::Utf8, true);
            let new_array = Arc::new(arrow::array::StringArray::from(values)) as _;
            RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(vec![new_field])),
                vec![new_array],
            )?
        }
        DataType::Binary => {
            let values = stmt
                .query_map([], |row| row.get::<_, Option<Vec<u8>>>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            let borrowed_values = values
                .iter()
                .map(|value| value.as_deref())
                .collect::<Vec<_>>();
            let new_field = arrow_schema::Field::new(column_name, DataType::Binary, true);
            let new_array = Arc::new(arrow::array::BinaryArray::from(borrowed_values)) as _;
            RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(vec![new_field])),
                vec![new_array],
            )?
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported Arrow type {:?} for column {}",
                column_type,
                column_name
            ));
        }
    };
    Ok(vec![batch])
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

#[cfg(test)]
mod tests {
    use super::{create_storage, get_data, insert_data};
    use crate::CmdOptions;
    use arrow::array::{BinaryArray, BooleanArray, Float32Array, Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_cmd_opts(storage_db: &str) -> CmdOptions {
        CmdOptions {
            no_tls: true,
            authorized_subject: None,
            csv_file: None,
            edinet_db: None,
            parquet_path: "./parquet".to_string(),
            storage_db: storage_db.to_string(),
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
    fn create_storage_generates_unique_targets() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("storage.db");
        let cmd_opts = test_cmd_opts(db_path.to_str().unwrap());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));

        let first = create_storage(&cmd_opts, "subject", schema.clone(), None).unwrap();
        let second = create_storage(&cmd_opts, "subject", schema, None).unwrap();

        assert_ne!(first, second);
    }

    #[test]
    fn store_data_preserves_arrow_types() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("storage.db");
        let cmd_opts = test_cmd_opts(db_path.to_str().unwrap());
        let schema = Arc::new(Schema::new(vec![
            Field::new("flag", DataType::Boolean, true),
            Field::new("count", DataType::Int32, true),
            Field::new("ratio", DataType::Float32, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("blob", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
                Arc::new(Float32Array::from(vec![Some(1.5), Some(2.5)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
                Arc::new(BinaryArray::from(vec![Some(&b"x"[..]), Some(&b"y"[..])])),
            ],
        )
        .unwrap();

        let target = create_storage(&cmd_opts, "subject", schema, None).unwrap();
        insert_data(&cmd_opts, "subject", &target, batch).unwrap();

        assert_eq!(
            get_data(&cmd_opts, "subject", &target, "flag").unwrap()[0]
                .schema_ref()
                .field(0)
                .data_type(),
            &DataType::Boolean
        );
        assert_eq!(
            get_data(&cmd_opts, "subject", &target, "count").unwrap()[0]
                .schema_ref()
                .field(0)
                .data_type(),
            &DataType::Int32
        );
        assert_eq!(
            get_data(&cmd_opts, "subject", &target, "ratio").unwrap()[0]
                .schema_ref()
                .field(0)
                .data_type(),
            &DataType::Float32
        );
        assert_eq!(
            get_data(&cmd_opts, "subject", &target, "label").unwrap()[0]
                .schema_ref()
                .field(0)
                .data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            get_data(&cmd_opts, "subject", &target, "blob").unwrap()[0]
                .schema_ref()
                .field(0)
                .data_type(),
            &DataType::Binary
        );
    }
}
