use anyhow::Context;
use anyhow::Result;
use json;
use json::JsonValue;
use mysql::prelude::Queryable;
use mysql::*;
use std::sync::atomic::AtomicU64;

pub struct Database {
    pool: Pool,
    table_name: String,
    schema: Vec<String>,
    next_row_id: AtomicU64,
}

impl Database {
    pub fn new() -> Result<Self> {
        let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
        let table_name = std::env::var("TABLE_NAME").context("TABLE_NAME must be set")?;
        let opts = Opts::from_url(&db_url)?;
        let pool = Pool::new(opts)?;
        let mut conn = pool.get_conn()?;
        let schema = Self::get_schema(&mut conn, &table_name)?;
        Ok(Self {
            pool,
            table_name,
            schema,
            next_row_id: AtomicU64::new(1),
        })
    }

    pub fn query_as_json(&self) -> Result<JsonValue> {
        let mut conn = self.pool.get_conn()?;
        // TODO handle last row id to not drop the service
        let row: Vec<Value> = conn
            .query(format!(
                "SELECT * FROM {} WHERE id = {};",
                self.table_name,
                self.next_row_id
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ))
            .map_err(|e| anyhow::anyhow!("{}", e.to_string()))?;
        let mut data = json::JsonValue::new_object();
        data["table_name"] = JsonValue::String(self.table_name.clone());
        for (id, item) in row.into_iter().enumerate() {
            data[self.schema[id].clone()] = into_json_value(item);
        }
        Ok(data)
    }

    pub fn get_schema(conn: &mut PooledConn, table_name: &str) -> Result<Vec<String>> {
        conn.query(format!("SHOW `columns` FROM `{}`;", table_name,))
            .map_err(|e| anyhow::anyhow!("{}", e.to_string()))
    }
}

pub fn into_json_value(item: Value) -> JsonValue {
    match item {
        Value::Bytes(bytes) => JsonValue::String(String::from_utf8_lossy(&bytes).into_owned()),
        Value::Double(value) => JsonValue::Number(value.into()),
        Value::Float(value) => JsonValue::Number(value.into()),
        Value::Int(value) => JsonValue::Number(value.into()),
        Value::UInt(value) => JsonValue::Number(value.into()),
        Value::NULL => JsonValue::Null,
        Value::Date(y, m, d, h, min, s, ms) => {
            JsonValue::String(format!("{}.{}.{}-{}:{}:{}::{}", y, m, d, h, min, s, ms))
        }
        Value::Time(n, d, h, m, s, ms) => {
            let sign = if n {"-"} else {""};
            JsonValue::String(format!("{} {}{}:{}:{}::{}", d, sign, h, m, s, ms))
        }
    }
}
