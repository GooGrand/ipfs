use crate::ipfs_connector::Ipfs;
use anyhow::Result;
use db::Database;
use json::JsonValue;
use mysql::Value;
use sha256::digest;

pub mod db;
pub mod ipfs_connector;

pub struct App {
    pub db: Database,
    pub ipfs: Ipfs,
    pub last_hash: Option<String>,
}

impl App {
    pub fn new() -> Result<Self> {
        let db = Database::new()?;
        let ipfs = Ipfs::new();
        Ok(Self {
            db,
            ipfs,
            last_hash: None,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            let res = self.db.query_next_row()?;
            let hash = self.calculate_hash(&res, self.last_hash.clone());
            // create hash
            let json = Self::wrap_to_json_string(self.db.columns(), res, self.db.table_name());
            let cid = self.ipfs.add_file(json).await?;
            let ipns_key = self.ipfs.publish(&cid).await?;
            println!("IPNS key - {:?}", ipns_key);
            println!("Current CID - {:?}", cid);
            self.last_hash = Some(hash);
            println!("Current hash - {:?}", self.last_hash.as_ref());
        }
    }

    /// Function wraps row values to the json string with type:
    /// {
    ///     "table_name": "table",
    ///     "a": 1,
    ///     "b": "smth",
    ///     ...
    /// }
    ///
    ///  # Arguments
    ///
    /// * `columns` - A Vector of columnn names
    /// * `values` - A Vector of columnn value for given row
    /// * `table_name` - Table name
    ///
    /// Returns json string
    ///
    pub fn wrap_to_json_string(
        columns: Vec<String>,
        values: Vec<Value>,
        table_name: String,
    ) -> String {
        let mut data = json::JsonValue::new_object();
        data["table_name"] = JsonValue::String(table_name);
        for (id, item) in values.into_iter().enumerate() {
            data[columns[id].clone()] = Self::into_json_value(item);
        }
        data.dump()
    }

    // Mapper from mysql::Value to JsonValue. Probably, json lib is not necessary here, but it's easier
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
                let sign = if n { "-" } else { "" };
                JsonValue::String(format!("{} {}{}:{}:{}::{}", d, sign, h, m, s, ms))
            }
        }
    }

    // Necessary mapping, because mysql::from_value::<String>() could panic on not string types
    pub fn into_string(item: &Value) -> String {
        match item {
            Value::Bytes(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Value::Double(value) => value.to_string(),
            Value::Float(value) => value.to_string(),
            Value::Int(value) => value.to_string(),
            Value::UInt(value) => value.to_string(),
            Value::NULL => "null".to_string(),
            Value::Date(y, m, d, h, min, s, ms) => {
                format!("{}.{}.{}-{}:{}:{}::{}", y, m, d, h, min, s, ms)
            }
            Value::Time(n, d, h, m, s, ms) => {
                let sign = if *n { "-" } else { "" };
                format!("{} {}{}:{}:{}::{}", d, sign, h, m, s, ms)
            }
        }
    }

    /// Function compute hash for the given row in the way:
    /// [a, b, c ...] -> hash(hash(a) + hash(b) + hash(c) + previous_row_hash)
    /// Initial row hash - hash(hash(a) + hash(b) + hash(c))
    pub fn calculate_hash(&self, values: &Vec<Value>, prev_hash: Option<String>) -> String {
        if let Some(prev) = prev_hash {
            digest(
                values
                    .into_iter()
                    .map(|v| digest(Self::into_string(&v)))
                    .collect::<Vec<String>>()
                    .join("")
                    + &prev,
            )
        } else {
            digest(
                values
                    .into_iter()
                    .map(|v| digest(Self::into_string(&v)))
                    .collect::<Vec<String>>()
                    .join(""),
            )
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    App::new()?.run().await
}
