use anyhow::Context;
use anyhow::Result;
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

    /// Query next row from the given table
    ///
    /// Returns `Ok(Vec<mysql::Value>)`, the values of row with sql typed wrapper
    pub fn query_next_row(&self) -> Result<Vec<Value>> {
        let mut conn = self.pool.get_conn()?;
        // TODO handle last row id to not drop the service
        conn.query(format!(
            "SELECT * FROM {} WHERE id = {};",
            self.table_name,
            self.next_row_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
        .map_err(|e| anyhow::anyhow!("{}", e.to_string()))
    }

    /// Query column names for given table
    ///
    ///  # Arguments
    ///
    /// * `conn` - A connection to db
    /// * `table_name` - Table name
    ///
    /// Returns `Ok(Vec<String>)`, Vector of column names
    pub fn get_schema(conn: &mut PooledConn, table_name: &str) -> Result<Vec<String>> {
        conn.query(format!("SHOW `columns` FROM `{}`;", table_name,))
            .map_err(|e| anyhow::anyhow!("{}", e.to_string()))
    }

    pub fn columns(&self) -> Vec<String> {
        self.schema.clone()
    }

    pub fn table_name(&self) -> String {
        self.table_name.clone()
    }
}
