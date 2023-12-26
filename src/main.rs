use anyhow::{Context, Result};
use db::Database;
use ipfs_api::request::{DagCodec, DagPut};
use ipfs_api::{IpfsApi, IpfsClient};
use std::io::Cursor;

pub mod db;
pub mod ipfs_connector;

#[tokio::main]
async fn main() -> Result<()> {
    let _db = Database::new()?;
    let client = IpfsClient::default();
    let data = Cursor::new(r#"{ "hello" : "world" }"#);

    let dag_put = DagPut::builder().input_codec(DagCodec::Json).build();
    let _cid = client
        .dag_put_with_options(data, dag_put)
        .await?
        .cid
        .cid_string;

    // loop {
    //     let data = db.query_next_row()?;
    // }
    Ok(())
}
