use std::io::Cursor;

use ipfs_api::IpfsApi;
use ipfs_api::IpfsClient;

pub struct Ipfs {
    client: IpfsClient,
}

impl Ipfs {
    pub fn new() -> Self {
        Self {
            client: IpfsClient::default(),
        }
    }

    /// Publishes uploaded file to IPNS
    ///
    ///  # Arguments
    ///
    /// * `cid` - A string representation of CID
    ///
    /// Returns `Ok(String)`, the string is the public key of IPNS
    pub async fn publish(&self, cid: &str) -> anyhow::Result<String> {
        let path = format!("/ipfs/{}", cid);
        self.client
            .name_publish(&path, false, Some("12h"), None, None)
            .await
            .map(|res| res.value)
            .map_err(|err| anyhow::anyhow!("{}", err.to_string()))
    }

    /// Upload file to IPFS
    ///
    ///  # Arguments
    ///
    /// * `json` - A string representation of json file, that will be uploaded
    ///
    /// Returns `Ok(String)`, the string is the CID of the file in IPFS
    pub async fn add_file(&self, json: String) -> anyhow::Result<String> {
        let data = Cursor::new(json);
        self.client
            .add(data)
            .await
            .map(|res| res.hash)
            .map_err(|err| anyhow::anyhow!("{}", err.to_string()))
    }
}
