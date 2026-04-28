use anyhow::Context;
use reqwest::Client;

#[derive(Debug, Clone)]
pub struct BlockfrostClient {
    client: Client,
    api_key: String,
    base_url: String,
}

impl BlockfrostClient {
    pub fn new(api_key: String) -> Self {
        BlockfrostClient {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("failed to build HTTP client"),
            api_key,
            base_url: "https://cardano-mainnet.blockfrost.io/api/v0".to_string(),
        }
    }

    /// Returns `true` if the tx is confirmed on-chain, `false` if 404 (orphan).
    pub async fn tx_exists(&self, tx_hash: &str) -> anyhow::Result<bool> {
        let url = format!("{}/txs/{}", self.base_url, tx_hash);
        let resp = self
            .client
            .get(&url)
            .header("project_id", &self.api_key)
            .send()
            .await
            .with_context(|| format!("HTTP request failed for tx {}", tx_hash))?;

        match resp.status().as_u16() {
            200 => Ok(true),
            404 => Ok(false),
            429 => anyhow::bail!("rate limited by Blockfrost (tx {})", tx_hash),
            code => anyhow::bail!("unexpected Blockfrost status {} for tx {}", code, tx_hash),
        }
    }
}
