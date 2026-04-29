use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Serialize;
use sha2::Sha256;
use tracing::{error, warn};

use crate::pipeline::types::ParsedBlock;
use crate::sinks::EventSink;

type HmacSha256 = Hmac<Sha256>;

#[derive(Serialize)]
struct WebhookPayload<'a> {
    slot: u64,
    epoch: u64,
    timestamp: chrono::DateTime<chrono::Utc>,
    events: &'a Vec<crate::parser::GovEvent>,
}

#[derive(Serialize)]
struct RollbackPayload {
    rollback: bool,
    to_slot: u64,
}

pub struct WebhookSink {
    client: Client,
    url: String,
    secret: Option<String>,
}

impl WebhookSink {
    pub fn new(url: String, secret: Option<String>) -> Self {
        Self {
            client: Client::new(),
            url,
            secret,
        }
    }

    fn sign(&self, body: &[u8]) -> Option<String> {
        let secret = self.secret.as_deref()?;
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).ok()?;
        mac.update(body);
        let result = mac.finalize();
        Some(hex::encode(result.into_bytes()))
    }

    async fn post(&self, body: Vec<u8>) -> anyhow::Result<()> {
        let mut req = self.client.post(&self.url).body(body.clone()).header("Content-Type", "application/json");

        if let Some(sig) = self.sign(&body) {
            req = req.header("X-Signature-SHA256", sig);
        }

        let resp = req.send().await?;
        if !resp.status().is_success() {
            warn!("webhook-sink: {} returned {}", self.url, resp.status());
        }
        Ok(())
    }
}

#[async_trait]
impl EventSink for WebhookSink {
    async fn handle_block(&self, block: &ParsedBlock) -> anyhow::Result<()> {
        if block.events.is_empty() {
            return Ok(());
        }

        let payload = WebhookPayload {
            slot: block.slot,
            epoch: block.epoch,
            timestamp: block.timestamp,
            events: &block.events,
        };

        let body = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(e) => {
                error!("webhook-sink: serialize failed at slot {}: {}", block.slot, e);
                return Ok(());
            }
        };

        if let Err(e) = self.post(body).await {
            error!("webhook-sink: POST failed at slot {}: {}", block.slot, e);
        }

        Ok(())
    }

    async fn handle_rollback(&self, to_slot: u64) -> anyhow::Result<()> {
        let payload = RollbackPayload { rollback: true, to_slot };
        let body = serde_json::to_vec(&payload)?;
        if let Err(e) = self.post(body).await {
            error!("webhook-sink: rollback POST failed for slot {}: {}", to_slot, e);
        }
        Ok(())
    }
}
