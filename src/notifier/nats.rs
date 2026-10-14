//! NATS notifier: publishes newly indexed broker items, one message per file.
//!
//! Enabled with the `nats` feature; configured through [`NatsConfig`]
//! (`BGPKIT_BROKER_NATS_*` environment variables). Notifications are emitted by
//! the long-running `serve` daemon only; the one-shot `update` and `bootstrap`
//! commands do not publish.
//!
//! Prerequisites: a JetStream stream whose subject filter covers `{root}.>`
//! must exist (for example a stream `FILES` on `bgpkit.file.>`); the broker
//! does not create streams, and publishes fail while no stream matches.
//!
//! The message ID is the file's canonical URL, so publisher retries within the
//! stream's duplicate window are deduplicated by the server.
//!
//! [`NatsConfig`]: crate::config::NatsConfig

use crate::config::NatsConfig;
use crate::{BrokerError, BrokerItem};

/// How many times a single item is (re)published before it is reported failed.
const PUBLISH_ATTEMPTS: u32 = 3;

/// Builds the per-item publish subject: `{root}.{project}.{collector}.{type}`.
pub fn subject_for(root_subject: &str, item: &BrokerItem) -> String {
    let project = if item.collector_id.starts_with("rrc") {
        "ripe-ris"
    } else {
        "route-views"
    };
    let root = root_subject.trim_end_matches('.');
    format!(
        "{}.{}.{}.{}",
        root, project, item.collector_id, item.data_type
    )
}

/// Publishes new broker items to a NATS JetStream stream.
pub struct NatsNotifier {
    client: async_nats::Client,
    jetstream: async_nats::jetstream::Context,
    root_subject: String,
}

impl NatsNotifier {
    /// Connects using a [`NatsConfig`].
    ///
    /// Returns `Ok(None)` when the configured URL is unset, which disables
    /// publishing without failing the caller.
    pub async fn from_config(config: &NatsConfig) -> Result<Option<Self>, BrokerError> {
        let url = match config.url.as_deref() {
            Some(url) => url,
            None => return Ok(None),
        };
        Ok(Some(
            Self::connect(
                url,
                &config.user,
                &config.password,
                config.root_subject.clone(),
            )
            .await?,
        ))
    }

    pub async fn connect(
        url: &str,
        user: &str,
        password: &str,
        root_subject: String,
    ) -> Result<Self, BrokerError> {
        let client = async_nats::ConnectOptions::new()
            .user_and_password(user.to_string(), password.to_string())
            .connect(url)
            .await
            .map_err(|e| BrokerError::NotifierError(format!("NATS connection error: {e}")))?;
        let jetstream = async_nats::jetstream::new(client.clone());
        Ok(Self {
            client,
            jetstream,
            root_subject,
        })
    }

    /// Publishes one message per item and waits for the JetStream acks.
    ///
    /// A failing item is retried a bounded number of times and then reported;
    /// it does not stop the remaining items of the batch. The returned error
    /// summarizes the failures so the caller can log them; durable delivery
    /// beyond this retry is intentionally out of scope (consumers reconcile
    /// against the catalog).
    pub async fn send(&self, items: &[BrokerItem]) -> Result<(), BrokerError> {
        let mut failures = 0usize;
        let mut first_error = String::new();
        for item in items {
            if let Err(e) = self.send_one(item).await {
                failures += 1;
                if first_error.is_empty() {
                    first_error = format!("{}: {}", item.url, e);
                }
            }
        }
        let flush_error = match self.client.flush().await {
            Ok(()) => None,
            Err(e) => Some(format!("flush error: {e}")),
        };
        let mut problems: Vec<String> = Vec::new();
        if failures > 0 {
            problems.push(format!(
                "{failures} of {} items failed to publish; first item error: {first_error}",
                items.len()
            ));
        }
        if let Some(flush_error) = flush_error {
            problems.push(flush_error);
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(BrokerError::NotifierError(problems.join("; ")))
        }
    }

    async fn send_one(&self, item: &BrokerItem) -> Result<(), BrokerError> {
        let subject = subject_for(&self.root_subject, item);
        let payload = serde_json::to_vec(item)
            .map_err(|e| BrokerError::NotifierError(format!("NATS payload error: {e}")))?;
        let mut last_error = String::new();
        for attempt in 1..=PUBLISH_ATTEMPTS {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", item.url.as_str());
            let result = match self
                .jetstream
                .publish_with_headers(subject.clone(), headers, payload.clone().into())
                .await
            {
                Ok(ack) => match ack.await {
                    Ok(_) => return Ok(()),
                    Err(e) => format!("ack error: {e}"),
                },
                Err(e) => format!("publish error: {e}"),
            };
            last_error = result;
            if attempt < PUBLISH_ATTEMPTS {
                tokio::time::sleep(std::time::Duration::from_millis(200 * u64::from(attempt)))
                    .await;
            }
        }
        Err(BrokerError::NotifierError(last_error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDateTime;

    fn item(collector_id: &str, data_type: &str) -> BrokerItem {
        BrokerItem {
            ts_start: NaiveDateTime::default(),
            ts_end: NaiveDateTime::default(),
            collector_id: collector_id.to_string(),
            data_type: data_type.to_string(),
            url: format!("https://example.com/{collector_id}/{data_type}.gz"),
            rough_size: 0,
            exact_size: 0,
        }
    }

    #[test]
    fn subject_uses_project_vocabulary() {
        assert_eq!(
            subject_for("bgpkit.file", &item("rrc00", "updates")),
            "bgpkit.file.ripe-ris.rrc00.updates"
        );
        assert_eq!(
            subject_for("bgpkit.file.", &item("route-views2", "rib")),
            "bgpkit.file.route-views.route-views2.rib"
        );
    }

    #[test]
    fn config_defaults_to_disabled() {
        let config = NatsConfig::default();
        assert!(config.url.is_none());
        assert_eq!(config.root_subject, "bgpkit.file");
    }
}
