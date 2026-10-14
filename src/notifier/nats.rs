//! NATS notifier: publishes newly indexed broker items, one message per file.
//!
//! Enabled with the `nats` feature. Environment configuration:
//! - `BGPKIT_BROKER_NATS_URL`: when unset or blank the notifier is disabled
//! - `BGPKIT_BROKER_NATS_USER`, `BGPKIT_BROKER_NATS_PASSWORD` (default `public`)
//! - `BGPKIT_BROKER_NATS_ROOT_SUBJECT` (default `bgpkit.file`)
//!
//! Messages are published through JetStream so that consumers can track their
//! own positions across restarts. The message ID is the file's canonical URL,
//! so publisher retries within the stream's duplicate window are deduplicated
//! by the server.

use crate::{BrokerError, BrokerItem};

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
    /// Connects using environment configuration.
    ///
    /// Returns `Ok(None)` when `BGPKIT_BROKER_NATS_URL` is not set, which
    /// disables publishing without failing the caller.
    pub async fn from_env() -> Result<Option<Self>, BrokerError> {
        let url = match std::env::var("BGPKIT_BROKER_NATS_URL") {
            Ok(url) if !url.trim().is_empty() => url,
            _ => return Ok(None),
        };
        let user =
            std::env::var("BGPKIT_BROKER_NATS_USER").unwrap_or_else(|_| "public".to_string());
        let password =
            std::env::var("BGPKIT_BROKER_NATS_PASSWORD").unwrap_or_else(|_| "public".to_string());
        let root_subject = std::env::var("BGPKIT_BROKER_NATS_ROOT_SUBJECT")
            .unwrap_or_else(|_| "bgpkit.file".to_string());
        Ok(Some(
            Self::connect(&url, &user, &password, root_subject).await?,
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
    pub async fn send(&self, items: &[BrokerItem]) -> Result<(), BrokerError> {
        for item in items {
            let subject = subject_for(&self.root_subject, item);
            let payload = serde_json::to_vec(item)
                .map_err(|e| BrokerError::NotifierError(format!("NATS payload error: {e}")))?;
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", item.url.as_str());
            let ack = self
                .jetstream
                .publish_with_headers(subject, headers, payload.into())
                .await
                .map_err(|e| BrokerError::NotifierError(format!("NATS publish error: {e}")))?;
            ack.await
                .map_err(|e| BrokerError::NotifierError(format!("NATS ack error: {e}")))?;
        }
        self.client
            .flush()
            .await
            .map_err(|e| BrokerError::NotifierError(format!("NATS flush error: {e}")))?;
        Ok(())
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
}
