use crate::{BrokerError, BrokerItem};

pub struct NatsNotifier {
    client: async_nats::Client,
}

fn item_to_subject(item: &BrokerItem) -> String {
    let project = match item.collector_id.starts_with("rrc") {
        true => "riperis",
        false => "route-views",
    };

    format!(
        "public.broker.{}.{}.{}",
        project, item.collector_id, item.data_type
    )
}

impl NatsNotifier {
    /// Creates a new NATS notifier.
    pub async fn new() -> Result<Self, BrokerError> {
        dotenvy::dotenv().ok();

        let url = match dotenvy::var("BGPKIT_BROKER_NATS_URL") {
            Ok(url) => url,
            Err(_) => {
                return Err(BrokerError::NotifierError(
                    "BGPKIT_BROKER_NATS_URL env variable not set".to_string(),
                ))
            }
        };
        let user = match dotenvy::var("BGPKIT_BROKER_NATS_USER") {
            Ok(user) => user,
            Err(_) => {
                return Err(BrokerError::NotifierError(
                    "BGPKIT_BROKER_NATS_USER env variable not set".to_string(),
                ))
            }
        };
        let pass = match dotenvy::var("BGPKIT_BROKER_NATS_PASS") {
            Ok(pass) => pass,
            Err(_) => {
                return Err(BrokerError::NotifierError(
                    "BGPKIT_BROKER_NATS_PASS env variable not set".to_string(),
                ))
            }
        };

        let client = match async_nats::ConnectOptions::with_user_and_password(user, pass)
            .connect(url)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                return Err(BrokerError::BrokerError(format!(
                    "NATS connection error: {}",
                    e
                )))
            }
        };

        Ok(Self { client })
    }

    /// Publishes broker items to NATS server.
    ///
    /// # Arguments
    ///
    /// * `items` - A reference to a vector of `BrokerItem` objects to be published.
    ///
    /// # Errors
    ///
    /// Returns an `async_nats::Error` if there was an error during the publishing process.
    pub async fn notify_items(&self, items: &Vec<BrokerItem>) -> Result<(), BrokerError> {
        for item in items {
            let item_str = serde_json::to_string(item)?;
            let subject = item_to_subject(item);
            if let Err(e) = self.client.publish(subject, item_str.into()).await {
                return Err(BrokerError::NotifierError(format!(
                    "NATS publish error: {}",
                    e
                )));
            }
        }
        if let Err(e) = self.client.flush().await {
            return Err(BrokerError::NotifierError(format!(
                "NATS flush error: {}",
                e
            )));
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection() {
        let notifier = NatsNotifier::new().await.unwrap();
        dbg!(notifier.client.connection_state());

        let item = BrokerItem {
            ts_start: Default::default(),
            ts_end: Default::default(),
            collector_id: "rrc99".to_string(),
            data_type: "rib".to_string(),
            url: "https://bgpkit.com".to_string(),
            rough_size: 100,
            exact_size: 101,
        };
        dbg!(notifier.notify_items(&vec![item]).await).ok();
    }
}
