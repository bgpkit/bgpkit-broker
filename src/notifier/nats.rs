use crate::{BrokerError, BrokerItem};
use async_nats::Subscriber;
use futures::StreamExt;
use tracing::{error, info};

pub struct NatsNotifier {
    client: async_nats::Client,
    root_subject: String,
    subscriber: Option<Subscriber>,
}

fn item_to_subject(root_subject: &str, item: &BrokerItem) -> String {
    let project = match item.collector_id.starts_with("rrc") {
        true => "riperis",
        false => "route-views",
    };

    let subject = root_subject.strip_suffix('.').unwrap_or(root_subject);

    format!(
        "{}.{}.{}.{}",
        subject, project, item.collector_id, item.data_type
    )
}

impl NatsNotifier {
    /// Creates a new NATS notifier.
    pub async fn new(url: Option<String>) -> Result<Self, BrokerError> {
        dotenvy::dotenv().ok();

        let url = match url {
            None => match dotenvy::var("BGPKIT_BROKER_NATS_URL") {
                Ok(url) => url,
                Err(_) => {
                    return Err(BrokerError::NotifierError(
                        "BGPKIT_BROKER_NATS_URL env variable not set".to_string(),
                    ));
                }
            },
            Some(u) => u,
        };
        let user = dotenvy::var("BGPKIT_BROKER_NATS_USER").unwrap_or("public".to_string());
        let password = dotenvy::var("BGPKIT_BROKER_NATS_PASSWORD").unwrap_or("public".to_string());

        let root_subject = dotenvy::var("BGPKIT_BROKER_NATS_ROOT_SUBJECT")
            .unwrap_or_else(|_| "public.broker".to_string());

        let client = match async_nats::ConnectOptions::new()
            .user_and_password(user, password)
            .connect(url.clone())
            .await
        {
            Ok(c) => {
                info!(
                    "successfully connected to NATS server at {} with root subject '{}'",
                    &url, root_subject
                );
                c
            }
            Err(e) => {
                return Err(BrokerError::BrokerError(format!(
                    "NATS connection error: {}",
                    e
                )));
            }
        };

        Ok(Self {
            client,
            root_subject,
            subscriber: None,
        })
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
    pub async fn send(&self, items: &[BrokerItem]) -> Result<(), BrokerError> {
        for item in items {
            let item_str = serde_json::to_string(item)?;
            let subject = item_to_subject(self.root_subject.as_str(), item);
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

    pub async fn start_subscription(&mut self, subject: Option<String>) -> Result<(), BrokerError> {
        let sub = match subject {
            Some(s) => s,
            None => format!(
                "{}.>",
                self.root_subject
                    .strip_suffix('.')
                    .unwrap_or(self.root_subject.as_str())
            ),
        };

        match self.client.subscribe(sub.clone()).await {
            Ok(subscriber) => {
                info!("subscribed to NATS subject: {}", sub);
                self.subscriber = Some(subscriber);
                Ok(())
            }
            Err(e) => Err(BrokerError::BrokerError(format!(
                "NATS subscription error: {}",
                e
            ))),
        }
    }

    pub async fn next(&mut self) -> Option<BrokerItem> {
        match self.subscriber.as_mut() {
            None => None,
            Some(s) => match s.next().await {
                None => None,
                Some(msg) => {
                    let msg_text = std::str::from_utf8(msg.payload.as_ref()).unwrap();
                    match serde_json::from_str::<BrokerItem>(msg_text) {
                        Ok(item) => Some(item),
                        Err(_e) => {
                            error!("NATS message deserialization error: {}", msg_text);
                            None
                        }
                    }
                }
            },
        }
    }
}
