use crate::{BrokerError, BrokerItem};

#[cfg(feature = "nats")]
mod nats;

pub struct Notifier {
    #[cfg(feature = "nats")]
    nats: Option<nats::NatsNotifier>,
}

impl Notifier {
    pub async fn new() -> Self {
        Self {
            #[cfg(feature = "nats")]
            nats: nats::NatsNotifier::new().await.ok(),
        }
    }

    pub async fn notify_items(&self, items: &Vec<BrokerItem>) -> Result<(), BrokerError> {
        #[cfg(feature = "nats")]
        if let Some(nats) = &self.nats {
            nats.notify_items(items).await?;
        }

        Ok(())
    }
}
