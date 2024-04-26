use crate::notifier::nats::NatsNotifier;
use crate::{BrokerError, BrokerItem};
use tracing::info;

#[cfg(feature = "nats")]
mod nats;

pub struct Notifier {
    #[cfg(feature = "nats")]
    nats: Option<NatsNotifier>,
}

impl Notifier {
    pub async fn new() -> Self {
        Self {
            #[cfg(feature = "nats")]
            nats: {
                match NatsNotifier::new().await {
                    Ok(n) => Some(n),
                    Err(e) => {
                        info!("NATS notifier not available: {}", e);
                        None
                    }
                }
            },
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
