#[cfg(feature = "nats")]
mod nats;

#[cfg(feature = "nats")]
pub use nats::NatsNotifier;
