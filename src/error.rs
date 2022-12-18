//! Error handling module.
use std::fmt::{Display, Formatter};

/// Broker error enum.
///
/// Includes two sub types:
/// 1. NetworkError: a wrapper around [ureq::Error] string representation, which would be from
/// making network requests or parsing return value to JSON.
/// 2. BrokerError: a String type returned from the BGPKIT Broker API.
#[derive(Debug)]
pub enum BrokerError {
    NetworkError(String),
    BrokerError(String),
}

impl Display for BrokerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BrokerError::NetworkError(e) => {write!(f, "NETWORK_ERROR: {}", e)}
            BrokerError::BrokerError(e) => {write!(f, "BROKER_ERROR: {}", e)}
        }
    }
}

impl From<ureq::Error> for BrokerError {
    fn from(e: ureq::Error) -> Self {
        BrokerError::NetworkError(e.to_string())
    }
}