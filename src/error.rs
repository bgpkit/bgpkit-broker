//! Error handling module.
use std::convert;
use std::fmt::{Display, Formatter};

/// Broker error enum.
///
/// Includes two sub types:
/// 1. NetworkError: a wrapper around [reqwest::Error], which would be from making network requests
///     or parsing return value to JSON.
/// 2. BrokerError: a String type returned from the BGPKIT Broker API.
#[derive(Debug)]
pub enum BrokerError {
    NetworkError(reqwest::Error),
    BrokerError(String),
}

impl Display for BrokerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BrokerError::NetworkError(e) => {write!(f, "NETWORK_ERROR: {}", e.to_string())}
            BrokerError::BrokerError(e) => {write!(f, "BROKER_ERROR: {}", e)}
        }
    }
}

impl convert::From<reqwest::Error> for BrokerError {
    fn from(e: reqwest::Error) -> Self {
        BrokerError::NetworkError(e)
    }
}