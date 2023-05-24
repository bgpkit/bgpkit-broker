//! Error handling module.
use thiserror::Error;

/// Broker error enum.
#[derive(Error, Debug)]
pub enum BrokerError {
    #[error("NetworkError: {0}")]
    NetworkError(#[from] reqwest::Error),

    #[error("BrokerError: {0}")]
    BrokerError(String),

    #[cfg(feature = "crawler")]
    #[error("CrawlerError: {0}")]
    CrawlerError(String),

    #[error("DateTimeParseError: {0}")]
    DateTimeParseError(#[from] chrono::ParseError),

    #[cfg(feature = "db")]
    #[error("DbError: {0}")]
    DbError(#[from] duckdb::Error),
}
