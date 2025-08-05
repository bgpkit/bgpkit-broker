//! Error handling module.
use thiserror::Error;

/// Broker error enum.
#[derive(Error, Debug)]
pub enum BrokerError {
    #[error("NetworkError: {0}")]
    NetworkError(#[from] reqwest::Error),

    #[error("BrokerError: {0}")]
    BrokerError(String),

    #[error("ConfigurationError: {0}")]
    ConfigurationError(String),

    #[cfg(feature = "cli")]
    #[error("CrawlerError: {0}")]
    CrawlerError(String),

    #[cfg(feature = "cli")]
    #[error("IoError: {0}")]
    IoError(#[from] std::io::Error),

    #[cfg(feature = "cli")]
    #[error("ConfigConfigError: {0}")]
    ConfigJsonError(#[from] serde_json::Error),

    #[cfg(feature = "cli")]
    #[error("ConfigUnknownError: {0}")]
    ConfigUnknownError(String),

    #[error("DateTimeParseError: {0}")]
    DateTimeParseError(#[from] chrono::ParseError),

    #[cfg(feature = "backend")]
    #[error("DatabaseError: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[cfg(feature = "nats")]
    #[error("NotifierError: {0}")]
    NotifierError(String),
}
