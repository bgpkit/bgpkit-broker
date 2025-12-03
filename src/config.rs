//! Configuration management for BGPKIT Broker.
//!
//! This module provides a centralized configuration struct that loads settings
//! from environment variables. All configuration is loaded once at startup
//! and can be displayed for logging purposes.

use std::fmt;
use std::time::Duration;

/// Default values for crawler configuration
const DEFAULT_CRAWLER_MAX_RETRIES: u32 = 3;
const DEFAULT_CRAWLER_BACKOFF_MS: u64 = 1000;
const DEFAULT_CRAWLER_COLLECTOR_CONCURRENCY: usize = 2;
const DEFAULT_CRAWLER_MONTH_CONCURRENCY: usize = 2;

/// Default values for backup configuration
const DEFAULT_BACKUP_INTERVAL_HOURS: u64 = 24;

/// Default values for database maintenance
const DEFAULT_META_RETENTION_DAYS: i64 = 30;

/// Crawler configuration settings.
///
/// Controls how the broker crawls BGP archive servers for MRT files.
#[derive(Debug, Clone)]
pub struct CrawlerConfig {
    /// Maximum number of retry attempts for failed HTTP requests.
    /// Environment variable: `BGPKIT_BROKER_CRAWLER_MAX_RETRIES`
    pub max_retries: u32,

    /// Initial backoff duration in milliseconds between retries.
    /// Environment variable: `BGPKIT_BROKER_CRAWLER_BACKOFF_MS`
    pub backoff_ms: u64,

    /// Number of collectors to crawl simultaneously.
    /// Environment variable: `BGPKIT_BROKER_CRAWLER_COLLECTOR_CONCURRENCY`
    pub collector_concurrency: usize,

    /// Number of months to crawl in parallel per collector.
    /// Primarily affects bootstrap crawls; regular updates typically only fetch 1-2 months.
    /// Environment variable: `BGPKIT_BROKER_CRAWLER_MONTH_CONCURRENCY`
    pub month_concurrency: usize,
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_CRAWLER_MAX_RETRIES,
            backoff_ms: DEFAULT_CRAWLER_BACKOFF_MS,
            collector_concurrency: DEFAULT_CRAWLER_COLLECTOR_CONCURRENCY,
            month_concurrency: DEFAULT_CRAWLER_MONTH_CONCURRENCY,
        }
    }
}

impl CrawlerConfig {
    /// Load crawler configuration from environment variables.
    pub fn from_env() -> Self {
        Self {
            max_retries: std::env::var("BGPKIT_BROKER_CRAWLER_MAX_RETRIES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_CRAWLER_MAX_RETRIES),
            backoff_ms: std::env::var("BGPKIT_BROKER_CRAWLER_BACKOFF_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_CRAWLER_BACKOFF_MS),
            collector_concurrency: std::env::var("BGPKIT_BROKER_CRAWLER_COLLECTOR_CONCURRENCY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_CRAWLER_COLLECTOR_CONCURRENCY),
            month_concurrency: std::env::var("BGPKIT_BROKER_CRAWLER_MONTH_CONCURRENCY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_CRAWLER_MONTH_CONCURRENCY),
        }
    }
}

impl fmt::Display for CrawlerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "collector_concurrency={}, month_concurrency={}, max_retries={}, backoff_ms={}",
            self.collector_concurrency, self.month_concurrency, self.max_retries, self.backoff_ms
        )
    }
}

/// Backup configuration settings.
///
/// Controls automatic database backups to local or S3 storage.
#[derive(Debug, Clone)]
pub struct BackupConfig {
    /// Destination path for backups (local path or S3 URL).
    /// Environment variable: `BGPKIT_BROKER_BACKUP_TO`
    pub destination: Option<String>,

    /// Interval between backups in hours.
    /// Environment variable: `BGPKIT_BROKER_BACKUP_INTERVAL_HOURS`
    pub interval_hours: u64,

    /// URL to ping on successful backup (for monitoring).
    /// Environment variable: `BGPKIT_BROKER_BACKUP_HEARTBEAT_URL`
    pub heartbeat_url: Option<String>,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            destination: None,
            interval_hours: DEFAULT_BACKUP_INTERVAL_HOURS,
            heartbeat_url: None,
        }
    }
}

impl BackupConfig {
    /// Load backup configuration from environment variables.
    pub fn from_env() -> Self {
        Self {
            destination: std::env::var("BGPKIT_BROKER_BACKUP_TO").ok(),
            interval_hours: std::env::var("BGPKIT_BROKER_BACKUP_INTERVAL_HOURS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_BACKUP_INTERVAL_HOURS),
            heartbeat_url: std::env::var("BGPKIT_BROKER_BACKUP_HEARTBEAT_URL").ok(),
        }
    }

    /// Returns true if backup is configured.
    pub fn is_enabled(&self) -> bool {
        self.destination.is_some()
    }

    /// Get the backup interval as a Duration.
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_hours * 60 * 60)
    }
}

/// Heartbeat configuration for monitoring.
#[derive(Debug, Clone, Default)]
pub struct HeartbeatConfig {
    /// General heartbeat URL (pinged after each update).
    /// Environment variable: `BGPKIT_BROKER_HEARTBEAT_URL`
    pub general_url: Option<String>,

    /// Backup heartbeat URL (pinged after each backup).
    /// Environment variable: `BGPKIT_BROKER_BACKUP_HEARTBEAT_URL`
    pub backup_url: Option<String>,
}

impl HeartbeatConfig {
    /// Load heartbeat configuration from environment variables.
    pub fn from_env() -> Self {
        Self {
            general_url: std::env::var("BGPKIT_BROKER_HEARTBEAT_URL").ok(),
            backup_url: std::env::var("BGPKIT_BROKER_BACKUP_HEARTBEAT_URL").ok(),
        }
    }

    /// Returns true if any heartbeat is configured.
    pub fn is_any_enabled(&self) -> bool {
        self.general_url.is_some() || self.backup_url.is_some()
    }
}

/// NATS notification configuration.
#[derive(Debug, Clone, Default)]
pub struct NatsConfig {
    /// NATS server URL.
    /// Environment variable: `BGPKIT_BROKER_NATS_URL`
    pub url: Option<String>,

    /// NATS username.
    /// Environment variable: `BGPKIT_BROKER_NATS_USER`
    pub user: Option<String>,

    /// NATS root subject for messages.
    /// Environment variable: `BGPKIT_BROKER_NATS_ROOT_SUBJECT`
    pub root_subject: Option<String>,
}

impl NatsConfig {
    /// Load NATS configuration from environment variables.
    pub fn from_env() -> Self {
        Self {
            url: std::env::var("BGPKIT_BROKER_NATS_URL").ok(),
            user: std::env::var("BGPKIT_BROKER_NATS_USER").ok(),
            root_subject: std::env::var("BGPKIT_BROKER_NATS_ROOT_SUBJECT").ok(),
        }
    }

    /// Returns true if NATS is configured.
    pub fn is_enabled(&self) -> bool {
        self.url.is_some()
    }
}

/// Database maintenance configuration.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Number of days to retain meta entries.
    /// Environment variable: `BGPKIT_BROKER_META_RETENTION_DAYS`
    pub meta_retention_days: i64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            meta_retention_days: DEFAULT_META_RETENTION_DAYS,
        }
    }
}

impl DatabaseConfig {
    /// Load database configuration from environment variables.
    pub fn from_env() -> Self {
        Self {
            meta_retention_days: std::env::var("BGPKIT_BROKER_META_RETENTION_DAYS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_META_RETENTION_DAYS),
        }
    }
}

/// Complete BGPKIT Broker configuration.
///
/// This struct aggregates all configuration settings and provides methods
/// for loading from environment variables and displaying configuration summaries.
#[derive(Debug, Clone, Default)]
pub struct BrokerConfig {
    /// Crawler settings
    pub crawler: CrawlerConfig,

    /// Backup settings
    pub backup: BackupConfig,

    /// Heartbeat settings
    pub heartbeat: HeartbeatConfig,

    /// NATS notification settings
    pub nats: NatsConfig,

    /// Database maintenance settings
    pub database: DatabaseConfig,
}

impl BrokerConfig {
    /// Create a new BrokerConfig with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Load all configuration from environment variables.
    pub fn from_env() -> Self {
        Self {
            crawler: CrawlerConfig::from_env(),
            backup: BackupConfig::from_env(),
            heartbeat: HeartbeatConfig::from_env(),
            nats: NatsConfig::from_env(),
            database: DatabaseConfig::from_env(),
        }
    }

    /// Display configuration summary for logging.
    ///
    /// Returns a vector of log lines suitable for info-level logging.
    pub fn display_summary(
        &self,
        do_update: bool,
        do_api: bool,
        update_interval: u64,
        host: &str,
        port: u16,
    ) -> Vec<String> {
        let mut lines = Vec::new();

        lines.push("=== BGPKIT Broker Configuration ===".to_string());

        // Update service status
        if do_update {
            lines.push(format!(
                "Periodic updates: ENABLED (interval: {} seconds)",
                update_interval
            ));
            lines.push(format!("Crawler config: {}", self.crawler));
        } else {
            lines.push("Periodic updates: DISABLED".to_string());
        }

        // API service status
        if do_api {
            lines.push(format!("API service: ENABLED ({}:{})", host, port));
        } else {
            lines.push("API service: DISABLED".to_string());
        }

        // Backup configuration
        if let Some(ref dest) = self.backup.destination {
            let is_s3 = oneio::s3_url_parse(dest).is_ok();
            let s3_ok = is_s3 && oneio::s3_env_check().is_ok();

            if is_s3 && !s3_ok {
                lines.push(format!(
                    "Backup: CONFIGURED to S3 ({}) every {} hours - WARNING: S3 env vars not set",
                    dest, self.backup.interval_hours
                ));
            } else if is_s3 {
                lines.push(format!(
                    "Backup: CONFIGURED to S3 ({}) every {} hours",
                    dest, self.backup.interval_hours
                ));
            } else {
                lines.push(format!(
                    "Backup: CONFIGURED to local path ({}) every {} hours",
                    dest, self.backup.interval_hours
                ));
            }
        } else {
            lines.push("Backup: DISABLED".to_string());
        }

        // Heartbeat configuration
        let general = self.heartbeat.general_url.is_some();
        let backup = self.heartbeat.backup_url.is_some();
        match (general, backup) {
            (true, true) => {
                lines.push("Heartbeats: CONFIGURED (both general and backup)".to_string())
            }
            (true, false) => lines.push("Heartbeats: CONFIGURED (general only)".to_string()),
            (false, true) => lines.push("Heartbeats: CONFIGURED (backup only)".to_string()),
            (false, false) => lines.push("Heartbeats: DISABLED".to_string()),
        }

        // NATS configuration
        if self.nats.is_enabled() {
            lines.push("NATS notifications: CONFIGURED".to_string());
        } else {
            lines.push("NATS notifications: DISABLED".to_string());
        }

        // Database maintenance
        lines.push(format!(
            "Database: meta_retention_days={}",
            self.database.meta_retention_days
        ));

        lines.push("=====================================".to_string());

        lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = BrokerConfig::default();
        assert_eq!(config.crawler.max_retries, 3);
        assert_eq!(config.crawler.backoff_ms, 1000);
        assert_eq!(config.crawler.collector_concurrency, 2);
        assert_eq!(config.crawler.month_concurrency, 2);
        assert_eq!(config.backup.interval_hours, 24);
        assert_eq!(config.database.meta_retention_days, 30);
        assert!(!config.backup.is_enabled());
        assert!(!config.nats.is_enabled());
    }

    #[test]
    fn test_crawler_config_display() {
        let config = CrawlerConfig::default();
        let display = format!("{}", config);
        assert!(display.contains("collector_concurrency=2"));
        assert!(display.contains("month_concurrency=2"));
        assert!(display.contains("max_retries=3"));
        assert!(display.contains("backoff_ms=1000"));
    }

    #[test]
    fn test_backup_interval() {
        let config = BackupConfig {
            destination: Some("test".to_string()),
            interval_hours: 12,
            heartbeat_url: None,
        };
        assert_eq!(config.interval(), Duration::from_secs(12 * 60 * 60));
    }
}
