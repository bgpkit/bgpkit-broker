//! Configuration management for BGPKIT Broker.
//!
//! This module provides a centralized configuration struct that loads settings
//! from environment variables. All configuration is loaded once at startup
//! and can be displayed for logging purposes.

use std::fmt;
use std::time::Duration;

use crate::db::sqlite_pool_config;

/// Default values for crawler configuration
const DEFAULT_CRAWLER_MAX_RETRIES: u32 = 3;
const DEFAULT_CRAWLER_BACKOFF_MS: u64 = 1000;
const DEFAULT_CRAWLER_COLLECTOR_CONCURRENCY: usize = 2;
const DEFAULT_CRAWLER_MONTH_CONCURRENCY: usize = 2;

/// Default values for backup configuration
const DEFAULT_BACKUP_INTERVAL_HOURS: u64 = 24;

/// Default values for database maintenance
const DEFAULT_META_RETENTION_DAYS: i64 = 30;
const DEFAULT_POSTGRES_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_DB_CONNECT_RETRIES: u32 = 10;
const DEFAULT_DB_CONNECT_BACKOFF_MS: u64 = 3000;

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

/// Selected Broker database backend.
///
/// SQLite remains the default deployment contract. PostgreSQL is selected only
/// when an explicit connection URL is supplied.
#[derive(Clone, PartialEq, Eq)]
pub enum DatabaseTarget {
    Sqlite(String),
    Postgres(String),
}

impl fmt::Debug for DatabaseTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(path) => formatter.debug_tuple("Sqlite").field(path).finish(),
            Self::Postgres(_) => formatter
                .debug_tuple("Postgres")
                .field(&"<redacted>")
                .finish(),
        }
    }
}

pub const DEFAULT_SQLITE_PATH: &str = "bgpkit-broker.sqlite3";

impl DatabaseTarget {
    /// Resolve the database target with a positional CLI value taking precedence
    /// over environment configuration. A positional path beginning with `pg://`,
    /// `postgres://`, or `postgresql://` selects PostgreSQL; any other path is
    /// a local SQLite file.
    pub fn resolve(
        database_path: Option<&str>,
        environment_postgres_url: Option<&str>,
        environment_sqlite_path: Option<&str>,
    ) -> Self {
        if let Some(path) = non_empty(database_path) {
            return postgres_url_from_path(path)
                .map(Self::Postgres)
                .unwrap_or_else(|| Self::Sqlite(path.to_string()));
        }
        if let Some(url) = non_empty(environment_postgres_url) {
            return Self::Postgres(normalize_postgres_url(url));
        }
        Self::Sqlite(
            non_empty(environment_sqlite_path)
                .unwrap_or(DEFAULT_SQLITE_PATH)
                .to_string(),
        )
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn postgres_url_from_path(path: &str) -> Option<String> {
    let lowercase_path = path.to_ascii_lowercase();
    (lowercase_path.starts_with("pg://")
        || lowercase_path.starts_with("postgres://")
        || lowercase_path.starts_with("postgresql://"))
    .then(|| normalize_postgres_url(path))
}

fn normalize_postgres_url(url: &str) -> String {
    url.get(..5)
        .filter(|prefix| prefix.eq_ignore_ascii_case("pg://"))
        .map(|_| format!("postgresql://{}", &url[5..]))
        .unwrap_or_else(|| url.to_string())
}

/// Database maintenance configuration.
#[derive(Clone)]
pub struct DatabaseConfig {
    /// Number of days to retain meta entries.
    /// Environment variable: `BGPKIT_BROKER_META_RETENTION_DAYS`
    pub meta_retention_days: i64,
    /// Explicit PostgreSQL connection URL. When unset, callers use SQLite.
    /// Environment variable: `BGPKIT_BROKER_POSTGRES_URL`
    pub postgres_url: Option<String>,
    /// SQLite catalog file path when no CLI database path or PostgreSQL URL is supplied.
    /// Environment variable: `BGPKIT_BROKER_SQLITE_PATH`
    pub sqlite_path: Option<String>,
    /// Maximum number of connections in the PostgreSQL pool.
    /// Environment variable: `BGPKIT_BROKER_POSTGRES_MAX_CONNECTIONS`
    pub postgres_max_connections: u32,
    /// Maximum connection attempts before the serve command gives up and exits.
    /// Environment variable: `BGPKIT_BROKER_DB_CONNECT_RETRIES`
    pub db_connect_retries: u32,
    /// Initial backoff between connection attempts, in milliseconds. The delay
    /// doubles after each failed attempt (1x, 2x, 4x, ...).
    /// Environment variable: `BGPKIT_BROKER_DB_CONNECT_BACKOFF_MS`
    pub db_connect_backoff_ms: u64,
}

impl fmt::Debug for DatabaseConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatabaseConfig")
            .field("meta_retention_days", &self.meta_retention_days)
            .field(
                "postgres_url",
                &self.postgres_url.as_ref().map(|_| "<redacted>"),
            )
            .field("sqlite_path", &self.sqlite_path)
            .field("postgres_max_connections", &self.postgres_max_connections)
            .finish()
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            meta_retention_days: DEFAULT_META_RETENTION_DAYS,
            postgres_url: None,
            sqlite_path: None,
            postgres_max_connections: DEFAULT_POSTGRES_MAX_CONNECTIONS,
            db_connect_retries: DEFAULT_DB_CONNECT_RETRIES,
            db_connect_backoff_ms: DEFAULT_DB_CONNECT_BACKOFF_MS,
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
            postgres_url: std::env::var("BGPKIT_BROKER_POSTGRES_URL").ok(),
            sqlite_path: std::env::var("BGPKIT_BROKER_SQLITE_PATH").ok(),
            postgres_max_connections: std::env::var("BGPKIT_BROKER_POSTGRES_MAX_CONNECTIONS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_POSTGRES_MAX_CONNECTIONS),
            db_connect_retries: std::env::var("BGPKIT_BROKER_DB_CONNECT_RETRIES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_DB_CONNECT_RETRIES),
            db_connect_backoff_ms: std::env::var("BGPKIT_BROKER_DB_CONNECT_BACKOFF_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_DB_CONNECT_BACKOFF_MS),
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
        database_target: &DatabaseTarget,
    ) -> Vec<String> {
        let mut lines = Vec::new();

        lines.push("=== BGPKIT Broker Configuration ===".to_string());

        // Database backend (the Debug impl redacts PostgreSQL URLs)
        lines.push(format!("Database backend: {:?}", database_target));

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

        // Database maintenance
        let (sqlite_max_connections, sqlite_cache_size_kib) = sqlite_pool_config();
        lines.push(format!(
            "Database: meta_retention_days={}, sqlite_max_connections={}, sqlite_cache_size_kib={}",
            self.database.meta_retention_days, sqlite_max_connections, sqlite_cache_size_kib
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
        assert_eq!(config.database.db_connect_retries, 10);
        assert_eq!(config.database.db_connect_backoff_ms, 3000);
        assert!(!config.backup.is_enabled());
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

    #[test]
    fn database_target_resolves_cli_path_and_environment_defaults() {
        assert_eq!(
            DatabaseTarget::resolve(None, None, None),
            DatabaseTarget::Sqlite("bgpkit-broker.sqlite3".to_string())
        );
        assert_eq!(
            DatabaseTarget::resolve(None, None, Some("/data/broker.sqlite3")),
            DatabaseTarget::Sqlite("/data/broker.sqlite3".to_string())
        );
        assert_eq!(
            DatabaseTarget::resolve(
                Some("/tmp/override.sqlite3"),
                Some("postgresql://broker@db/catalog"),
                Some("/data/broker.sqlite3"),
            ),
            DatabaseTarget::Sqlite("/tmp/override.sqlite3".to_string())
        );
        assert_eq!(
            DatabaseTarget::resolve(
                Some("pg://broker@db/catalog"),
                None,
                Some("/data/broker.sqlite3"),
            ),
            DatabaseTarget::Postgres("postgresql://broker@db/catalog".to_string())
        );
        assert_eq!(
            DatabaseTarget::resolve(
                Some("postgresql://broker@db/catalog"),
                None,
                Some("/data/broker.sqlite3"),
            ),
            DatabaseTarget::Postgres("postgresql://broker@db/catalog".to_string())
        );
        assert_eq!(
            DatabaseTarget::resolve(
                None,
                Some("postgresql://broker@db/environment"),
                Some("/data/broker.sqlite3"),
            ),
            DatabaseTarget::Postgres("postgresql://broker@db/environment".to_string())
        );
        assert_eq!(
            DatabaseTarget::resolve(None, Some("   "), Some("   ")),
            DatabaseTarget::Sqlite("bgpkit-broker.sqlite3".to_string())
        );
    }
}
