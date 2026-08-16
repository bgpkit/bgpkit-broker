use super::{DbSearchResult, LocalBrokerDb, PostgresDb, UpdatesMeta};
use crate::config::DatabaseTarget;
use crate::{BrokerError, BrokerItem, Collector};
use chrono::NaiveDateTime;
use std::time::Duration;
use tracing::{error, info};

#[derive(Clone)]
pub enum DatabaseBackend {
    Sqlite(LocalBrokerDb),
    Postgres(PostgresDb),
}

/// Connection retry parameters for the serve command.
#[derive(Debug, Clone, Copy)]
pub struct ConnectRetryConfig {
    /// Maximum number of connection attempts.
    pub max_attempts: u32,
    /// Initial delay between attempts; doubles after each failure.
    pub initial_backoff: Duration,
}

impl ConnectRetryConfig {
    /// Delay before the attempt with the given one-based index, assuming all
    /// previous attempts failed. Doubles after each failure and returns
    /// `None` once the wait would exceed `Duration::MAX`.
    fn backoff_before_attempt(&self, attempt: u32) -> Option<Duration> {
        if attempt == 0 {
            return Some(Duration::ZERO);
        }
        let mut delay = self.initial_backoff;
        for _ in 1..attempt {
            delay = delay.checked_mul(2)?;
        }
        Some(delay)
    }
}

impl DatabaseBackend {
    pub async fn connect(
        target: &DatabaseTarget,
        max_connections: u32,
    ) -> Result<Self, BrokerError> {
        match target {
            DatabaseTarget::Sqlite(path) => LocalBrokerDb::new(path).await.map(Self::Sqlite),
            DatabaseTarget::Postgres(url) => PostgresDb::new(url, max_connections)
                .await
                .map(Self::Postgres),
        }
    }

    /// Connect to the database, retrying with exponential backoff while the
    /// server is unavailable. The serve command is expected to survive database
    /// restarts (for example a PostgreSQL maintenance reboot), so a transient
    /// connect failure must not kill the process.
    pub async fn connect_with_retry(
        target: &DatabaseTarget,
        max_connections: u32,
        retry: ConnectRetryConfig,
    ) -> Result<Self, BrokerError> {
        let mut attempt: u32 = 0;
        loop {
            match Self::connect(target, max_connections).await {
                Ok(db) => {
                    if attempt > 0 {
                        info!("database connection established on attempt {}", attempt + 1);
                    }
                    return Ok(db);
                }
                Err(e) => {
                    if attempt + 1 >= retry.max_attempts {
                        error!(
                            "failed to connect to database after {} attempts: {}",
                            retry.max_attempts, e
                        );
                        return Err(e);
                    }
                    attempt += 1;
                    match retry.backoff_before_attempt(attempt) {
                        Some(backoff) => {
                            error!(
                                "failed to connect to database (attempt {}/{}): {}; retrying in {:?}",
                                attempt, retry.max_attempts, e, backoff
                            );
                            tokio::time::sleep(backoff).await;
                        }
                        None => {
                            // Exponential growth overflowed; cap the wait instead
                            // of spinning without delay.
                            let backoff = retry.initial_backoff;
                            error!(
                                "database connect backoff overflowed at attempt {}; retrying in {:?}",
                                attempt, backoff
                            );
                            tokio::time::sleep(backoff).await;
                        }
                    }
                }
            }
        }
    }

    pub async fn reload_collectors(&mut self) -> Result<(), BrokerError> {
        match self {
            Self::Sqlite(database) => {
                database.reload_collectors().await;
                Ok(())
            }
            Self::Postgres(database) => database.reload_collectors().await,
        }
    }

    pub async fn analyze(&self) -> Result<(), BrokerError> {
        match self {
            Self::Sqlite(database) => database.analyze().await,
            Self::Postgres(database) => database.analyze().await,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn search(
        &self,
        collectors: Option<Vec<String>>,
        project: Option<String>,
        data_type: Option<String>,
        ts_start: Option<NaiveDateTime>,
        ts_end: Option<NaiveDateTime>,
        page: Option<usize>,
        page_size: Option<usize>,
    ) -> Result<DbSearchResult, BrokerError> {
        match self {
            Self::Sqlite(database) => {
                database
                    .search(
                        collectors, project, data_type, ts_start, ts_end, page, page_size,
                    )
                    .await
            }
            Self::Postgres(database) => {
                database
                    .search(
                        collectors, project, data_type, ts_start, ts_end, page, page_size,
                    )
                    .await
            }
        }
    }

    pub async fn insert_items(
        &self,
        items: &[BrokerItem],
        update_latest: bool,
    ) -> Result<Vec<BrokerItem>, BrokerError> {
        match self {
            Self::Sqlite(database) => database.insert_items(items, update_latest).await,
            Self::Postgres(database) => database.insert_items(items, update_latest).await,
        }
    }

    pub async fn insert_collector(&self, collector: &Collector) -> Result<(), BrokerError> {
        match self {
            Self::Sqlite(database) => database.insert_collector(collector).await,
            Self::Postgres(database) => database.insert_collector(collector).await,
        }
    }

    pub async fn get_latest_timestamp(&self) -> Result<Option<NaiveDateTime>, BrokerError> {
        match self {
            Self::Sqlite(database) => database.get_latest_timestamp().await,
            Self::Postgres(database) => database.get_latest_timestamp().await,
        }
    }

    pub async fn get_latest_files(&self) -> Result<Vec<BrokerItem>, BrokerError> {
        match self {
            Self::Sqlite(database) => database.get_latest_files().await,
            Self::Postgres(database) => database.get_latest_files().await,
        }
    }

    pub async fn update_latest_files(
        &self,
        files: &[BrokerItem],
        bootstrap: bool,
    ) -> Result<(), BrokerError> {
        match self {
            Self::Sqlite(database) => {
                database.update_latest_files(files, bootstrap).await;
                Ok(())
            }
            Self::Postgres(database) => database.update_latest_files(files, bootstrap).await,
        }
    }

    pub async fn insert_meta(
        &self,
        crawl_duration: i32,
        item_inserted: i32,
    ) -> Result<Vec<UpdatesMeta>, BrokerError> {
        match self {
            Self::Sqlite(database) => database.insert_meta(crawl_duration, item_inserted).await,
            Self::Postgres(database) => database.insert_meta(crawl_duration, item_inserted).await,
        }
    }

    pub async fn get_latest_updates_meta(&self) -> Result<Option<UpdatesMeta>, BrokerError> {
        match self {
            Self::Sqlite(database) => database.get_latest_updates_meta().await,
            Self::Postgres(database) => database.get_latest_updates_meta().await,
        }
    }

    pub async fn cleanup_old_meta_entries(&self, retention_days: i64) -> Result<(), BrokerError> {
        match self {
            Self::Sqlite(database) => database.cleanup_old_meta_entries().await.map(|_| ()),
            Self::Postgres(database) => database.cleanup_old_meta_entries(retention_days).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_with_retry_exhausts_attempts_and_returns_error() {
        // A database path under a regular file fails fast on every platform,
        // mirroring "database unavailable" without TCP timeouts or
        // platform-specific permission tricks. The retry loop must surface
        // the final error after exhausting attempts instead of hanging or
        // panicking.
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"regular file, not a directory").unwrap();
        let db_path = blocker.join("broker.sqlite3");

        let target = DatabaseTarget::Sqlite(db_path.to_str().unwrap().to_string());
        let retry = ConnectRetryConfig {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(10),
        };
        let result = DatabaseBackend::connect_with_retry(&target, 2, retry).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn connect_with_retry_succeeds_when_database_is_available() {
        let dir = tempfile::tempdir().unwrap();
        let mut db_path = dir.path().to_path_buf();
        db_path.push("broker.sqlite3");
        let target = DatabaseTarget::Sqlite(db_path.to_str().unwrap().to_string());
        let retry = ConnectRetryConfig {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(10),
        };
        assert!(DatabaseBackend::connect_with_retry(&target, 2, retry)
            .await
            .is_ok());
    }

    #[test]
    fn connect_backoff_grows_exponentially() {
        let retry = ConnectRetryConfig {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(3000),
        };
        // The first retry waits one initial backoff, then it doubles.
        assert_eq!(
            retry.backoff_before_attempt(1),
            Some(Duration::from_millis(3000))
        );
        assert_eq!(
            retry.backoff_before_attempt(2),
            Some(Duration::from_millis(6000))
        );
        assert_eq!(
            retry.backoff_before_attempt(3),
            Some(Duration::from_millis(12_000))
        );
        assert_eq!(
            retry.backoff_before_attempt(4),
            Some(Duration::from_millis(24_000))
        );
    }

    #[test]
    fn connect_backoff_overflow_returns_none_instead_of_panicking() {
        let retry = ConnectRetryConfig {
            max_attempts: u32::MAX,
            initial_backoff: Duration::MAX / 4,
        };
        // Doubling stays representable for the first few attempts...
        assert_eq!(retry.backoff_before_attempt(1), Some(Duration::MAX / 4));
        // ...until the multiplication exceeds Duration::MAX, which must
        // return None rather than panic.
        assert!(retry.backoff_before_attempt(2).is_some());
        assert!(retry.backoff_before_attempt(3).is_some());
        assert_eq!(retry.backoff_before_attempt(33), None);
    }
}
