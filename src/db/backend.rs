use super::{DbSearchResult, LocalBrokerDb, PostgresDb, UpdatesMeta};
use crate::config::DatabaseTarget;
use crate::{BrokerError, BrokerItem, Collector};
use chrono::NaiveDateTime;

#[derive(Clone)]
pub enum DatabaseBackend {
    Sqlite(LocalBrokerDb),
    Postgres(PostgresDb),
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

    pub async fn get_latest_files(&self) -> Vec<BrokerItem> {
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
