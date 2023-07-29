mod api;
mod config;

use crate::config::BrokerConfig;

use crate::api::{start_api_service, BrokerSearchQuery};
use bgpkit_broker::{
    crawl_collector, load_collectors, BgpkitBroker, Collector, LocalBrokerDb, DEFAULT_PAGE_SIZE,
};
use chrono::Utc;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use tabled::settings::Style;
use tabled::Table;
use tokio::runtime::Runtime;
use tracing::{debug, info};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(long)]
    no_log: bool,

    #[clap(subcommand)]
    command: Commands,
}

fn min_update_interval_check(s: &str) -> Result<u64, String> {
    let v = s.parse::<u64>().map_err(|e| e.to_string())?;
    if v < 300 {
        Err("update interval should be at least 300 seconds (5 minutes)".to_string())
    } else {
        Ok(v)
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Serve the Broker content via RESTful API
    Serve {
        /// update interval in seconds
        #[clap(short = 'i', long, default_value = "300", value_parser = min_update_interval_check)]
        update_interval: u64,

        /// host address
        #[clap(short = 'h', long, default_value = "0.0.0.0")]
        host: String,

        /// port number
        #[clap(short = 'p', long, default_value = "40064")]
        port: u16,

        /// root path, useful for configuring docs UI
        #[clap(short = 'r', long, default_value = "/")]
        root: String,

        /// disable updater service
        #[clap(long, group = "disable")]
        no_updater: bool,

        /// disable API service
        #[clap(long, group = "disable")]
        no_api: bool,

        /// bootstrap if empty
        #[clap(long)]
        bootstrap: bool,
    },

    /// Update the Broker database
    Update {},

    /// Bootstrap the Broker database
    Bootstrap { path: Option<String> },

    /// Export broker database to parquet file
    Backup {
        /// whether to include the duckdb version in the backup file
        #[clap(short, long)]
        include_version: bool,
    },

    /// Search MRT files in Broker db
    Search {
        #[clap(flatten)]
        query: BrokerSearchQuery,

        #[clap(short, long)]
        url: Option<String>,

        /// print out search results in JSON format instead of Markdown table
        #[clap(short, long)]
        json: bool,
    },
}

fn get_tokio_runtime() -> Runtime {
    let blocking_cpus = num_cpus::get();

    debug!("using {} cores for parsing html pages", blocking_cpus);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(blocking_cpus)
        .build()
        .unwrap();
    rt
}

/// update the database with data crawled from the given collectors
async fn update_database(db: LocalBrokerDb, collectors: Vec<Collector>) {
    let now = Utc::now();
    let latest_date = match { db.get_latest_timestamp().unwrap().map(|t| t.date()) } {
        Some(t) => Some(t),
        None => {
            // if bootstrap is false and we have an empty database
            // we crawl data from 30 days ago
            Some(Utc::now().date_naive() - chrono::Duration::days(30))
        }
    };

    // crawl all collectors in parallel, 10 collectors in parallel by default, unordered.
    // for bootstrapping (no data in db), we only crawl one collector at a time
    let buffer_size = match latest_date {
        Some(_) => 5,
        None => 1,
    };

    debug!("unordered buffer size is {}", buffer_size);

    let mut stream = futures::stream::iter(&collectors)
        .map(|c| crawl_collector(c, latest_date))
        .buffer_unordered(buffer_size);

    info!(
        "start updating broker database for {} collectors",
        &collectors.len()
    );
    let mut total_inserted_count = 0;
    while let Some(res) = stream.next().await {
        let db = db.clone();
        match res {
            Ok(items) => {
                let inserted = db.insert_items(&items).unwrap();
                total_inserted_count += inserted.len();
            }
            Err(e) => {
                dbg!(e);
                break;
            }
        }
    }

    let duration = Utc::now() - now;
    // update meta timestamp
    db.insert_meta(duration.num_seconds() as i32, total_inserted_count as i32)
        .unwrap();

    info!("running checkpoint...");
    db.checkpoint().unwrap();

    info!("finished updating broker database");
}

fn enable_logging() {
    tracing_subscriber::fmt()
        .with_ansi(true)
        .with_level(true)
        .with_target(false)
        .init();
}

fn main() {
    let cli = Cli::parse();

    let do_log = !cli.no_log;

    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "bgpkit_broker=info,poem=debug");
    }

    let config = match envy::prefixed("BGPKIT_BROKER_").from_env::<BrokerConfig>() {
        Ok(config) => {
            println!("{:#?}", &config);
            config
        }
        Err(error) => panic!("{:#?}", error),
    };

    match cli.command {
        Commands::Serve {
            update_interval,
            host,
            port,
            root,
            no_updater,
            no_api,
            bootstrap,
        } => {
            if do_log {
                enable_logging();
            }

            let database = LocalBrokerDb::new(config.local_db_path.as_str(), false).unwrap();
            let db = database.clone();

            if db.get_entry_count().unwrap() < 100_000 && bootstrap {
                info!("database needs bootstrap, bootstrapping now...");
                db.bootstrap(config.db_bootstrap_path.as_str()).unwrap();
                info!("database needs bootstrap, bootstrapping now... done");
            }

            if !no_updater {
                std::thread::spawn(move || {
                    let rt = get_tokio_runtime();

                    // load all collectors from configuration file
                    let collectors = load_collectors(config.collectors_config.as_str()).unwrap();

                    rt.block_on(async {
                        let mut interval =
                            tokio::time::interval(std::time::Duration::from_secs(update_interval));

                        loop {
                            interval.tick().await;
                            update_database(db.clone(), collectors.clone()).await;
                            info!("wait for {} seconds before next update", update_interval);
                        }
                    });
                });
            }

            if !no_api {
                let rt = get_tokio_runtime();
                rt.block_on(async {
                    start_api_service(database.clone(), host, port, root)
                        .await
                        .unwrap();
                });
            }
        }
        Commands::Backup { include_version } => {
            if do_log {
                enable_logging();
            }

            info!("backing up database...");

            // exporting duckdb file and parquet file
            let db = LocalBrokerDb::new(config.local_db_path.as_str(), false).unwrap();
            let exported_db_path = db
                .backup_duckdb(config.backup_dir.as_str(), include_version)
                .unwrap();
            let exported_parquet_path = db.backup_parquet(config.backup_dir.as_str()).unwrap();

            if config.do_s3_backup() {
                let s3_bucket = config.s3_bucket.unwrap();
                let s3_dir = config.s3_dir.unwrap();
                let s3_parquet_path = format!(
                    "{}/{}",
                    s3_dir,
                    exported_parquet_path.split('/').last().unwrap()
                );
                let s3_duckdb_path =
                    format!("{}/{}", s3_dir, exported_db_path.split('/').last().unwrap());

                info!(
                    "uploading parquet file {} to S3 at {}",
                    exported_parquet_path, s3_parquet_path
                );
                oneio::s3_upload(
                    s3_bucket.as_str(),
                    s3_parquet_path.as_str(),
                    exported_parquet_path.as_str(),
                )
                .unwrap();

                info!(
                    "uploading duckdb file {} to S3 at {}",
                    exported_db_path, s3_duckdb_path
                );
                oneio::s3_upload(
                    s3_bucket.as_str(),
                    s3_duckdb_path.as_str(),
                    exported_db_path.as_str(),
                )
                .unwrap();
            }

            info!("finished exporting db");
        }
        Commands::Bootstrap { path } => {
            if do_log {
                enable_logging();
            }
            let db = LocalBrokerDb::new(config.local_db_path.as_str(), false).unwrap();

            let path_str = match path {
                Some(p) => p,
                None => config.db_bootstrap_path,
            };
            db.bootstrap(path_str.as_str()).unwrap()
        }
        Commands::Update {} => {
            if do_log {
                enable_logging();
            }
            // create a tokio runtime
            let rt = get_tokio_runtime();

            let db = LocalBrokerDb::new(config.local_db_path.as_str(), false).unwrap();
            // load all collectors from configuration file
            let collectors = load_collectors(config.collectors_config.as_str()).unwrap();

            rt.block_on(async {
                update_database(db, collectors).await;
            });
        }
        Commands::Search { query, json, url } => {
            let mut broker = BgpkitBroker::new();
            if let Some(url) = url {
                broker = broker.broker_url(url);
            }
            // health check first
            if broker.health_check().is_err() {
                println!("broker instance at {} is not available", broker.broker_url);
                return;
            }

            if let Some(ts_start) = query.ts_start {
                broker = broker.ts_start(ts_start);
            }
            if let Some(ts_end) = query.ts_end {
                broker = broker.ts_end(ts_end);
            }
            if let Some(project) = query.project {
                broker = broker.project(project);
            }
            if let Some(collector_id) = query.collector_id {
                broker = broker.collector_id(collector_id);
            }
            if let Some(data_type) = query.data_type {
                broker = broker.data_type(data_type);
            }
            let (page, page_size) = (
                query.page.unwrap_or(1),
                query.page_size.unwrap_or(DEFAULT_PAGE_SIZE),
            );
            broker = broker.page(page as i64);
            broker = broker.page_size(page_size as i64);
            let items = broker.query_single_page().unwrap();

            if json {
                println!("{}", serde_json::to_string_pretty(&items).unwrap());
            } else {
                println!("{}", Table::new(items).with(Style::markdown()));
            }
        }
    }
}
