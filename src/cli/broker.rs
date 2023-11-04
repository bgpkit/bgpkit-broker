mod api;

use crate::api::{start_api_service, BrokerSearchQuery};
use bgpkit_broker::{
    crawl_collector, load_collectors, BgpkitBroker, Collector, LocalBrokerDb, DEFAULT_PAGE_SIZE,
};
use chrono::Utc;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use std::process::exit;
use tabled::settings::Style;
use tabled::Table;
use tokio::runtime::Runtime;
use tracing::{debug, info};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    /// disable logging
    #[clap(long, global = true)]
    no_log: bool,

    #[clap(subcommand)]
    command: Commands,

    /// broker db file location
    #[clap(short, long, global = true, default_value = "bgpkit_broker.sqlite3")]
    db: String,
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
        no_update: bool,

        /// disable API service
        #[clap(long, group = "disable")]
        no_api: bool,
    },

    /// Update the Broker database
    Update {
        /// force number of days to look back.
        /// by default resume from the latest available data time.
        #[clap(short, long)]
        days: Option<u32>,
    },

    /// TODO: Bootstrap the Broker database
    Bootstrap {},

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

fn min_update_interval_check(s: &str) -> Result<u64, String> {
    let v = s.parse::<u64>().map_err(|e| e.to_string())?;
    if v < 300 {
        Err("update interval should be at least 300 seconds (5 minutes)".to_string())
    } else {
        Ok(v)
    }
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
async fn update_database(db: LocalBrokerDb, collectors: Vec<Collector>, days: Option<u32>) {
    let latest_date;
    if let Some(d) = days {
        // if days is specified, we crawl data from d days ago
        latest_date = Some(Utc::now().date_naive() - chrono::Duration::days(d as i64));
    } else {
        // otherwise, we crawl data from the latest timestamp in the database
        latest_date = match { db.get_latest_timestamp().await.unwrap().map(|t| t.date()) } {
            Some(t) => {
                info!("update broker db from the latest date in db: {}", t);
                Some(t)
            }
            None => {
                // if bootstrap is false and we have an empty database we crawl data from 30 days ago
                let date = Utc::now().date_naive() - chrono::Duration::days(30);
                info!(
                    "empty database, bootstrapping data from {} days ago ({})",
                    30, date
                );
                Some(date)
            }
        };
    }

    // crawl all collectors in parallel, 5 collectors in parallel by default, unordered.
    // for bootstrapping (no data in db), we only crawl one collector at a time
    const BUFFER_SIZE: usize = 5;

    debug!("unordered buffer size is {}", BUFFER_SIZE);

    let mut stream = futures::stream::iter(&collectors)
        .map(|c| crawl_collector(c, latest_date))
        .buffer_unordered(BUFFER_SIZE);

    info!(
        "start updating broker database for {} collectors",
        &collectors.len()
    );
    while let Some(res) = stream.next().await {
        let db = db.clone();
        match res {
            Ok(items) => {
                let _inserted = db.insert_items(&items, true).await.unwrap();
            }
            Err(e) => {
                dbg!(e);
                break;
            }
        }
    }

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

    let db_file_path: String = cli.db.clone();
    if std::fs::metadata(&db_file_path).is_err() {
        eprintln!("The specified database file does not exist.");
        exit(1);
    }

    match cli.command {
        Commands::Serve {
            update_interval,
            host,
            port,
            root,
            no_update,
            no_api,
        } => {
            if do_log {
                enable_logging();
            }

            if !no_update {
                // starting a new dedicated thread to periodically fetch new data from collectors
                let path = db_file_path.clone();
                std::thread::spawn(move || {
                    let rt = get_tokio_runtime();

                    let collectors = load_collectors().unwrap();

                    rt.block_on(async {
                        let db = LocalBrokerDb::new(path.as_str()).await.unwrap();
                        let mut interval =
                            tokio::time::interval(std::time::Duration::from_secs(update_interval));

                        loop {
                            interval.tick().await;
                            update_database(db.clone(), collectors.clone(), Some(60)).await;
                            info!("wait for {} seconds before next update", update_interval);
                        }
                    });
                });
            }

            if !no_api {
                let rt = get_tokio_runtime();
                rt.block_on(async {
                    let database = LocalBrokerDb::new(db_file_path.as_str()).await.unwrap();
                    start_api_service(database.clone(), host, port, root)
                        .await
                        .unwrap();
                });
            }
        }
        Commands::Bootstrap {} => {
            todo!()
            // handle bootstrap
            // if do_log {
            //     enable_logging();
            // }
            // let bootstrap_path = match cli.bootstrap_parquet {
            //     true => config.db_bootstrap_parquet_path.clone(),
            //     false => config.db_bootstrap_duckdb_path.clone(),
            // };
            //
            // let _ = LocalBrokerDb::new(config.db_file_path.as_str(), false, Some(bootstrap_path))
            //     .unwrap();
        }
        Commands::Update { days } => {
            if do_log {
                enable_logging();
            }
            // create a tokio runtime
            let rt = get_tokio_runtime();

            // load all collectors from configuration file
            let collectors = load_collectors().unwrap();

            rt.block_on(async {
                let db = LocalBrokerDb::new(db_file_path.as_str()).await.unwrap();
                update_database(db, collectors, days).await;
            });
        }
        Commands::Search { query, json, url } => {
            // TODO: add support for search against local database
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
