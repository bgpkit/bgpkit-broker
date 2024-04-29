mod api;
mod backup;
mod bootstrap;

use crate::api::{start_api_service, BrokerSearchQuery};
use crate::backup::backup_database;
use crate::bootstrap::download_file;
use bgpkit_broker::notifier::NatsNotifier;
use bgpkit_broker::{
    crawl_collector, load_collectors, BgpkitBroker, Collector, LocalBrokerDb, DEFAULT_PAGE_SIZE,
};
use bgpkit_commons::collectors::MrtCollector;
use chrono::{Duration, NaiveDateTime, Utc};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::process::exit;
use tabled::settings::Style;
use tabled::{Table, Tabled};
use tokio::runtime::Runtime;
use tracing::{debug, error, info};

fn is_local_path(path: &str) -> bool {
    if path.contains("://") {
        return false;
    }
    let path = Path::new(path);
    path.is_absolute() || path.is_relative()
}

fn parse_s3_path(path: &str) -> Option<(String, String)> {
    // split a path like s3://bucket/path/to/file into (bucket, path/to/file)
    let parts = path.split("://").collect::<Vec<&str>>();
    if parts.len() != 2 || parts[0] != "s3" {
        return None;
    }
    let parts = parts[1].split('/').collect::<Vec<&str>>();
    let bucket = parts[0].to_string();
    // join the rest delimited by `/`
    let path = format!("/{}", parts[1..].join("/"));
    if parts.ends_with(&["/"]) {
        return None;
    }
    Some((bucket, path))
}

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    /// disable logging
    #[clap(long, global = true)]
    no_log: bool,

    #[clap(long, global = true)]
    env: Option<String>,

    #[clap(subcommand)]
    command: Commands,
}

const BOOTSTRAP_URL: &str = "https://spaces.bgpkit.org/broker/bgpkit_broker.sqlite3";

#[derive(Subcommand)]
enum Commands {
    /// Serve the Broker content via RESTful API
    Serve {
        /// broker db file location
        db_path: String,

        /// update interval in seconds
        #[clap(short = 'i', long, default_value = "300", value_parser = min_update_interval_check)]
        update_interval: u64,

        /// bootstrap the database if it does not exist
        #[clap(short, long)]
        bootstrap: bool,

        /// disable bootstrap progress bar
        #[clap(short, long)]
        silent: bool,

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
        /// broker db file location
        #[clap()]
        db_path: String,

        /// force number of days to look back.
        /// by default resume from the latest available data time.
        #[clap(short, long)]
        days: Option<u32>,
    },

    /// Bootstrap the broker database
    Bootstrap {
        /// Bootstrap from location (remote or local)
        #[clap(
            short,
            long,
            default_value = BOOTSTRAP_URL
        )]
        from: String,

        /// broker db file location
        #[clap()]
        db_path: String,

        /// disable bootstrap progress bar
        #[clap(short, long)]
        silent: bool,
    },

    /// Backup Broker database
    Backup {
        /// source database location
        from: String,

        /// remote database location
        to: String,

        /// force writing backup file to existing file if specified
        #[clap(short, long)]
        force: bool,

        /// specify sqlite3 command path
        #[clap(short, long)]
        sqlite_cmd_path: Option<String>,
    },

    /// Search MRT files in Broker db
    Search {
        #[clap(flatten)]
        query: BrokerSearchQuery,

        /// Specify broker endpoint
        #[clap(short, long)]
        url: Option<String>,

        /// Print out search results in JSON format instead of Markdown table
        #[clap(short, long)]
        json: bool,
    },

    /// Display latest MRT files indexed
    Latest {
        /// filter by collector ID
        #[clap(short, long)]
        collector: Option<String>,

        /// Specify broker endpoint
        #[clap(short, long)]
        url: Option<String>,

        /// Showing only latest items that are outdated
        #[clap(short, long)]
        outdated: bool,

        /// Print out search results in JSON format instead of Markdown table
        #[clap(short, long)]
        json: bool,
    },

    /// Streaming live from a broker NATS server
    Live {
        /// URL to NATS server, e.g. nats://localhost:4222.
        /// If not specified, will try to read from BGPKIT_BROKER_NATS_URL env variable.
        #[clap(short, long)]
        url: Option<String>,

        /// Subject to subscribe to, default to public.broker.>
        #[clap(short, long)]
        subject: Option<String>,

        /// Pretty print JSON output
        #[clap(short, long)]
        pretty: bool,
    },

    /// Check broker instance health and missing collectors
    Doctor {},
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
async fn update_database(
    db: LocalBrokerDb,
    collectors: Vec<Collector>,
    days: Option<u32>,
    notify: bool,
) {
    let notifier = match notify {
        true => NatsNotifier::new(None).await.ok(),
        false => None,
    };

    let now = Utc::now();
    let latest_date;
    if let Some(d) = days {
        // if days is specified, we crawl data from d days ago
        latest_date = Some(Utc::now().date_naive() - Duration::days(d as i64));
    } else {
        // otherwise, we crawl data from the latest timestamp in the database
        latest_date = match db.get_latest_timestamp().await.unwrap().map(|t| t.date()) {
            Some(t) => {
                let start_date = t - Duration::days(1);
                info!(
                    "update broker db from the latest date - 1 in db: {}",
                    start_date
                );
                Some(start_date)
            }
            None => {
                // if bootstrap is false and we have an empty database we crawl data from 30 days ago
                let date = Utc::now().date_naive() - Duration::days(30);
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
    let mut total_inserted_count = 0;
    while let Some(res) = stream.next().await {
        let db = db.clone();
        match res {
            Ok(items) => {
                let inserted = db.insert_items(&items, true).await.unwrap();
                if !inserted.is_empty() {
                    if let Some(n) = &notifier {
                        if let Err(e) = n.send(&inserted).await {
                            error!("{}", e);
                        }
                    }
                }
                total_inserted_count += inserted.len();
            }
            Err(e) => {
                error!("{}", e);
                continue;
            }
        }
    }

    let duration = Utc::now() - now;
    db.insert_meta(duration.num_seconds() as i32, total_inserted_count as i32)
        .await
        .unwrap();

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

    if let Some(env_path) = cli.env {
        match dotenvy::from_path_override(env_path.as_str()) {
            Ok(_) => {
                info!("loaded environment variables from {}", env_path);
            }
            Err(_) => {
                error!("failed to load environment variables from {}", env_path);
                exit(1);
            }
        };
    }

    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "bgpkit_broker=info,poem=debug");
    }

    match cli.command {
        Commands::Serve {
            db_path,
            update_interval,
            bootstrap,
            silent,
            host,
            port,
            root,
            no_update,
            no_api,
        } => {
            if do_log {
                enable_logging();
            }

            if std::fs::metadata(&db_path).is_err() {
                if bootstrap {
                    // bootstrap the database
                    let rt = get_tokio_runtime();
                    let from = BOOTSTRAP_URL.to_string();
                    rt.block_on(async {
                        info!(
                            "downloading bootstrap database file {} to {}",
                            &from, &db_path
                        );
                        download_file(&from, &db_path, silent).await.unwrap();
                    });
                } else {
                    error!(
                    "The specified database file does not exist. Consider run bootstrap command or serve command with `--bootstrap` flag."
                );
                    exit(1);
                }
            }

            if !no_update {
                // starting a new dedicated thread to periodically fetch new data from collectors
                let path = db_path.clone();
                std::thread::spawn(move || {
                    let rt = get_tokio_runtime();

                    let collectors = load_collectors().unwrap();

                    rt.block_on(async {
                        let db = LocalBrokerDb::new(path.as_str()).await.unwrap();
                        let mut interval =
                            tokio::time::interval(std::time::Duration::from_secs(update_interval));

                        loop {
                            interval.tick().await;
                            // updating from the latest data available
                            update_database(db.clone(), collectors.clone(), None, true).await;
                            info!("wait for {} seconds before next update", update_interval);
                        }
                    });
                });
            }

            if !no_api {
                let rt = get_tokio_runtime();
                rt.block_on(async {
                    let database = LocalBrokerDb::new(db_path.as_str()).await.unwrap();
                    start_api_service(database.clone(), host, port, root)
                        .await
                        .unwrap();
                });
            }
        }
        Commands::Bootstrap {
            from,
            db_path,
            silent,
        } => {
            if do_log {
                enable_logging();
            }

            // check if file exists
            if std::fs::metadata(&db_path).is_ok() {
                error!("The specified database path already exists, skip bootstrapping.");
                exit(1);
            }

            // download the database file
            let rt = get_tokio_runtime();
            rt.block_on(async {
                info!(
                    "downloading bootstrap database file {} to {}",
                    &from, &db_path
                );
                download_file(&from, &db_path, silent).await.unwrap();
            });
        }
        Commands::Backup {
            from,
            to,
            force,
            sqlite_cmd_path,
        } => {
            if do_log {
                enable_logging();
            }

            // check if file exists
            if std::fs::metadata(&from).is_err() {
                error!("The specified database path does not exist.");
                exit(1);
            }

            if is_local_path(&to) {
                // back up to local directory
                backup_database(&from, &to, force, sqlite_cmd_path).unwrap();
                return;
            }

            if let Some((bucket, s3_path)) = parse_s3_path(&to) {
                // back up to S3
                let temp_dir = tempfile::tempdir().unwrap();
                let temp_file_path = temp_dir
                    .path()
                    .join("temp.db")
                    .to_str()
                    .unwrap()
                    .to_string();

                match backup_database(&from, &temp_file_path, force, sqlite_cmd_path) {
                    Ok(_) => {
                        info!(
                            "uploading backup file {} to S3 at s3://{}/{}",
                            &temp_file_path, &bucket, &s3_path
                        );
                        match oneio::s3_upload(&bucket, &s3_path, &temp_file_path) {
                            Ok(_) => {
                                info!("backup file uploaded to S3");
                            }
                            Err(e) => {
                                error!("failed to upload backup file to S3: {}", e);
                                exit(1);
                            }
                        }
                    }
                    Err(_) => {
                        error!("failed to backup database");
                        exit(1);
                    }
                }
            }
        }
        Commands::Update { db_path, days } => {
            if std::fs::metadata(&db_path).is_err() {
                error!("The specified database file does not exist.");
                exit(1);
            }

            if do_log {
                enable_logging();
            }
            // create a tokio runtime
            let rt = get_tokio_runtime();

            // load all collectors from configuration file
            let collectors = load_collectors().unwrap();

            rt.block_on(async {
                let db = LocalBrokerDb::new(&db_path).await.unwrap();
                update_database(db, collectors, days, true).await;
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
        Commands::Latest {
            collector,
            url,
            outdated,
            json,
        } => {
            let mut broker = BgpkitBroker::new();
            if let Some(url) = url {
                broker = broker.broker_url(url);
            }
            // health check first
            if broker.health_check().is_err() {
                println!("broker instance at {} is not available", broker.broker_url);
                return;
            }
            if let Some(collector_id) = collector {
                broker = broker.collector_id(collector_id);
            }

            let mut items = broker.latest().unwrap();
            if outdated {
                const DEPRECATED_COLLECTORS: [&str; 6] = [
                    "rrc02",
                    "rrc08",
                    "rrc09",
                    "route-views.jinx",
                    "route-views.siex",
                    "route-views.saopaulo",
                ];
                items.retain(|item| {
                    if DEPRECATED_COLLECTORS.contains(&item.collector_id.as_str()) {
                        return false;
                    }
                    let now = Utc::now().naive_utc();
                    (now - item.ts_start)
                        > match item.is_rib() {
                            true => Duration::hours(24),
                            false => Duration::hours(1),
                        }
                });
            }
            if json {
                println!("{}", serde_json::to_string_pretty(&items).unwrap());
            } else {
                println!("{}", Table::new(items).with(Style::markdown()));
            }
        }
        Commands::Live {
            url,
            subject,
            pretty,
        } => {
            dotenvy::dotenv().ok();
            if do_log {
                enable_logging();
            }
            let rt = get_tokio_runtime();
            rt.block_on(async {
                let mut notifier = match NatsNotifier::new(url).await {
                    Ok(n) => n,
                    Err(e) => {
                        error!("{}", e);
                        return;
                    }
                };
                if let Err(e) = notifier.start_subscription(subject).await {
                    error!("{}", e);
                    return;
                }
                while let Some(item) = notifier.next().await {
                    if pretty {
                        println!("{}", serde_json::to_string_pretty(&item).unwrap());
                    } else {
                        println!("{}", item);
                    }
                }
            });
        }

        Commands::Doctor {} => {
            if do_log {
                enable_logging();
            }
            println!("checking broker instance health...");
            let broker = BgpkitBroker::new();
            if broker.health_check().is_ok() {
                println!("\tbroker instance at {} is healthy", broker.broker_url);
            } else {
                println!(
                    "\tbroker instance at {} is not available",
                    broker.broker_url
                );
                return;
            }

            println!();

            #[derive(Tabled)]
            struct CollectorInfo {
                project: String,
                name: String,
                country: String,
                activated_on: NaiveDateTime,
                data_url: String,
            }

            println!("checking for missing collectors...");
            let latest_items = broker.latest().unwrap();
            let latest_collectors: HashSet<String> =
                latest_items.into_iter().map(|i| i.collector_id).collect();
            let all_collectors_map: HashMap<String, MrtCollector> =
                bgpkit_commons::collectors::get_all_collectors()
                    .unwrap()
                    .into_iter()
                    .map(|c| (c.name.clone(), c))
                    .collect();

            let all_collector_names: HashSet<String> = all_collectors_map
                .values()
                .map(|c| c.name.clone())
                .collect();

            // get the difference between the two sets
            let missing_collectors: Vec<CollectorInfo> = all_collector_names
                .difference(&latest_collectors)
                .map(|c| {
                    // convert to CollectorInfo
                    let collector = all_collectors_map.get(c).unwrap();
                    let country_map = bgpkit_commons::countries::Countries::new().unwrap();
                    CollectorInfo {
                        project: collector.project.to_string(),
                        name: collector.name.clone(),
                        country: country_map
                            .lookup_by_code(&collector.country)
                            .unwrap()
                            .name
                            .clone(),
                        activated_on: collector.activated_on,
                        data_url: collector.data_url.clone(),
                    }
                })
                .sorted_by(|a, b| a.activated_on.cmp(&b.activated_on))
                .collect();

            if missing_collectors.is_empty() {
                println!("all collectors are up to date");
            } else {
                println!("missing the following collectors:");
                println!("{}", Table::new(missing_collectors).with(Style::markdown()));
            }
        }
    }
}
