mod api;
mod backup;
mod bootstrap;
mod utils;

use crate::api::{start_api_service, BrokerSearchQuery};
use crate::backup::{backup_database, perform_periodic_backup};
use crate::bootstrap::download_file;
use crate::utils::get_missing_collectors;
use bgpkit_broker::notifier::NatsNotifier;
use bgpkit_broker::{
    crawl_collector, load_collectors, BgpkitBroker, BrokerError, Collector, LocalBrokerDb,
    DEFAULT_PAGE_SIZE,
};
use chrono::{Duration, NaiveDateTime, Utc};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::Path;
use std::process::exit;
use tabled::settings::Style;
use tabled::Table;
use tokio::runtime::Runtime;
use tracing::{debug, error, info};

pub(crate) fn is_local_path(path: &str) -> bool {
    if path.contains("://") {
        return false;
    }
    let path = Path::new(path);
    path.is_absolute() || path.is_relative()
}

pub(crate) fn parse_s3_path(path: &str) -> Option<(String, String)> {
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
        #[clap(long, default_value = "0.0.0.0")]
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

        /// bootstrap the database and update if a source database does not exist
        #[clap(long)]
        bootstrap: bool,

        /// bootstrap location (remote or local)
        #[clap(
            long,
            default_value = BOOTSTRAP_URL
        )]
        bootstrap_url: String,

        /// force writing a backup file to an existing file if specified
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

    /// List public BGP collector peers
    Peers {
        /// filter by collector ID
        #[clap(short, long)]
        collector: Option<String>,

        /// filter by peer AS number
        #[clap(short = 'a', long)]
        peer_asn: Option<u32>,

        /// filter by peer IP address
        #[clap(short = 'i', long)]
        peer_ip: Option<IpAddr>,

        /// show only full-feed peers
        #[clap(short, long)]
        full_feed_only: bool,

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

async fn try_send_heartbeat(url: Option<String>) -> Result<(), BrokerError> {
    let url = match url {
        Some(u) => u,
        None => match dotenvy::var("BGPKIT_BROKER_HEARTBEAT_URL") {
            Ok(u) => u,
            Err(_) => {
                info!("no heartbeat url specified, skipping");
                return Ok(());
            }
        },
    };
    info!("sending heartbeat to {}", &url);
    reqwest::get(&url).await?.error_for_status()?;
    Ok(())
}

async fn try_send_backup_heartbeat() -> Result<(), BrokerError> {
    match dotenvy::var("BGPKIT_BROKER_BACKUP_HEARTBEAT_URL") {
        Ok(url) => {
            info!("sending backup heartbeat to {}", &url);
            reqwest::get(&url).await?.error_for_status()?;
            Ok(())
        }
        Err(_) => {
            info!("no backup heartbeat url specified, skipping");
            Ok(())
        }
    }
}

/// update the database with data crawled from the given collectors
async fn update_database(
    db: &mut LocalBrokerDb,
    collectors: Vec<Collector>,
    days: Option<u32>,
    notifier: &Option<NatsNotifier>,
    send_heartbeat: bool,
) {
    let now = Utc::now();

    let latest_ts_map: HashMap<String, NaiveDateTime> = db
        .get_latest_files()
        .await
        .into_iter()
        .map(|f| (f.collector_id.clone(), f.ts_start))
        .collect();

    let mut collector_updated = false;
    for c in &collectors {
        if !latest_ts_map.contains_key(&c.id) {
            info!(
                "collector {} not found in database, inserting collector meta information first...",
                &c.id
            );
            db.insert_collector(c).await.unwrap();
            collector_updated = true;
        }
    }
    if collector_updated {
        info!("collector list updated, reload collectors list into memory");
        db.reload_collectors().await;
    }

    // crawl all collectors in parallel, 5 collectors in parallel by default, unordered.
    // for bootstrapping (no data in db), we only crawl one collector at a time
    const BUFFER_SIZE: usize = 5;

    debug!("unordered buffer size is {}", BUFFER_SIZE);

    let mut stream = futures::stream::iter(&collectors)
        .map(|c| {
            let latest_date;
            if let Some(d) = days {
                latest_date = Some(Utc::now().date_naive() - Duration::days(d as i64));
            } else {
                latest_date = latest_ts_map.get(&c.id).cloned().map(|ts| ts.date());
            }
            crawl_collector(c, latest_date)
        })
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
                    if let Some(n) = notifier {
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

    if send_heartbeat {
        if let Err(e) = try_send_heartbeat(None).await {
            error!("{}", e);
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

fn display_configuration_summary(
    do_update: bool,
    do_api: bool,
    update_interval: u64,
    host: &str,
    port: u16,
) {
    info!("=== BGPKIT Broker Configuration ===");

    // Update service status
    if do_update {
        info!(
            "Periodic updates: ENABLED (interval: {} seconds)",
            update_interval
        );
    } else {
        info!("Periodic updates: DISABLED");
    }

    // API service status
    if do_api {
        info!("API service: ENABLED ({}:{})", host, port);
    } else {
        info!("API service: DISABLED");
    }

    // Backup configuration
    match std::env::var("BGPKIT_BROKER_BACKUP_TO") {
        Ok(backup_to) => {
            if oneio::s3_url_parse(&backup_to).is_ok() {
                // S3 backup
                if oneio::s3_env_check().is_err() {
                    error!("Backup: CONFIGURED to S3 ({}) - WARNING: S3 environment variables not properly set", backup_to);
                } else {
                    info!("Backup: CONFIGURED to S3 ({})", backup_to);
                }
            } else {
                // Local backup
                info!("Backup: CONFIGURED to local path ({})", backup_to);
            }
        }
        Err(_) => {
            info!("Backup: DISABLED");
        }
    }

    // Heartbeat configuration
    let general_heartbeat = std::env::var("BGPKIT_BROKER_HEARTBEAT_URL").is_ok();
    let backup_heartbeat = std::env::var("BGPKIT_BROKER_BACKUP_HEARTBEAT_URL").is_ok();

    match (general_heartbeat, backup_heartbeat) {
        (true, true) => info!("Heartbeats: CONFIGURED (both general and backup)"),
        (true, false) => info!("Heartbeats: CONFIGURED (general only)"),
        (false, true) => info!("Heartbeats: CONFIGURED (backup only)"),
        (false, false) => info!("Heartbeats: DISABLED"),
    }

    // NATS configuration
    if std::env::var("BGPKIT_BROKER_NATS_URL").is_ok() {
        info!("NATS notifications: CONFIGURED");
    } else {
        info!("NATS notifications: DISABLED");
    }

    info!("=====================================");
}

fn main() {
    dotenvy::dotenv().ok();

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
            let do_update = !no_update;
            let do_api = !no_api;
            if do_log {
                enable_logging();
            }

            // Display configuration summary
            if do_log {
                display_configuration_summary(do_update, do_api, update_interval, &host, port);
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

                        // The update thread will handle the first update
                    });
                } else {
                    error!(
                    "The specified database file does not exist. Consider run bootstrap command or serve command with `--bootstrap` flag."
                );
                    exit(1);
                }
            }

            // set global panic hook so that child threads (updater or api) will crash the process should it encounter a panic
            std::panic::set_hook(Box::new(|panic_info| {
                eprintln!("Global panic hook: {}", panic_info);
                if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
                    eprintln!("Panic payload: {}", s);
                } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
                    eprintln!("Panic payload: {}", s);
                }
                exit(1)
            }));

            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

            if do_update {
                // starting a new dedicated thread to periodically fetch new data from collectors
                let path = db_path.clone();
                let backup_to = std::env::var("BGPKIT_BROKER_BACKUP_TO").ok();
                let backup_to_clone = backup_to.clone();
                std::thread::spawn(move || {
                    let rt = get_tokio_runtime();

                    let collectors = load_collectors().unwrap();
                    rt.block_on(async {
                        let notifier = match NatsNotifier::new(None).await {
                            Ok(n) => Some(n),
                            Err(_e) => {
                                info!("no nats notifier configured, skip pushing notification");
                                None
                            }
                        };

                        let mut db = LocalBrokerDb::new(path.as_str()).await.unwrap();
                        let mut update_interval_timer =
                            tokio::time::interval(std::time::Duration::from_secs(update_interval));

                        // track last backup time for daily backups
                        let mut last_backup_time = std::time::Instant::now();

                        // the first tick happens without waiting
                        update_interval_timer.tick().await;

                        // first execution
                        update_database(&mut db, collectors.clone(), None, &notifier, true).await;
                        db.analyze().await.unwrap();
                        // perform initial backup if configured
                        if let Some(ref backup_destination) = backup_to_clone {
                            info!("performing initial backup after first update...");
                            match perform_periodic_backup(&path, backup_destination, None).await {
                                Ok(_) => {
                                    info!("initial backup completed successfully");
                                    last_backup_time = std::time::Instant::now();

                                    // send backup heartbeat if configured
                                    if let Err(e) = try_send_backup_heartbeat().await {
                                        error!("failed to send backup heartbeat: {}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("initial backup failed: {}", e);
                                }
                            }
                        }

                        ready_tx.send(()).unwrap();
                        loop {
                            update_interval_timer.tick().await;

                            // updating from the latest data available
                            update_database(&mut db, collectors.clone(), None, &notifier, true)
                                .await;

                            // check if backup is needed (daily)
                            if let Some(ref backup_destination) = backup_to_clone {
                                let now = std::time::Instant::now();
                                let backup_interval = std::time::Duration::from_secs(24 * 60 * 60); // 24 hours

                                if now.duration_since(last_backup_time) >= backup_interval {
                                    info!("starting daily backup procedure...");
                                    match perform_periodic_backup(&path, backup_destination, None)
                                        .await
                                    {
                                        Ok(_) => {
                                            info!("daily backup completed successfully");
                                            last_backup_time = now;

                                            // send backup heartbeat if configured
                                            if let Err(e) = try_send_backup_heartbeat().await {
                                                error!("failed to send backup heartbeat: {}", e);
                                            }
                                        }
                                        Err(e) => {
                                            error!("daily backup failed: {}", e);
                                        }
                                    }
                                }
                            }

                            info!("wait for {} seconds before next update", update_interval);
                        }
                    });
                });

                if backup_to.is_some() {
                    info!(
                        "periodic backup enabled, backing up to: {}",
                        backup_to.as_ref().unwrap()
                    );
                } else {
                    info!("BGPKIT_BROKER_BACKUP_TO not set, periodic backup disabled");
                }
            }

            if do_api {
                let rt = get_tokio_runtime();
                rt.block_on(async {
                    if do_update {
                        // if update is enabled,
                        // we wait for the first update to complete before proceeding
                        ready_rx.await.unwrap();
                    }
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
            bootstrap,
            bootstrap_url,
            force,
            sqlite_cmd_path,
        } => {
            if do_log {
                enable_logging();
            }

            if oneio::s3_url_parse(&to).is_ok() && oneio::s3_env_check().is_err() {
                // backup to a s3 location and s3 environment variable check fails
                error!("Missing one or multiple required S3 environment variables: AWS_REGION AWS_ENDPOINT AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY");
                exit(1);
            }

            // check if the source database file exists
            if std::fs::metadata(&from).is_err() {
                if !bootstrap {
                    error!("The specified database path does not exist.");
                    exit(1);
                }

                // download the database file
                let collectors = load_collectors().unwrap();
                get_tokio_runtime().block_on(async {
                    info!(
                        "downloading bootstrap database file {} to {}",
                        &bootstrap_url, &from
                    );
                    download_file(&bootstrap_url, &from, true).await.unwrap();
                    let mut db = LocalBrokerDb::new(&from).await.unwrap();
                    update_database(&mut db, collectors, None, &None, false).await;
                    db.analyze().await.unwrap();
                });
            }

            if is_local_path(&to) {
                // back up to the local directory
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

            get_tokio_runtime().block_on(async {
                if let Err(e) = try_send_backup_heartbeat().await {
                    error!("failed to send backup heartbeat: {}", e);
                }
            });
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
                let mut db = LocalBrokerDb::new(&db_path).await.unwrap();
                let notifier = match NatsNotifier::new(None).await {
                    Ok(n) => Some(n),
                    Err(_e) => {
                        info!("no nats notifier configured, skip pushing notification");
                        None
                    }
                };
                update_database(&mut db, collectors, days, &notifier, false).await;
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

        Commands::Peers {
            collector,
            peer_asn,
            peer_ip,
            full_feed_only,
            json,
        } => {
            let mut broker = BgpkitBroker::new();
            // health check first
            if broker.health_check().is_err() {
                println!("broker instance at {} is not available", broker.broker_url);
                return;
            }
            if let Some(collector_id) = collector {
                broker = broker.collector_id(collector_id);
            }
            if let Some(asn) = peer_asn {
                broker = broker.peers_asn(asn);
            }
            if let Some(ip) = peer_ip {
                broker = broker.peers_ip(ip);
            }
            if full_feed_only {
                broker = broker.peers_only_full_feed(true);
            }
            let items = broker.get_peers().unwrap();

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

            println!("checking for missing collectors...");
            let latest_items = broker.latest().unwrap();

            let missing_collectors = get_missing_collectors(&latest_items);

            if missing_collectors.is_empty() {
                println!("all collectors are up to date");
            } else {
                println!("missing the following collectors:");
                println!("{}", Table::new(missing_collectors).with(Style::markdown()));
            }
        }
    }
}
