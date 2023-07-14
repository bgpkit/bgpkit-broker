mod api;
mod config;
mod search;

use crate::config::BrokerConfig;

use crate::api::start_api_service;
use bgpkit_broker::{crawl_collector, load_collectors, LocalBrokerDb};
use chrono::Utc;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use tokio::runtime::Runtime;
use tracing::{debug, info};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    /// configuration file path, by default $HOME/.bgpkit/broker.toml is used
    #[clap(short, long)]
    config: Option<String>,

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

        /// API socket address
        #[clap(short = 's', long, default_value = "127.0.0.1:3000")]
        api_socket: String,

        /// disable updater service
        #[clap(long, group = "disable")]
        disable_updater: bool,

        /// disable API service
        #[clap(long, group = "disable")]
        disable_api: bool,
    },

    /// Update the Broker database, useful in cronjob
    Update {},

    /// Bootstrap the Broker database, useful in cronjob
    Bootstrap { path: Option<String> },

    /// Export broker database to parquet file
    Export { path: String },
    // TODO: search should use the broker SDK (i.e. http queries) instead of query directly to DB
    // /// Search MRT files in Broker db
    // Search {
    //     #[clap(flatten)]
    //     query: BrokerSearchQuery,
    // },
}

fn get_tokio_runtime() -> Runtime {
    // configure async runtime
    // let blocking_cpus = match num_cpus::get() {
    //     1 => 1,
    //     n => n,
    // };
    let blocking_cpus = num_cpus::get();

    debug!("using {} cores for parsing html pages", blocking_cpus);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(blocking_cpus)
        .build()
        .unwrap();
    rt
}

fn main() {
    let cli = Cli::parse();

    let config = BrokerConfig::new(&cli.config);

    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "bgpkit_broker=info,poem=debug");
    }
    tracing_subscriber::fmt::init();

    match cli.command {
        Commands::Serve {
            update_interval,
            api_socket,
            disable_updater,
            disable_api,
        } => {
            let database = LocalBrokerDb::new("broker.duckdb", false).unwrap();
            let db = database.clone();

            if !disable_updater {
                std::thread::spawn(move || {
                    let rt = get_tokio_runtime();
                    rt.block_on(async {
                        let mut interval =
                            tokio::time::interval(std::time::Duration::from_secs(update_interval));

                        loop {
                            interval.tick().await;

                            let latest_date =
                                match { db.get_latest_timestamp().unwrap().map(|t| t.date()) } {
                                    Some(t) => Some(t),
                                    None => {
                                        // if bootstrap is false and we have an empty database
                                        // we crawl data from 30 days ago
                                        Some(Utc::now().date_naive() - chrono::Duration::days(30))
                                    }
                                };

                            // load all collectors from configuration file
                            let collectors =
                                load_collectors(config.collectors_file.as_str()).unwrap();

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
                            while let Some(res) = stream.next().await {
                                let db = db.clone();
                                match res {
                                    Ok(items) => {
                                        let _inserted = db.insert_items(&items).unwrap();
                                    }
                                    Err(e) => {
                                        dbg!(e);
                                        break;
                                    }
                                }
                            }
                            info!("finished updating broker database");

                            info!("wait for {} seconds before next update", update_interval);
                        }
                    });
                });
            }

            if !disable_api {
                let rt = get_tokio_runtime();
                rt.block_on(async {
                    start_api_service(database.clone(), api_socket.as_str())
                        .await
                        .unwrap();
                });
            }
        }
        Commands::Export { path } => {
            let db = LocalBrokerDb::new(config.local_db_file.as_str(), false).unwrap();
            db.export_parquet(path.as_str()).unwrap()
        }
        Commands::Bootstrap { path } => {
            let db = LocalBrokerDb::new(config.local_db_file.as_str(), false).unwrap();

            let path_str = match path {
                Some(p) => p,
                None => config.local_db_bootstrap_path,
            };
            db.bootstrap(path_str.as_str()).unwrap()
        }
        Commands::Update {} => {
            // create a tokio runtime
            let rt = get_tokio_runtime();

            let db = LocalBrokerDb::new(config.local_db_file.as_str(), false).unwrap();
            // get the latest data's date from the database
            let latest_date = match { db.get_latest_timestamp().unwrap().map(|t| t.date()) } {
                Some(t) => Some(t),
                None => {
                    // if bootstrap is false and we have an empty database
                    // we crawl data from 30 days ago
                    Some(Utc::now().date_naive() - chrono::Duration::days(30))
                }
            };

            rt.block_on(async {
                // load all collectors from configuration file
                let collectors = load_collectors(config.collectors_file.as_str()).unwrap();

                // crawl all collectors in parallel, 10 collectors in parallel by default, unordered.
                // for bootstrapping (no data in db), we only crawl one collector at a time
                let buffer_size = match latest_date {
                    Some(_) => 10,
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
                while let Some(res) = stream.next().await {
                    let db = db.clone();
                    match res {
                        Ok(items) => {
                            let _inserted = db.insert_items(&items).unwrap();
                        }
                        Err(e) => {
                            dbg!(e);
                            break;
                        }
                    }
                }
                info!("finished updating broker database")
            });
        } // Commands::Search { query } => {
          //     let db = LocalBrokerDb::new(config.local_db_file.as_str(), false).unwrap();
          //     let items = process_search_query(query, &db).unwrap();
          //
          //     let val = json!(items);
          //     println!("{}", serde_json::to_string_pretty(&val).unwrap());
          // }
    }
}
