use bgpkit_broker::cli::{process_search_query, BrokerConfig, BrokerSearchQuery};
use bgpkit_broker::{crawl_collector, load_collectors, LocalBrokerDb};
use chrono::Utc;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use serde_json::json;
use tokio::runtime::Runtime;
use tracing::info;

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

#[derive(Subcommand)]
enum Commands {
    /// Serve the Broker content via RESTful API
    Serve {},

    /// Update the Broker database, useful in cronjob
    Update {
        /// bootstrap the database
        #[clap(short, long)]
        bootstrap: bool,

        /// reset the database
        #[clap(short, long)]
        reset: bool,
    },

    /// Search MRT files in Broker db
    Search {
        #[clap(flatten)]
        query: BrokerSearchQuery,
    },
}

fn get_tokio_runtime() -> Runtime {
    // configure async runtime
    // let blocking_cpus = match num_cpus::get() {
    //     1 => 1,
    //     n => n,
    // };
    let blocking_cpus = num_cpus::get();

    info!("using {} cores for parsing html pages", blocking_cpus);
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

    tracing_subscriber::fmt().init();

    match cli.command {
        Commands::Serve { .. } => {
            // TODO: open with read-only mode
            // The service should serve via the exported parquet file instead of the database
        }
        Commands::Update { bootstrap, reset } => {
            // create a tokio runtime
            let rt = get_tokio_runtime();

            let db = LocalBrokerDb::new(config.local_db_file.as_str(), reset).unwrap();

            if bootstrap {
                db.bootstrap(config.local_db_bootstrap_path.as_str())
                    .unwrap()
            }

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

                info!("unordered buffer size is {}", buffer_size);

                let mut stream = futures::stream::iter(&collectors)
                    .map(|c| crawl_collector(c, latest_date))
                    .buffer_unordered(buffer_size);

                info!("start scraping for {} collectors", &collectors.len());
                while let Some(res) = stream.next().await {
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
            });
        }
        Commands::Search { query } => {
            let db = LocalBrokerDb::new(config.local_db_file.as_str(), false).unwrap();
            let items = process_search_query(query, &db).unwrap();

            let val = json!(items);
            println!("{}", serde_json::to_string_pretty(&val).unwrap());
        }
    }
}
