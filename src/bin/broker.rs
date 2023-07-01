use bgpkit_broker::{crawl_collector, load_collectors, BrokerConfig, LocalBrokerDb};
use chrono::Utc;
use clap::{Parser, Subcommand};
use futures::StreamExt;
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
    Update {},

    /// Search MRT files in Broker db
    Search {},
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
        Commands::Update { .. } => {
            let rt = get_tokio_runtime();
            rt.block_on(async {
                // load all collectors from configuration file
                let collectors = load_collectors(config.collectors_file.as_str()).unwrap();

                // crawl all collectors in parallel, 10 collectors in parallel by default, unordered
                let buffer_size = 10;
                let mut stream = futures::stream::iter(&collectors)
                    .map(|c| {
                        // use "two hours ago" as default from_ts to avoid missing data during the bordering days between months
                        let from_date =
                            (Utc::now() - chrono::Duration::seconds(60 * 60 * 2)).date_naive();
                        crawl_collector(c, Some(from_date))
                    })
                    .buffer_unordered(buffer_size);

                info!("start scraping for {} collectors", &collectors.len());
                while let Some(res) = stream.next().await {
                    // opening db connection within the loop to reduce lock time
                    let mut db =
                        LocalBrokerDb::new(Some(config.local_db_file.clone()), false).unwrap();
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
        Commands::Search { .. } => {}
    }
}
