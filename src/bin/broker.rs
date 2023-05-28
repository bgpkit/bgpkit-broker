use bgpkit_broker::{crawl_collector, load_collectors, BrokerDb};
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

    tracing_subscriber::fmt().init();

    match cli.command {
        Commands::Serve { .. } => {
            // TODO: open with read-only mode
        }
        Commands::Update { .. } => {
            let rt = get_tokio_runtime();
            rt.block_on(async {
                let mut db = BrokerDb::new(Some("broker.duckdb".to_string()), true).unwrap();
                let collectors = load_collectors("deployment/collectors.json").unwrap();

                let buffer_size = 20;

                let mut stream = futures::stream::iter(&collectors)
                    .map(|c| {
                        let two_months_ago = Utc::now().date_naive() - chrono::Duration::days(60);
                        crawl_collector(c, Some(two_months_ago))
                    })
                    .buffer_unordered(buffer_size);

                info!("start scraping for {} collectors", &collectors.len());
                while let Some(res) = stream.next().await {
                    match res {
                        Ok(items) => {
                            db.insert_items(&items).unwrap();
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
