use config::Config;
use std::collections::HashMap;
use std::path::Path;

pub struct BrokerConfig {
    /// path to the file contains the list of collectors
    pub collectors_file: String,

    /// path to the db file that stores the broker data locally
    pub local_db_file: String,
}

const EMPTY_CONFIG: &str = r#"### broker configuration file

### path to the file contains the list of collectors
# collectors_file="https://spaces.bgpkit.org/broker/collectors.json"
# broker_db_local_path="~/.bgpkit/broker.duckdb"
# broker_db_backup_path="~/.bgpkit/broker-backup.parquet"
"#;

impl BrokerConfig {
    /// function to create and initialize a new configuration
    pub fn new(path: &Option<String>) -> BrokerConfig {
        let mut builder = Config::builder();
        // by default use $HOME/.bgpkit/broker.toml as the configuration file path
        let home_dir = dirs::home_dir().unwrap().to_str().unwrap().to_owned();
        // config dir
        let bgpkit_config_dir = format!("{}/.bgpkit", home_dir.as_str());

        // Add in toml configuration file
        match path {
            Some(p) => {
                let path = Path::new(p.as_str());
                if path.exists() {
                    builder = builder.add_source(config::File::with_name(path.to_str().unwrap()));
                } else {
                    std::fs::write(p.as_str(), EMPTY_CONFIG).expect("Unable to create config file");
                }
            }
            None => {
                std::fs::create_dir_all(bgpkit_config_dir.as_str()).unwrap();
                let p = format!("{}/broker.toml", bgpkit_config_dir.as_str());
                if Path::new(p.as_str()).exists() {
                    builder = builder.add_source(config::File::with_name(p.as_str()));
                } else {
                    std::fs::write(p.as_str(), EMPTY_CONFIG)
                        .unwrap_or_else(|_| panic!("Unable to create config file {}", p.as_str()));
                }
            }
        }
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `BGPKIT_BROKER_DEBUG=1 ./target/app` would set the `debug` key
        builder = builder.add_source(config::Environment::with_prefix("BGPKIT_BROKER"));

        let settings = builder.build().unwrap();
        let config = settings
            .try_deserialize::<HashMap<String, String>>()
            .unwrap();

        let collectors_file = match config.get("collectors_file") {
            Some(p) => p.to_owned(),
            None => "https://spaces.bgpkit.org/broker/collectors.json".to_string(),
        };

        let local_db_file = match config.get("broker_db_local_path") {
            Some(p) => {
                let path = Path::new(p);
                path.to_str().unwrap().to_string()
            }
            None => {
                format!("{}/broker.duckdb", bgpkit_config_dir.as_str())
            }
        };

        BrokerConfig {
            collectors_file,
            local_db_file,
        }
    }
}
