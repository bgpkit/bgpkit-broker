use serde::Deserialize;
use std::env::set_var;

/// BGPKIT Broker configuration
///
/// Main environment variables, each with a default value:
///
/// - `BGPKIT_BROKER_COLLECTORS_CONFIG`: path to the file contains the list of collectors
///     - default: `https://spaces.bgpkit.org/broker/collectors.json`
/// - `BGPKIT_BROKER_LOCAL_DB_PATH`: path to the db file that stores the broker data locally
///     - default: `./bgpkit/broker.duckdb`
/// - `BGPKIT_BROKER_DB_BOOTSTRAP_PATH`: path to the db file bootstrap parquet file
///     - default: `https://data.bgpkit.com/broker/broker-backup.duckdb`
/// - `BGPKIT_BROKER_CONFIG_DIR`: configuration file
///    - default: `./bgpkit/`
/// - `BGPKIT_BROKER_BACKUP_DIR`: backup directory
///   - default: `./bgpkit/backup/`
///
/// S3 backup environment variables, all required to enable S3 backup:
/// - `BGPKIT_BROKER_S3_REGION`: S3 backup configuration: region
/// - `BGPKIT_BROKER_S3_ACCESS_KEY_ID`: S3 backup configuration: access key id
/// - `BGPKIT_BROKER_S3_SECRET_ACCESS_KEY`: S3 backup configuration: secret access key
/// - `BGPKIT_BROKER_S3_ENDPOINT`: S3 backup configuration: endpoint
/// - `BGPKIT_BROKER_S3_BUCKET`: S3 backup configuration: bucket
/// - `BGPKIT_BROKER_S3_DIR`: S3 backup configuration: directory
#[derive(Deserialize, Debug)]
pub struct BrokerConfig {
    /// path to the file contains the list of collectors
    #[serde(default = "default_collectors_file")]
    pub collectors_config: String,

    /// path to the db file that stores the broker data locally
    #[serde(default = "default_local_db_path")]
    pub local_db_path: String,

    /// path to the db file bootstrap parquet file
    #[serde(default = "default_bootstrap_path")]
    pub db_bootstrap_path: String,

    /// configuration file
    #[serde(default = "default_config_dir")]
    pub config_dir: String,

    /// backup directory
    #[serde(default = "default_backup_dir")]
    pub backup_dir: String,

    /// S3 backup configuration: region
    pub s3_region: Option<String>,

    /// S3 backup configuration: access key id
    pub s3_access_key_id: Option<String>,

    /// S3 backup configuration: secret access key
    pub s3_secret_access_key: Option<String>,

    /// S3 backup configuration: endpoint
    pub s3_endpoint: Option<String>,

    /// S3 backup configuration: bucket
    pub s3_bucket: Option<String>,

    /// S3 backup configuration: directory
    pub s3_dir: Option<String>,
}

fn default_collectors_file() -> String {
    "https://spaces.bgpkit.org/broker/collectors.json".to_string()
}

fn default_local_db_path() -> String {
    let home_dir = dirs::home_dir().unwrap().to_str().unwrap().to_owned();
    format!("{}/.bgpkit/broker.duckdb", home_dir.as_str())
}

fn default_bootstrap_path() -> String {
    "https://data.bgpkit.com/broker/broker-backup.duckdb".to_string()
}

fn default_config_dir() -> String {
    let home_dir = dirs::home_dir().unwrap().to_str().unwrap().to_owned();
    let dir_path = format!("{}/.bgpkit", home_dir.as_str());
    std::fs::create_dir_all(dir_path.as_str()).unwrap();
    dir_path
}

fn default_backup_dir() -> String {
    let home_dir = dirs::home_dir().unwrap().to_str().unwrap().to_owned();
    let dir_path = format!("{}/.bgpkit/backup", home_dir.as_str());
    std::fs::create_dir_all(dir_path.as_str()).unwrap();
    dir_path
}

impl BrokerConfig {
    /// whether to enable S3 backup, if so, set the corresponding environment variables
    pub fn do_s3_backup(&self) -> bool {
        if self.s3_region.is_some()
            && self.s3_access_key_id.is_some()
            && self.s3_secret_access_key.is_some()
            && self.s3_endpoint.is_some()
            && self.s3_bucket.is_some()
            && self.s3_dir.is_some()
        {
            set_var("AWS_ACCESS_KEY_ID", self.s3_access_key_id.as_ref().unwrap());
            set_var(
                "AWS_SECRET_ACCESS_KEY",
                self.s3_secret_access_key.as_ref().unwrap(),
            );
            set_var("AWS_REGION", self.s3_region.as_ref().unwrap());
            set_var("AWS_ENDPOINT", self.s3_endpoint.as_ref().unwrap());

            return true;
        }
        false
    }
}
