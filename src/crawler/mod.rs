mod common;
mod riperis;
mod routeviews;

use serde::{Deserialize, Serialize};

// public interface
pub use riperis::crawl_ripe_ris;
pub use routeviews::crawl_routeviews;

#[derive(Debug, Serialize, Deserialize)]
pub struct Collector {
    pub id: String,
    pub project: String,
    pub url: String,
}
