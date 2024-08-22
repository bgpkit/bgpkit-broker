// https://spaces.bgpkit.org/broker/bgpkit_broker.sqlite3

use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use std::cmp::min;
use std::fs::File;
use std::io::Write;
use std::time::Duration;

pub async fn download_file(url: &str, path: &str, silent: bool) -> Result<(), String> {
    let client = reqwest::ClientBuilder::new()
        .user_agent("bgpkit-broker/3")
        .timeout(Duration::from_secs(30))
        .build()
        .or(Err("Failed to create reqwest client".to_string()))?;

    // Reqwest setup
    let res = client
        .get(url)
        .send()
        .await
        .or(Err(format!("Failed to GET from '{}'", &url)))?;
    let total_size = res
        .content_length()
        .ok_or(format!("Failed to get content length from '{}'", &url))?;

    // Indicatif setup
    let pb = ProgressBar::new(total_size);
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})").unwrap()
        .progress_chars("#>-"));
    if !silent {
        pb.set_message(format!("Downloading {} to {}...", url, path));
    }

    // download chunks
    let mut file = File::create(path).or(Err(format!("Failed to create file '{}'", path)))?;
    let mut downloaded: u64 = 0;
    let mut stream = res.bytes_stream();

    while let Some(item) = stream.next().await {
        let chunk = item.or(Err("Error while downloading file".to_string()))?;
        file.write_all(&chunk)
            .or(Err("Error while writing to file".to_string()))?;
        let new = min(downloaded + (chunk.len() as u64), total_size);
        downloaded = new;
        if !silent {
            pb.set_position(new);
        }
    }

    if !silent {
        pb.finish_with_message(format!("Downloading {} to {}... Done", url, path));
    }
    Ok(())
}
