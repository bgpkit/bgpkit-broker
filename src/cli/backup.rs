use itertools::Itertools;
use std::process::{exit, Command};
use tracing::{error, info};

/// Number of upload attempts (including the first) before a backup attempt
/// gives up and waits for the next backup interval window.
const S3_UPLOAD_MAX_ATTEMPTS: u32 = 3;

/// Initial delay before the first upload retry; doubles on each retry.
const S3_UPLOAD_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

pub(crate) fn backup_database(
    from: &str,
    to: &str,
    force: bool,
    sqlite_cmd_path: Option<String>,
) -> Result<(), String> {
    // back up to local directory
    if std::fs::metadata(to).is_ok() && !force {
        error!("The specified database path already exists, skip backing up.");
        exit(1);
    }

    let sqlite_path = sqlite_cmd_path.unwrap_or_else(|| match which::which("sqlite3") {
        Ok(p) => p.to_string_lossy().to_string(),
        Err(_) => {
            error!("sqlite3 not found in PATH, please install sqlite3 first.");
            exit(1);
        }
    });

    let mut command = Command::new(sqlite_path.as_str());
    command.arg(from).arg(format!(".backup {}", to).as_str());

    let command_str = format!(
        "{} {}",
        command.get_program().to_string_lossy(),
        command
            .get_args()
            .map(|s| {
                let str = s.to_string_lossy();
                // if string contains space, wrap it with single quote
                if str.contains(' ') {
                    format!("'{}'", str)
                } else {
                    str.to_string()
                }
            })
            .join(" ")
    );

    info!("running command: {}", command_str);

    let output = command.output().expect("Failed to execute command");

    match output.status.success() {
        true => Ok(()),
        false => Err(format!(
            "Command executed with error: {}",
            String::from_utf8_lossy(&output.stderr)
        )),
    }
}

pub(crate) async fn perform_periodic_backup(
    from: &str,
    backup_to: &str,
    sqlite_cmd_path: Option<String>,
) -> Result<(), String> {
    info!("performing periodic backup from {} to {}", from, backup_to);

    if crate::utils::is_local_path(backup_to) {
        backup_database(from, backup_to, true, sqlite_cmd_path)
    } else if let Some((bucket, s3_path)) = crate::utils::parse_s3_path(backup_to) {
        perform_s3_backup(from, &bucket, &s3_path, sqlite_cmd_path).await
    } else {
        Err("invalid backup destination format".to_string())
    }
}

async fn perform_s3_backup(
    from: &str,
    bucket: &str,
    s3_path: &str,
    sqlite_cmd_path: Option<String>,
) -> Result<(), String> {
    let temp_dir =
        tempfile::tempdir().map_err(|e| format!("failed to create temporary directory: {}", e))?;
    let temp_file_path = temp_dir
        .path()
        .join("temp.db")
        .to_str()
        .ok_or("failed to convert temp file path to string")?
        .to_string();

    match backup_database(from, &temp_file_path, true, sqlite_cmd_path) {
        Ok(_) => {
            info!(
                "uploading backup file {} to S3 at s3://{}/{}",
                &temp_file_path, bucket, s3_path
            );
            s3_upload_with_retry(bucket, s3_path, &temp_file_path).await
        }
        Err(e) => {
            error!("failed to create periodic backup database: {}", e);
            Err(e)
        }
    }
}

/// Upload with a bounded number of attempts and exponential backoff, so one
/// flaky transfer does not restart the full file from byte zero on every
/// update tick. The outer serve loop additionally gates retries on the backup
/// interval, see issue #107.
async fn s3_upload_with_retry(bucket: &str, s3_path: &str, file_path: &str) -> Result<(), String> {
    let mut delay = S3_UPLOAD_RETRY_DELAY;
    for attempt in 1..=S3_UPLOAD_MAX_ATTEMPTS {
        match oneio::s3_upload(bucket, s3_path, file_path) {
            Ok(_) => {
                info!("periodic backup file uploaded to S3");
                return Ok(());
            }
            Err(e) if attempt == S3_UPLOAD_MAX_ATTEMPTS => {
                error!(
                    "failed to upload periodic backup file to S3 after {} attempts: {}",
                    attempt, e
                );
                return Err(format!("failed to upload backup file to S3: {}", e));
            }
            Err(e) => {
                error!(
                    "upload attempt {}/{} failed: {}; retrying in {:?}",
                    attempt, S3_UPLOAD_MAX_ATTEMPTS, e, delay
                );
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
        }
    }
    unreachable!("loop always returns on the final attempt")
}

/// Decide whether a new backup attempt should start now, given the last
/// attempt time and the configured interval. Attempts (not successes) advance
/// the gate: a failed backup is not retried until a full interval has
/// elapsed, preventing a full-file re-upload on every update tick (#107).
#[cfg_attr(test, allow(dead_code))]
pub(crate) fn backup_due(last_attempt: std::time::Instant, interval: std::time::Duration) -> bool {
    last_attempt.elapsed() >= interval
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_due_requires_full_interval_since_last_attempt() {
        let interval = std::time::Duration::from_secs(60);
        // A gate opened a moment ago is not due.
        assert!(!backup_due(std::time::Instant::now(), interval));
    }

    #[test]
    fn backup_due_passes_after_interval_elapsed() {
        let interval = std::time::Duration::from_secs(0);
        // Zero interval means the gate has fully elapsed.
        assert!(backup_due(std::time::Instant::now(), interval));
    }

    #[test]
    fn upload_attempt_bounds_match_retry_policy() {
        assert_eq!(S3_UPLOAD_MAX_ATTEMPTS, 3);
        assert_eq!(S3_UPLOAD_RETRY_DELAY, std::time::Duration::from_secs(2));
    }
}
