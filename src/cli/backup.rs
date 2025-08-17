use itertools::Itertools;
use std::process::{exit, Command};
use tracing::{error, info};

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

    if crate::is_local_path(backup_to) {
        backup_database(from, backup_to, true, sqlite_cmd_path)
    } else if let Some((bucket, s3_path)) = crate::parse_s3_path(backup_to) {
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
    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let temp_file_path = temp_dir
        .path()
        .join("temp.db")
        .to_str()
        .unwrap()
        .to_string();

    match backup_database(from, &temp_file_path, true, sqlite_cmd_path) {
        Ok(_) => {
            info!(
                "uploading backup file {} to S3 at s3://{}/{}",
                &temp_file_path, bucket, s3_path
            );
            match oneio::s3_upload(bucket, s3_path, &temp_file_path) {
                Ok(_) => {
                    info!("periodic backup file uploaded to S3");
                    Ok(())
                }
                Err(e) => {
                    error!("failed to upload periodic backup file to S3: {}", e);
                    Err(format!("failed to upload backup file to S3: {}", e))
                }
            }
        }
        Err(e) => {
            error!("failed to create periodic backup database: {}", e);
            Err(e)
        }
    }
}
