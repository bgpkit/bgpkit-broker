use itertools::Itertools;
use std::process::{exit, Command};
use tracing::{error, info};

pub(crate) fn backup_database(from: &str, to: &str, force: bool) {
    // back up to local directory
    if std::fs::metadata(&to).is_ok() && !force {
        error!("The specified database path already exists, skip backing up.");
        exit(1);
    }

    let mut command = Command::new("sqlite3");
    command.arg(&from).arg(format!(".backup {}", to).as_str());

    let command_str = format!(
        "{} {}",
        command.get_program().to_string_lossy(),
        command
            .get_args()
            .map(|s| {
                let str = s.to_string_lossy();
                // if string contains space, wrap it with single quote
                if str.contains(" ") {
                    format!("'{}'", str)
                } else {
                    str.to_string()
                }
            })
            .join(" ")
    );

    info!("running command: {}", command_str);

    let output = command.output().expect("Failed to execute command");

    if !output.status.success() {
        error!(
            "Command executed with error: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    } else {
        info!("Backup successfully to {}", to);
    }
}
