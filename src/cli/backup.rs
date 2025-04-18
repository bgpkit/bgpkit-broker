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
