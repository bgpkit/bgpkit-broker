use crate::BrokerError;
use oneio::OneIoError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collector {
    pub id: String,
    pub project: String,
    pub url: String,
}

pub fn load_collectors(path: &str) -> Result<Vec<Collector>, BrokerError> {
    #[derive(Debug, Serialize, Deserialize)]
    struct Config {
        projects: Vec<ConfProject>,
    }
    #[derive(Debug, Serialize, Deserialize)]
    struct ConfProject {
        name: String,
        collectors: Vec<ConfCollector>,
    }
    #[derive(Debug, Serialize, Deserialize)]
    struct ConfCollector {
        id: String,
        url: String,
    }

    let config = match oneio::read_json_struct::<Config>(path) {
        Ok(config) => config,
        Err(e) => match e {
            OneIoError::IoError(e) => {
                return Err(BrokerError::IoError(e));
            }
            OneIoError::JsonParsingError(e) => {
                return Err(BrokerError::ConfigJsonError(e));
            }
            _ => {
                return Err(BrokerError::ConfigUnknownError(e.to_string()));
            }
        },
    };

    Ok(config
        .projects
        .into_iter()
        .flat_map(|project| {
            assert!(["routeviews", "riperis"].contains(&project.name.as_str()));
            let project_name = project.name.clone();
            project
                .collectors
                .into_iter()
                .map(|c| Collector {
                    id: c.id,
                    project: project_name.clone(),
                    url: c.url,
                })
                .collect::<Vec<Collector>>()
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_collectors() {
        let collectors = load_collectors("deployment/collectors.json").unwrap();
        dbg!(collectors);
    }
}
