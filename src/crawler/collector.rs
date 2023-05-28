use crate::BrokerError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
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

    let config_file = std::fs::File::open(path)?;
    let config: Config = serde_json::from_reader(config_file)?;

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
