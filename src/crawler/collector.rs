use crate::BrokerError;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collector {
    pub id: String,
    pub project: String,
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    projects: Vec<ConfProject>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConfProject {
    name: String,
    collectors: Vec<ConfCollector>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConfCollector {
    id: String,
    url: String,
}

pub fn load_collectors() -> Result<Vec<Collector>, BrokerError> {
    // load config
    info!("loading default collectors config");
    let config: Config = DEFAULT_COLLECTORS_CONFIG.clone();

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

lazy_static! {
    pub static ref DEFAULT_COLLECTORS_CONFIG: Config = serde_json::from_str(
        r#"
    {
  "projects": [
    {
      "name": "riperis",
      "collectors": [
        {
          "id": "rrc00",
          "url": "https://data.ris.ripe.net/rrc00"
        },
        {
          "id": "rrc01",
          "url": "https://data.ris.ripe.net/rrc01"
        },
        {
          "id": "rrc02",
          "url": "https://data.ris.ripe.net/rrc02"
        },
        {
          "id": "rrc03",
          "url": "https://data.ris.ripe.net/rrc03"
        },
        {
          "id": "rrc04",
          "url": "https://data.ris.ripe.net/rrc04"
        },
        {
          "id": "rrc05",
          "url": "https://data.ris.ripe.net/rrc05"
        },
        {
          "id": "rrc06",
          "url": "https://data.ris.ripe.net/rrc06"
        },
        {
          "id": "rrc07",
          "url": "https://data.ris.ripe.net/rrc07"
        },
        {
          "id": "rrc08",
          "url": "https://data.ris.ripe.net/rrc08"
        },
        {
          "id": "rrc09",
          "url": "https://data.ris.ripe.net/rrc09"
        },
        {
          "id": "rrc10",
          "url": "https://data.ris.ripe.net/rrc10"
        },
        {
          "id": "rrc11",
          "url": "https://data.ris.ripe.net/rrc11"
        },
        {
          "id": "rrc12",
          "url": "https://data.ris.ripe.net/rrc12"
        },
        {
          "id": "rrc13",
          "url": "https://data.ris.ripe.net/rrc13"
        },
        {
          "id": "rrc14",
          "url": "https://data.ris.ripe.net/rrc14"
        },
        {
          "id": "rrc15",
          "url": "https://data.ris.ripe.net/rrc15"
        },
        {
          "id": "rrc16",
          "url": "https://data.ris.ripe.net/rrc16"
        },
        {
          "id": "rrc18",
          "url": "https://data.ris.ripe.net/rrc18"
        },
        {
          "id": "rrc19",
          "url": "https://data.ris.ripe.net/rrc19"
        },
        {
          "id": "rrc20",
          "url": "https://data.ris.ripe.net/rrc20"
        },
        {
          "id": "rrc21",
          "url": "https://data.ris.ripe.net/rrc21"
        },
        {
          "id": "rrc22",
          "url": "https://data.ris.ripe.net/rrc22"
        },
        {
          "id": "rrc23",
          "url": "https://data.ris.ripe.net/rrc23"
        },
        {
          "id": "rrc24",
          "url": "https://data.ris.ripe.net/rrc24"
        },
        {
          "id": "rrc25",
          "url": "https://data.ris.ripe.net/rrc25"
        },
        {
          "id": "rrc26",
          "url": "https://data.ris.ripe.net/rrc26"
        }
      ]
    },
    {
      "name": "routeviews",
      "collectors": [
        {
          "id": "route-views2",
          "url": "http://archive.routeviews.org/bgpdata"
        },
        {
          "id": "route-views3",
          "url": "http://archive.routeviews.org/route-views3/bgpdata"
        },
        {
          "id": "route-views4",
          "url": "http://archive.routeviews.org/route-views4/bgpdata"
        },
        {
          "id": "route-views5",
          "url": "http://archive.routeviews.org/route-views5/bgpdata"
        },
        {
          "id": "route-views6",
          "url": "http://archive.routeviews.org/route-views6/bgpdata"
        },
        {
          "id":"route-views.amsix",
          "url": "http://archive.routeviews.org/route-views.amsix/bgpdata"
        },
        {
          "id":"route-views.chicago",
          "url": "http://archive.routeviews.org/route-views.chicago/bgpdata"
        },
        {
          "id":"route-views.chile",
          "url": "http://archive.routeviews.org/route-views.chile/bgpdata"
        },
        {
          "id":"route-views.eqix",
          "url": "http://archive.routeviews.org/route-views.eqix/bgpdata"
        },
        {
          "id":"route-views.flix",
          "url": "http://archive.routeviews.org/route-views.flix/bgpdata"
        },
        {
          "id":"route-views.gorex",
          "url": "http://archive.routeviews.org/route-views.gorex/bgpdata"
        },
        {
          "id":"route-views.isc",
          "url": "http://archive.routeviews.org/route-views.isc/bgpdata"
        },
        {
          "id":"route-views.kixp",
          "url": "http://archive.routeviews.org/route-views.kixp/bgpdata"
        },
        {
          "id":"route-views.jinx",
          "url": "http://archive.routeviews.org/route-views.jinx/bgpdata"
        },
        {
          "id":"route-views.linx",
          "url": "http://archive.routeviews.org/route-views.linx/bgpdata"
        },
        {
          "id":"route-views.napafrica",
          "url": "http://archive.routeviews.org/route-views.napafrica/bgpdata"
        },
        {
          "id":"route-views.nwax",
          "url": "http://archive.routeviews.org/route-views.nwax/bgpdata"
        },
        {
          "id":"route-views.phoix",
          "url": "http://archive.routeviews.org/route-views.phoix/bgpdata"
        },
        {
          "id":"route-views.telxatl",
          "url": "http://archive.routeviews.org/route-views.telxatl/bgpdata"
        },
        {
          "id":"route-views.wide",
          "url": "http://archive.routeviews.org/route-views.wide/bgpdata"
        },
        {
          "id":"route-views.sydney",
          "url": "http://archive.routeviews.org/route-views.sydney/bgpdata"
        },
        {
          "id":"route-views.saopaulo",
          "url": "http://archive.routeviews.org/route-views.saopaulo/bgpdata"
        },
        {
          "id":"route-views2.saopaulo",
          "url": "http://archive.routeviews.org/route-views2.saopaulo/bgpdata"
        },
        {
          "id":"route-views.sg",
          "url": "http://archive.routeviews.org/route-views.sg/bgpdata"
        },
        {
          "id":"route-views.perth",
          "url": "http://archive.routeviews.org/route-views.perth/bgpdata"
        },
        {
          "id":"route-views.peru",
          "url": "http://archive.routeviews.org/route-views.peru/bgpdata"
        },
        {
          "id":"route-views.sfmix",
          "url": "http://archive.routeviews.org/route-views.sfmix/bgpdata"
        },
        {
          "id":"route-views.siex",
          "url": "http://archive.routeviews.org/route-views.siex/bgpdata"
        },
        {
          "id":"route-views.soxrs",
          "url": "http://archive.routeviews.org/route-views.soxrs/bgpdata"
        },
        {
          "id":"route-views.mwix",
          "url": "http://archive.routeviews.org/route-views.mwix/bgpdata"
        },
        {
          "id":"route-views.rio",
          "url": "http://archive.routeviews.org/route-views.rio/bgpdata"
        },
        {
          "id":"route-views.fortaleza",
          "url": "http://archive.routeviews.org/route-views.fortaleza/bgpdata"
        },
        {
          "id":"route-views.gixa",
          "url": "http://archive.routeviews.org/route-views.gixa/bgpdata"
        },
        {
          "id":"route-views.bdix",
          "url": "http://archive.routeviews.org/route-views.bdix/bgpdata"
        },
        {
          "id":"route-views.bknix",
          "url": "http://archive.routeviews.org/route-views.bknix/bgpdata"
        },
        {
          "id":"route-views.ny",
          "url": "http://archive.routeviews.org/route-views.ny/bgpdata"
        },
        {
          "id":"route-views.uaeix",
          "url": "http://archive.routeviews.org/route-views.uaeix/bgpdata"
        }
      ]
    }
  ]
}
    "#
    )
    .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_collectors() {
        let collectors = load_collectors().unwrap();
        dbg!(collectors);
    }
}
