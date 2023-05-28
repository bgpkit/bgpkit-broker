use crate::BrokerError;
use chrono::{Datelike, NaiveDate, Utc};
use regex::Regex;
use scraper::{Html, Selector};

const SIZE_KB: u64 = u64::pow(1024, 1);
const SIZE_MB: u64 = u64::pow(1024, 2);
const SIZE_GB: u64 = u64::pow(1024, 3);

fn size_str_to_bytes(size_str: &str, size_pattern: &Regex) -> Option<i64> {
    let cap = match size_pattern.captures(size_str) {
        Some(x) => x,
        None => return None,
    };
    let mut size = match cap[1].to_string().parse::<f64>() {
        Ok(x) => x,
        Err(_) => return None,
    };
    size *= match cap[2].to_ascii_lowercase().as_str() {
        "k" => SIZE_KB,
        "m" => SIZE_MB,
        "g" => SIZE_GB,
        "" => 1,
        other => panic!("unknown file size multiplier {}", other),
    } as f64;
    Some(size as i64)
}

/// Extract the MRT file links and file sizes from the body of a RouteViews or RIPE RIS page.
///
/// # Arguments
///
/// * `body`: RouteViews or RIPE RIS page body
///
/// returns: Vec<(String, i64)>
pub fn extract_link_size(body: &str) -> Vec<(String, i64)> {
    let mut res: Vec<(String, i64)> = vec![];

    if body.contains("table") {
        let size_pattern: Regex = Regex::new(r#" *([\d.]+)([MKGmkg]*)"#).unwrap();
        // table-based html pages, works with RouteViews and RIPE RIS old version
        let fragment = Html::parse_fragment(body);
        let row_selector = Selector::parse("tr").unwrap();
        let link_selector = Selector::parse("a").unwrap();
        for elem in fragment.select(&row_selector) {
            let text_arr = elem
                .text()
                .filter(|t| t.is_ascii() && !t.trim().is_empty())
                .collect::<Vec<_>>();
            let text = text_arr.join("");
            if text.is_empty() || text.contains("Name") || text.contains("Parent") {
                continue;
            }
            let href = elem
                .select(&link_selector)
                .next()
                .unwrap()
                .value()
                .attr("href");
            let size = match size_str_to_bytes(text_arr[2], &size_pattern) {
                None => continue,
                Some(v) => v,
            };
            res.push((href.unwrap().to_string(), size));
        }
    } else {
        for line in body.lines() {
            let size_pattern: Regex = Regex::new(r#" +([\d.]+)([MKGmkg]*)$"#).unwrap();
            let size = size_str_to_bytes(line, &size_pattern);
            if size.is_none() {
                continue;
            }

            let fragment = Html::parse_fragment(line);
            let link_selector = Selector::parse("a").unwrap();
            let mut link = "".to_string();
            if let Some(elem) = fragment.select(&link_selector).next() {
                link = elem.value().attr("href").unwrap().to_string();
            }
            res.push((link, size.unwrap()));
        }
    }
    res
}

/// Remove trailing slash from a string.
///
/// # Arguments
///
/// * `s`:
///
/// returns: String
pub(crate) fn remove_trailing_slash(s: impl ToString) -> String {
    let mut s = s.to_string();
    if s.ends_with("/") {
        s.pop();
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_link_size() {
        const RIPE_OLD: &str = r#"<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of /rrc00/2022.11</title>
 </head>
 <body>
<h1>Index of /rrc00/2022.11</h1>
  <table>
   <tr><th valign="top">&nbsp;</th><th><a href="?C=N;O=A">Name</a></th><th><a href="?C=M;O=A">Last modified</a></th><th><a href="?C=S;O=A">Size</a></th><th><a href="?C=D;O=A">Description</a></th></tr>
   <tr><th colspan="5"><hr></th></tr>
<tr><td valign="top">&nbsp;</td><td><a href="/rrc00/">Parent Directory</a></td><td>&nbsp;</td><td align="right">  - </td><td>&nbsp;</td></tr>
<tr><td valign="top">&nbsp;</td><td><a href="updates.20221128.2220.gz">updates.20221128.2220.gz</a></td><td align="right">2022-11-28 22:25  </td><td align="right">6.4M</td><td>&nbsp;</td></tr>
<tr><td valign="top">&nbsp;</td><td><a href="updates.20221128.2215.gz">updates.20221128.2215.gz</a></td><td align="right">2022-11-28 22:20  </td><td align="right">3.8M</td><td>&nbsp;</td></tr>
<tr><td valign="top">&nbsp;</td><td><a href="bview.20221102.0800.gz">bview.20221102.0800.gz</a></td><td align="right">2022-11-02 10:14  </td><td align="right">1.5G</td><td>&nbsp;</td></tr>
<tr><td valign="top">&nbsp;</td><td><a href="bview.20221102.0000.gz">bview.20221102.0000.gz</a></td><td align="right">2022-11-02 02:13  </td><td align="right">1.5G</td><td>&nbsp;</td></tr>
   <tr><th colspan="5"><hr></th></tr>
</table>
</body></html>
"#;

        const RIPE_NEW: &str = r#"<html>
<head><title>Index of /rrc00/2001.01/</title></head>
<body bgcolor="white">
<h1>Index of /rrc00/2001.01/</h1><hr><pre><a href="../">../</a>
<a href="bview.20010101.0609.gz">bview.20010101.0609.gz</a>                             01-Jan-2001 06:09     12M
<a href="bview.20010101.1410.gz">bview.20010101.1410.gz</a>                             01-Jan-2001 14:10     12M
<a href="updates.20010131.2236.gz">updates.20010131.2236.gz</a>                           31-Jan-2001 22:36     98K
<a href="updates.20010131.2251.gz">updates.20010131.2251.gz</a>                           31-Jan-2001 22:51     97K
</pre><hr></body>
</html>
"#;
        let res = extract_link_size(RIPE_NEW);
        assert_eq!(res.len(), 4);

        const ROUTEVIEWS: &str = r#"<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of /route-views.bdix/bgpdata/2022.10/UPDATES</title>
 </head>
 <body>
<h1>Index of /route-views.bdix/bgpdata/2022.10/UPDATES</h1>
  <table>
   <tr><th valign="top"><img src="/icons/blank.gif" alt="[ICO]"></th><th><a href="?C=N;O=D">Name</a></th><th><a href="?C=M;O=A">Last modified</a></th><th><a href="?C=S;O=A">Size</a></th><th><a href="?C=D;O=A">Description</a></th></tr>
   <tr><th colspan="5"><hr></th></tr>
<tr><td valign="top"><img src="/icons/back.gif" alt="[PARENTDIR]"></td><td><a href="/route-views.bdix/bgpdata/2022.10/">Parent Directory</a>       </td><td>&nbsp;</td><td align="right">  - </td><td>&nbsp;</td></tr>
<tr><td valign="top"><img src="/icons/unknown.gif" alt="[   ]"></td><td><a href="updates.20221001.0000.bz2">updates.20221001.000..&gt;</a></td><td align="right">2022-10-01 00:00  </td><td align="right"> 14 </td><td>&nbsp;</td></tr>
<tr><td valign="top"><img src="/icons/unknown.gif" alt="[   ]"></td><td><a href="updates.20221001.0015.bz2">updates.20221001.001..&gt;</a></td><td align="right">2022-10-01 00:15  </td><td align="right"> 14 </td><td>&nbsp;</td></tr>
<tr><td valign="top"><img src="/icons/unknown.gif" alt="[   ]"></td><td><a href="updates.20221026.1545.bz2">updates.20221026.154..&gt;</a></td><td align="right">2022-10-26 15:45  </td><td align="right"> 14 </td><td>&nbsp;</td></tr>
<tr><td valign="top"><img src="/icons/unknown.gif" alt="[   ]"></td><td><a href="updates.20221026.1600.bz2">updates.20221026.160..&gt;</a></td><td align="right">2022-10-26 16:00  </td><td align="right"> 14 </td><td>&nbsp;</td></tr>
   <tr><th colspan="5"><hr></th></tr>
</table>
</body></html>
"#;

        let res = extract_link_size(RIPE_OLD);
        assert_eq!(res.len(), 4);
        let res = extract_link_size(ROUTEVIEWS);
        assert_eq!(res.len(), 4);
    }
}

pub(crate) async fn crawl_months_list(
    collector_root_url: &str,
    from_month: Option<NaiveDate>,
) -> Result<Vec<NaiveDate>, BrokerError> {
    let rounded_month = from_month
        .map(|d| NaiveDate::from_ymd_opt(d.year(), d.month(), 1))
        .unwrap();

    let month_link_pattern: Regex = Regex::new(r#"<a href="(....\...)/">.*"#).unwrap();
    let body = reqwest::get(collector_root_url).await?.text().await?;
    let mut res = vec![];
    for cap in month_link_pattern.captures_iter(body.as_str()) {
        let month = cap[1].to_owned();
        let parsed_month =
            NaiveDate::parse_from_str(format!("{}.01", month.as_str()).as_str(), "%Y.%m.%d")?;
        if let Some(rounded) = rounded_month {
            let new_month = NaiveDate::from_ymd_opt(rounded.year(), rounded.month(), 1).unwrap();
            if parsed_month < new_month {
                continue;
            }
        }
        if parsed_month > Utc::now().naive_utc().date() {
            continue;
        }
        res.push(parsed_month);
    }
    Ok(res)
}
