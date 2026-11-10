//! Per-project crawl freshness.
//!
//! A crawl round can complete, insert nothing, and log a healthy summary while an origin
//! refuses the crawler or a mirror serves a frozen listing. Per-collector staleness is not
//! a signal: collectors retire and stay quiet forever. A whole project that stops
//! advancing is, because both archives publish continuously.
//!
//! The tracker compares the newest slot a project has in the store at the end of each
//! round. Reading the store instead of the crawler's return value is deliberate: a frozen
//! listing, a blocked origin and a parse that yields no links all look the same from the
//! crawler's side, and all of them leave the store behind.

use chrono::NaiveDateTime;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use tracing::warn;

/// Consecutive rounds without progress before a project is called stalled. One round can
/// legitimately bring nothing when an archive publishes slowly; three cannot.
pub const STALL_WARN_ROUNDS: u32 = 3;

#[derive(Default)]
pub struct FreshnessTracker {
    newest: HashMap<String, NaiveDateTime>,
    stalled_rounds: HashMap<String, u32>,
}

impl FreshnessTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the newest slot a project has in the store after a round.
    ///
    /// Returns how many consecutive rounds it has failed to advance. The first observation
    /// only establishes the baseline.
    pub fn observe(&mut self, project: &str, newest: Option<NaiveDateTime>) -> u32 {
        let Some(newest) = newest else {
            return self.stalled_rounds(project);
        };

        match self.newest.get(project) {
            Some(previous) if newest <= *previous => {
                let rounds = self.stalled_rounds.entry(project.to_string()).or_insert(0);
                *rounds += 1;
                *rounds
            }
            _ => {
                self.newest.insert(project.to_string(), newest);
                self.stalled_rounds.insert(project.to_string(), 0);
                0
            }
        }
    }

    pub fn stalled_rounds(&self, project: &str) -> u32 {
        self.stalled_rounds.get(project).copied().unwrap_or(0)
    }

    pub fn newest(&self, project: &str) -> Option<NaiveDateTime> {
        self.newest.get(project).copied()
    }
}

/// Process-wide tracker. One crawler runs per process, so this is cheaper and less
/// error-prone than threading the tracker through every call site of `update_database`.
static TRACKER: OnceLock<Mutex<FreshnessTracker>> = OnceLock::new();

fn tracker() -> &'static Mutex<FreshnessTracker> {
    TRACKER.get_or_init(|| Mutex::new(FreshnessTracker::new()))
}

/// Update the tracker with one round's per-project newest timestamps and publish the
/// result as gauges next to the crawl counters.
pub fn record_round(latest: &[(String, NaiveDateTime)]) {
    let mut guard = match tracker().lock() {
        Ok(guard) => guard,
        // A panic elsewhere must not take freshness reporting down with it.
        Err(poisoned) => poisoned.into_inner(),
    };

    for (project, newest) in latest {
        let rounds = guard.observe(project, Some(*newest));
        metrics::gauge!(
            "bgpkit_broker_crawler_project_newest_timestamp_seconds",
            "project" => project.clone()
        )
        .set(newest.and_utc().timestamp() as f64);
        metrics::gauge!(
            "bgpkit_broker_crawler_project_stalled_rounds",
            "project" => project.clone()
        )
        .set(rounds as f64);

        if rounds == STALL_WARN_ROUNDS {
            warn!(
                "project {} has not advanced for {} consecutive crawl rounds (newest slot {})",
                project, rounds, newest
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(hour: u32) -> NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(2026, 11, 10)
            .expect("date")
            .and_hms_opt(hour, 0, 0)
            .expect("time")
    }

    #[test]
    fn first_observation_only_sets_the_baseline() {
        let mut tracker = FreshnessTracker::new();
        assert_eq!(tracker.observe("route-views", Some(ts(18))), 0);
        assert_eq!(tracker.stalled_rounds("route-views"), 0);
        assert_eq!(tracker.newest("route-views"), Some(ts(18)));
    }

    #[test]
    fn repeated_slot_counts_as_stalled() {
        let mut tracker = FreshnessTracker::new();
        tracker.observe("route-views", Some(ts(18)));
        assert_eq!(tracker.observe("route-views", Some(ts(18))), 1);
        assert_eq!(tracker.observe("route-views", Some(ts(18))), 2);
        assert_eq!(tracker.observe("route-views", Some(ts(18))), 3);
    }

    #[test]
    fn advancing_slot_resets_the_counter() {
        let mut tracker = FreshnessTracker::new();
        tracker.observe("route-views", Some(ts(18)));
        tracker.observe("route-views", Some(ts(18)));
        assert_eq!(tracker.stalled_rounds("route-views"), 1);
        assert_eq!(tracker.observe("route-views", Some(ts(19))), 0);
        assert_eq!(tracker.newest("route-views"), Some(ts(19)));
    }

    #[test]
    fn a_project_without_data_keeps_its_last_count() {
        let mut tracker = FreshnessTracker::new();
        tracker.observe("ripe-ris", Some(ts(18)));
        tracker.observe("ripe-ris", Some(ts(18)));
        assert_eq!(tracker.observe("ripe-ris", None), 1);
    }

    #[test]
    fn projects_are_tracked_independently() {
        let mut tracker = FreshnessTracker::new();
        tracker.observe("route-views", Some(ts(18)));
        tracker.observe("ripe-ris", Some(ts(18)));
        assert_eq!(tracker.observe("route-views", Some(ts(18))), 1);
        assert_eq!(tracker.observe("ripe-ris", Some(ts(19))), 0);
        assert_eq!(tracker.stalled_rounds("route-views"), 1);
        assert_eq!(tracker.stalled_rounds("ripe-ris"), 0);
    }
}
