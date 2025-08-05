//! This example demonstrates the convenience shortcuts for common BGP data queries.
//!
//! The shortcuts module provides three main convenience methods:
//! - daily_ribs(): Get RIB files captured at midnight (daily snapshots)
//! - recent_updates(hours): Get update files from the last N hours
//! - most_diverse_collectors(n, project): Find collectors with the most diverse peer ASNs, optionally filtered by project

fn main() {
    let broker = bgpkit_broker::BgpkitBroker::new();

    // Example 1: Get daily RIB files from January 1, 2024
    println!("=== Daily RIBs Example ===");
    match broker
        .clone()
        .ts_start("2024-01-01")
        .ts_end("2024-01-02")
        .collector_id("route-views2")
        .daily_ribs()
    {
        Ok(daily_ribs) => {
            println!("Found {} daily RIB files", daily_ribs.len());
            for item in daily_ribs.iter().take(3) {
                println!(
                    "  Daily RIB: {} from {} at {}",
                    item.collector_id,
                    item.ts_start.format("%Y-%m-%d %H:%M:%S"),
                    item.url
                );
            }
        }
        Err(e) => println!("Error getting daily RIBs: {}", e),
    }

    // Example 2: Get recent update files from the last 24 hours
    println!("\n=== Recent Updates Example ===");
    match broker.clone().recent_updates(24) {
        Ok(recent_updates) => {
            println!(
                "Found {} update files from last 24 hours",
                recent_updates.len()
            );
            for item in recent_updates.iter().take(5) {
                println!(
                    "  Update: {} from {} at {}",
                    item.collector_id,
                    item.ts_start.format("%Y-%m-%d %H:%M:%S"),
                    item.url
                );
            }
        }
        Err(e) => println!("Error getting recent updates: {}", e),
    }

    // Example 3: Find the most diverse collectors
    println!("\n=== Most Diverse Collectors Example ===");
    match broker.clone().most_diverse_collectors(5, None) {
        Ok(diverse_collectors) => {
            println!("Top {} most diverse collectors:", diverse_collectors.len());
            for (i, collector) in diverse_collectors.iter().enumerate() {
                println!("  {}. {}", i + 1, collector);
            }

            // Use the diverse collectors to get RIB files
            if !diverse_collectors.is_empty() {
                let collector_list = diverse_collectors.join(",");
                println!("\n--- Using diverse collectors to get RIB files ---");

                match broker
                    .clone()
                    .collector_id(collector_list)
                    .data_type("rib")
                    .page_size(5)
                    .query_single_page()
                {
                    Ok(ribs) => {
                        println!("Found {} RIB files from diverse collectors", ribs.len());
                        for item in ribs {
                            println!(
                                "  RIB: {} from {} at {}",
                                item.collector_id,
                                item.ts_start.format("%Y-%m-%d %H:%M:%S"),
                                item.url
                            );
                        }
                    }
                    Err(e) => println!("Error getting RIBs from diverse collectors: {}", e),
                }
            }
        }
        Err(e) => println!("Error finding diverse collectors: {}", e),
    }

    // Example 4: Compare project-specific diverse collectors
    println!("\n=== Project-Specific Diverse Collectors Example ===");
    
    // RouteViews collectors
    match broker.clone().most_diverse_collectors(3, Some("routeviews")) {
        Ok(rv_collectors) => {
            println!("Top {} RouteViews diverse collectors:", rv_collectors.len());
            for (i, collector) in rv_collectors.iter().enumerate() {
                println!("  {}. {}", i + 1, collector);
            }
        }
        Err(e) => println!("Error finding RouteViews collectors: {}", e),
    }

    // RIPE RIS collectors  
    match broker.clone().most_diverse_collectors(3, Some("riperis")) {
        Ok(ripe_collectors) => {
            println!("Top {} RIPE RIS diverse collectors:", ripe_collectors.len());
            for (i, collector) in ripe_collectors.iter().enumerate() {
                println!("  {}. {}", i + 1, collector);
            }
        }
        Err(e) => println!("Error finding RIPE RIS collectors: {}", e),
    }

    println!("\n=== Shortcuts Example Complete ===");
}
