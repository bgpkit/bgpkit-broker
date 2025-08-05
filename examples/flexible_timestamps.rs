use bgpkit_broker::{BgpkitBroker, BrokerError};

/// This example demonstrates the flexible timestamp parsing capabilities.
/// The parse_timestamp function now returns DateTime<Utc> and formats consistently.
fn main() {
    println!("=== BGPKIT Broker Flexible Timestamp Parsing Demo ===\n");

    // Test different timestamp formats
    let timestamp_formats = vec![
        ("Unix timestamp", "1640995200"),
        ("RFC3339 with Z", "2022-01-01T00:00:00Z"),
        ("RFC3339 without Z", "2022-01-01T00:00:00"),
        ("Date with time", "2022-01-01 00:00:00"),
        ("Pure date (dash)", "2022-01-01"),
        ("Pure date (slash)", "2022/01/01"),
        ("Pure date (dot)", "2022.01.01"),
        ("Compact date", "20220101"),
        ("Whitespace trimmed", "  2022-01-01  "),
    ];

    println!("Testing valid timestamp formats (all normalized to RFC3339 with Z):");
    for (name, timestamp) in &timestamp_formats {
        match BgpkitBroker::new().ts_start(timestamp) {
            Ok(broker) => {
                println!(
                    "✓ {}: '{}' -> '{}'",
                    name,
                    timestamp,
                    broker.query_params.ts_start.unwrap_or_default()
                );
            }
            Err(e) => {
                println!("✗ {}: '{}' -> Error: {}", name, timestamp, e);
            }
        }
    }

    // Test invalid formats
    println!("\nTesting invalid timestamp formats:");
    let invalid_formats = vec![
        ("Invalid text", "invalid-timestamp"),
        ("Invalid date", "2022-13-01"),
        ("Partial date", "2022-01"),
        ("Invalid compact", "20221301"),
        ("Empty string", ""),
    ];

    for (name, timestamp) in &invalid_formats {
        match BgpkitBroker::new().ts_start(timestamp) {
            Ok(_) => {
                println!("✗ {}: '{}' -> Unexpected success", name, timestamp);
            }
            Err(e) => {
                println!("✓ {}: '{}' -> Expected error: {}", name, timestamp, e);
            }
        }
    }

    // Demonstrate practical usage with date ranges
    println!("\n=== Practical Usage Examples ===");

    // Example 1: Query using pure dates (much more user-friendly)
    println!("\n1. Query using pure dates:");
    match query_with_dates("2022-01-01", "2022-01-02") {
        Ok(count) => println!("   ✓ Found {} BGP archive files between dates", count),
        Err(e) => println!("   ✗ Query failed: {}", e),
    }

    // Example 2: Mixed timestamp formats
    println!("\n2. Mixed timestamp formats:");
    match query_mixed_formats() {
        Ok(count) => println!("   ✓ Found {} BGP archive files with mixed formats", count),
        Err(e) => println!("   ✗ Query failed: {}", e),
    }

    println!("\n=== Benefits of DateTime<Utc> Refactoring ===");
    println!("✓ Consistent formatting: All timestamps normalized to RFC3339 with Z");
    println!("✓ Type safety: Internal DateTime<Utc> prevents string formatting errors");
    println!("✓ Better validation: Proper date/time parsing with error handling");
    println!("✓ User-friendly: Accept dates in common formats");
    println!("✓ Automatic normalization: Pure dates become start-of-day timestamps");
    println!("✓ Backward compatible: Unix timestamps and RFC3339 still work");

    println!("\n=== Demo Complete ===");
}

/// Helper function to query with pure date strings
fn query_with_dates(start_date: &str, end_date: &str) -> Result<usize, BrokerError> {
    let broker = BgpkitBroker::new()
        .ts_start(start_date)?
        .ts_end(end_date)?
        .collector_id("route-views2")?
        .data_type("rib")?
        .page_size(5)?; // Limit results for demo

    let items = broker.query()?;
    Ok(items.len())
}

/// Helper function demonstrating mixed timestamp formats
fn query_mixed_formats() -> Result<usize, BrokerError> {
    let broker = BgpkitBroker::new()
        .ts_start("20220101")? // Compact format
        .ts_end("2022-01-02T00:00:00Z")? // RFC3339 format
        .page_size(3)?; // Limit results for demo

    let items = broker.query()?;
    Ok(items.len())
}
