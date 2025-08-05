use bgpkit_broker::BgpkitBroker;

/// This example demonstrates flexible timestamp parsing capabilities.
/// The SDK accepts various timestamp formats for user convenience.
fn main() {
    println!("=== BGPKIT Broker Timestamp Formats Demo ===\n");

    // Common timestamp formats supported
    let examples = [
        ("Unix timestamp", "1640995200"),
        ("RFC3339 date", "2022-01-01T00:00:00Z"),
        ("Simple date", "2022-01-01"),
        ("Date with slashes", "2022/01/01"),
        ("Compact date", "20220101"),
    ];

    println!("Supported timestamp formats:");
    for (description, timestamp) in examples {
        println!("  ✓ {}: '{}'", description, timestamp);
    }

    // Demonstrate practical usage
    println!("\n=== Practical Example ===");

    // Query using simple date format
    let broker = BgpkitBroker::new()
        .ts_start("2022-01-01")
        .ts_end("2022-01-02")
        .collector_id("route-views2")
        .data_type("rib")
        .page_size(5);

    match broker.query() {
        Ok(items) => {
            println!(
                "✓ Found {} BGP archive files using simple date format",
                items.len()
            );
            for item in items.iter().take(2) {
                println!("  - {} at {}", item.data_type, item.ts_start);
            }
        }
        Err(e) => println!("✗ Query failed: {}", e),
    }

    // Show error handling for invalid format
    println!("\n=== Error Handling ===");
    let invalid_broker = BgpkitBroker::new().ts_start("invalid-date").page_size(1);

    match invalid_broker.query() {
        Ok(_) => println!("✗ Unexpected success with invalid date"),
        Err(_) => println!("✓ Proper error handling: validation occurs at query time"),
    }

    println!("\n=== Demo Complete ===");
}
