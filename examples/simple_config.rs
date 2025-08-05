use bgpkit_broker::BgpkitBroker;

/// This example demonstrates the simple configuration API.
/// Configuration methods return Self for easy chaining, with validation occurring at query time.
fn main() {
    println!("=== BGPKIT Broker Simple Configuration Demo ===\n");

    // Example 1: Valid configuration with method chaining
    println!("1. Valid configuration:");
    let broker = configure_broker();
    println!("✓ Configuration successful!");
    println!("  Query params: {}", broker.query_params);

    // Make a real query to trigger validation
    match broker.query() {
        Ok(items) => println!("  ✓ Found {} BGP archive files", items.len()),
        Err(e) => println!("  ✗ Query error: {}", e),
    }

    // Example 2: Invalid configurations (errors occur at query time)
    println!("\n2. Invalid configurations (errors detected at query time):");

    let test_cases = [
        ("Invalid timestamp", "invalid-timestamp"),
        ("Invalid collector", "nonexistent-collector"),
        ("Invalid date", "2022-13-01"),
    ];

    for (description, invalid_value) in test_cases {
        let broker = BgpkitBroker::new()
            .ts_start(invalid_value)
            .page(1)
            .page_size(10);
        
        match broker.query() {
            Ok(_) => println!("  {} -> Unexpected success", description),
            Err(e) => println!("  {} -> ✓ Expected error: {}", description, e),
        }
    }

    // Example 3: Different ways to configure
    println!("\n3. Configuration patterns:");

    // Single method calls
    let _broker = BgpkitBroker::new().ts_start("1634693400");
    println!("  ✓ Single method call works");

    // Method chaining
    let _broker = BgpkitBroker::new()
        .ts_start("1634693400")
        .ts_end("1634693500")
        .collector_id("rrc00")
        .page_size(50);
    println!("  ✓ Method chaining works");

    println!("\n=== Current API Benefits ===");
    println!("✓ Clean method names without prefixes/suffixes");
    println!("✓ Simple method chaining without Result handling");
    println!("✓ Validation occurs at query time with helpful error messages");
    println!("✓ Flexible timestamp formats accepted");
    println!("✓ Configuration is always successful, errors only on invalid queries");

    println!("\n=== Demo Complete ===");
}

/// Helper function demonstrating clean configuration
fn configure_broker() -> BgpkitBroker {
    BgpkitBroker::new()
        .ts_start("2022-01-01T00:00:00Z")
        .ts_end("2022-01-01T01:00:00Z")
        .collector_id("route-views2")
        .project("routeviews")
        .data_type("rib")
        .page_size(5)
}
