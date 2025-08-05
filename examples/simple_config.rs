use bgpkit_broker::{BgpkitBroker, BrokerError};

/// This example demonstrates the clean, consistent configuration API.
/// All configuration methods return Result<Self, BrokerError> for consistency.
fn main() {
    println!("=== BGPKIT Broker Simple Configuration Demo ===\n");

    // Example 1: Valid configuration with method chaining
    println!("1. Valid configuration:");
    match configure_broker() {
        Ok(broker) => {
            println!("✓ Configuration successful!");
            println!("  Query params: {}", broker.query_params);

            // Make a real query
            match broker.query() {
                Ok(items) => println!("  ✓ Found {} BGP archive files", items.len()),
                Err(e) => println!("  ✗ Query error: {}", e),
            }
        }
        Err(e) => println!("✗ Configuration error: {}", e),
    }

    // Example 2: Invalid configurations
    println!("\n2. Invalid configurations:");

    let invalid_configs = [
        ("Invalid timestamp", "ts_start"),
        ("Invalid collector", "collector_id"),
        ("Invalid project", "project"),
        ("Invalid data type", "data_type"),
        ("Invalid page", "page"),
        ("Invalid page size", "page_size"),
    ];

    for (description, config_type) in invalid_configs {
        let result = match config_type {
            "ts_start" => BgpkitBroker::new().ts_start("invalid").map(|_| ()),
            "collector_id" => BgpkitBroker::new().collector_id("nonexistent").map(|_| ()),
            "project" => BgpkitBroker::new().project("invalid").map(|_| ()),
            "data_type" => BgpkitBroker::new().data_type("invalid").map(|_| ()),
            "page" => BgpkitBroker::new().page(0).map(|_| ()),
            "page_size" => BgpkitBroker::new().page_size(0).map(|_| ()),
            _ => unreachable!(),
        };

        match result {
            Ok(_) => println!("  {} -> Unexpected success", description),
            Err(e) => println!("  {} -> ✓ Expected error: {}", description, e),
        }
    }

    // Example 3: Different ways to configure
    println!("\n3. Different configuration patterns:");

    // Single method calls
    let _broker = BgpkitBroker::new().ts_start("1634693400").unwrap();
    println!("  ✓ Single method call works");

    // Method chaining
    let _broker = BgpkitBroker::new()
        .ts_start("1634693400")
        .unwrap()
        .ts_end("1634693500")
        .unwrap();
    println!("  ✓ Method chaining works");

    // Error propagation with ?
    fn config_with_propagation() -> Result<BgpkitBroker, BrokerError> {
        BgpkitBroker::new()
            .ts_start("1634693400")?
            .collector_id("rrc00")?
            .project("riperis")?
            .data_type("rib")
    }

    match config_with_propagation() {
        Ok(_) => println!("  ✓ Error propagation with ? works"),
        Err(e) => println!("  ✗ Unexpected error: {}", e),
    }

    println!("\n=== API Benefits ===");
    println!("✓ Clean method names without prefixes/suffixes");
    println!("✓ Consistent Result return types for all config methods");
    println!("✓ Early validation with helpful error messages");
    println!("✓ Perfect method chaining with error handling");
    println!("✓ Single approach - no confusion about which method to use");

    println!("\n=== Demo Complete ===");
}

/// Helper function demonstrating clean configuration
fn configure_broker() -> Result<BgpkitBroker, BrokerError> {
    BgpkitBroker::new()
        .ts_start("2022-01-01T00:00:00Z")?
        .ts_end("2022-01-01T01:00:00Z")?
        .collector_id("route-views2")?
        .project("routeviews")?
        .data_type("rib")?
        .page_size(5)
}
