use bgpkit_broker::BgpkitBroker;

/// Simple example demonstrating health check functionality.
fn main() {
    println!("=== BGPKIT Broker Health Check Demo ===\n");

    // Check default API endpoint health
    let broker = BgpkitBroker::new();

    println!("Checking broker API health at: {}", broker.broker_url);

    match broker.health_check() {
        Ok(()) => {
            println!("✓ Broker API is healthy and responding");

            // Test a simple query to further verify functionality
            println!("\nTesting basic query functionality...");
            match broker.latest() {
                Ok(items) => {
                    println!("✓ Successfully retrieved {} latest files", items.len());

                    // Show first few items as examples
                    for item in items.iter().take(3) {
                        println!(
                            "  - {}: {} ({})",
                            item.collector_id, item.data_type, item.url
                        );
                    }
                }
                Err(e) => println!("✗ Query failed: {}", e),
            }
        }
        Err(e) => {
            println!("✗ Broker API health check failed: {}", e);
            println!("  This might indicate network issues or API downtime");
        }
    }

    // Test with custom endpoint
    println!("\n=== Testing Custom Endpoint ===");
    let custom_broker = BgpkitBroker::new().broker_url("https://invalid-endpoint.example.com/api");

    println!("Checking invalid endpoint: {}", custom_broker.broker_url);
    match custom_broker.health_check() {
        Ok(()) => println!("✓ Unexpected success - endpoint responded"),
        Err(e) => println!("✓ Expected failure: {}", e),
    }

    println!("\n=== Health Check Complete ===");
}
