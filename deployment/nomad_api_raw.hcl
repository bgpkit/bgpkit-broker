job "bgpkit-broker-api" {
    type = "service"
    task "api" {
        driver = "raw_exec"
        config {
            command = "/usr/local/bin/bgpkit-broker"
            args    = [
                "serve",
                "--port", "40065",
                "/var/db/bgpkit/bgpkit_broker.sqlite3"
            ]
        }
        resources {
            memory = 4000
        }
    }
}
