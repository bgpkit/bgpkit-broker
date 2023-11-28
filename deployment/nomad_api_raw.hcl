job "bgpkit-broker-api" {
  type = "service"
  group "broker" {
    task "api" {
      driver = "raw_exec"

      config {
        command = "/usr/local/bin/bgpkit-broker"
        args    = [
          "serve",
          "--port", "40064",
          "/var/db/bgpkit/bgpkit_broker.sqlite3"
        ]
      }

      resources {
        memory = 4000
      }
    }
  }
}
