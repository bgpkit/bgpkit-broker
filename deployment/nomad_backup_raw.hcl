job "bgpkit-broker-backup" {
  type = "batch"
  periodic {
    cron             = "5 8 * * *"
    prohibit_overlap = true
  }

  task "bgpkit-broker-backup" {
    driver = "raw_exec"

    config {
        command = "/usr/local/bin/bgpkit-broker"
        args    = [
          "backup",
          "--env", "/usr/local/etc/bgpkit.d/broker.env",
          "/var/db/bgpkit/bgpkit_broker.sqlite3",
          "s3://spaces/broker/bgpkit_broker.sqlite3"
        ]
    }

    resources {
      memory = 4000
    }
  }
}
