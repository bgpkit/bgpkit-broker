job "bgpkit-broker-api" {

    type = "service"

    group "bsd" {

        task "api" {
          driver = "raw_exec"

          config {
            command = "/usr/local/bin/bgpkit-broker"
            args    = ["serve"]
          }

          resources {
            memory = 4000
          }
        }
    }
}
