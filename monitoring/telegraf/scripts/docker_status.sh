#!/bin/sh
# Generate Docker container status in InfluxDB line protocol
# Called by Telegraf inputs.exec — uses Docker Engine API via Unix socket
curl -s --unix-socket /var/run/docker.sock http://localhost/containers/json?all=true | \
  jq -r '.[] | "docker_container_status,container_name=" + (.Names[0] | ltrimstr("/")) + ",status=" + .State + " value=1"'
