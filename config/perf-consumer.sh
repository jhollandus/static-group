#!/bin/bash -e

CONFIG="$(/config/cfg-upd.sh /config/kafka-consumer.properties)"
kafka-consumer-perf-test --consumer.config "$CONFIG" "$@"
