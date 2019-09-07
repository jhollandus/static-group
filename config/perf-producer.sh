#!/bin/bash -e

CONFIG="$(/config/cfg-upd.sh /config/kafka-load.properties)"
kafka-producer-perf-test --producer.config "$CONFIG" "$@"
