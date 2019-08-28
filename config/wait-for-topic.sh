#!/bin/bash -e
ZK="$1"; shift
TOPIC="$1"; shift

while ! kafka-topics --zookeeper "$ZK" --describe --topic "$TOPIC"
do
    sleep 5
done