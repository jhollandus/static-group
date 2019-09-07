#!/usr/bin/env bash

# Create 128 topics this represents 3 magnitudes (base 2) less than what lyft has in production 1024.
# A similar number of consumers would be 8 in contrast to their 64

stats() {
    local count="$1"; shift
    local interval="$1"; shift
    local iters=0
    local out=$(mktemp)

    echo "Stats file: $out"
    while [ $iters -lt $count ]; do

        #get seconds of last minute
        local startTime=$(date +%s)
        startTime=$((startTime / 60))
        startTime=$((startTime * 60))

        local endTime=$((startTime + 60))

        curl -q "http://localhost:9021/2.0/monitoring/wNF8qDSRQBiT1A34pAOuOw/consumer_groups?startTimeMs=${startTime}000&stopTimeMs=${endTime}000&rollup=ONE_MINUTE&type=MEMBER&group=locations-consumer&memberClientId=locations-consumer" >> "$out"

        iters=$((iters + 1))
        sleep $interval

    done
    echo "Stats file: $out"
}

p() {
    echo ">>>>>> $1"
}

#stats 20 30 &
p "starting environment $(date)"
docker-compose up -d --scale kafka-consumer=8 --scale kafka-load=0
p 'Add load'
docker-compose up -d --scale kafka-load=1 kafka-load
sleep 120
p 'scaling up consumers to 16'
docker-compose up -d --scale kafka-consumer=16 kafka-consumer
sleep 120
p 'scaling down consumers to 8'
docker-compose up -d --scale kafka-consumer=8 kafka-consumer
sleep 120
p 'restarting consumers'
docker-compose restart kafka-consumer
