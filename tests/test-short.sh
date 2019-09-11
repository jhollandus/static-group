#!/usr/bin/env bash

# Create 128 topics this represents 3 magnitudes (base 2) less than what lyft has in production 1024.
# A similar number of consumers would be 8 in contrast to their 64

p() {
    echo ">>>>>> $1"
}

p "starting environment $(date)"
docker-compose -f docker-compose.yml -f docker-compose-static.yml up -d --scale kafka-consumer=2 --scale kafka-load=0
p 'Add load'
docker-compose -f docker-compose.yml -f docker-compose-static.yml up -d --scale kafka-load=1 kafka-load
sleep 30
p 'stopping consumers'
docker-compose -f docker-compose.yml -f docker-compose-static.yml stop kafka-consumer
docker-compose -f docker-compose.yml -f docker-compose-static.yml rm kafka-consumer

