#!/usr/bin/env bash

# Create 128 topics this represents 3 magnitudes (base 2) less than what lyft has in production 1024.
# A similar number of consumers would be 8 in contrast to their 64
SIZE="${1:-8}"; shift

ENV_FILE="${1:-}"

p() {
    echo ">>>>>> $1"
}

rm ./.env
if [ ! -z "$ENV_FILE" ]
then
    cat "$ENV_FILE" > .env
fi

p "Deployment test with $SIZE consumers"
p "starting environment $(date)"
docker-compose up -d --scale kafka-consumer=$SIZE --scale kafka-load=0
p 'Add load'
docker-compose up -d --scale kafka-load=1 kafka-load
sleep 180
p 'restarting consumers'
docker-compose restart kafka-consumer
sleep 180
p 'stopping consumers and load'
docker-compose up -d --scale kafka-consumer=0 --scale kafka-load=0 kafka-load kafka-consumer
