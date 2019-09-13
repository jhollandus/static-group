#!/usr/bin/env bash

# Create 128 topics this represents 3 magnitudes (base 2) less than what lyft has in production 1024.
# A similar number of consumers would be 8 in contrast to their 64
COMPOSE_FILE="$1"
GROUP="$2"

p() {
    echo "$(date) >>>>>> $1"
}

dc() {
    docker-compose -f docker-compose.yml -f $COMPOSE_FILE $@
}

wait_for_lag() {
    local tstart=$(date +%s)

    p "Wait for lag to go to zero or 5 minutes max, whichever happens first"
    echo ''

    while true; do
        local lag="$(kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group $GROUP 2> /dev/null | tail +3 | awk '{s+=$6} END {print s}')"

        local tnow=$(date +%s)
        local dtime=$((tnow - tstart))
        if [ $dtime -gt 300 ]
        then
            p "Wait time of 3 minutes has been exceeded with a lag of $lag"
            break
        fi

        if [ "$lag" == "0" ]
        then
            p "Zero lag achieved"
            break
        else
            echo -e "\r\033[0KLag: $lag"
            sleep 5
        fi
    done
}
export STAS_CONFIG_VERSION=$(date +%s)
export STAS_GROUP="$GROUP"
export STAS_MAX_GROUP_SIZE=8
export STAS_MORE__MAX_MESSAGES=1000
export STAS_MORE__MAX_POLL_TIME=5

p "starting environment"
dc up -d --scale kafka-consumer=0 --scale kafka-load=0
sleep 30

p "bring in consumers"
dc up -d --scale kafka-consumer=8 --scale kafka-load=0

p "Add load for 5 minutes"
dc up -d --scale kafka-load=2 kafka-load

p "Allow consumers to consume for 2 minutes"
sleep 120

p "restarting consumers"
dc restart kafka-consumer

wait_for_lag "$STAS_GROUP"

p "opening control center for review"
open 'http://localhost:9021'

read -p "Press enter to continue and shutdown the environment"

p "stopping environment"
dc down

for v in $(env | grep '^STAS_')
do
    unset "$(echo $v | cut -d '=' -f1)"
done

p "complete"
