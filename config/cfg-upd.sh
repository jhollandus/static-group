#!/bin/bash -e
CFG_FILE="$1"; shift

CFG="$(mktemp)"
cp "$CFG_FILE" "$CFG"
echo "${KAFKA_PROPS:-}" | tr , '\n' >> "$CFG"

echo "$CFG"