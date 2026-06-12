#!/bin/sh
# Continuously produces fake customer profile updates (CDC-style stream:
# the same customer_id appears many times, latest record wins).
set -eu

BOOTSTRAP="${BOOTSTRAP_SERVERS:-kafka:9092}"
TOPIC=customers
BIN=/opt/kafka/bin

until "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  echo "waiting for kafka..."; sleep 2
done
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "$TOPIC" --partitions 3 --replication-factor 1 >/dev/null

SCHEMA='{"type":"struct","fields":[
{"field":"customer_id","type":"int32"},
{"field":"name","type":"string"},
{"field":"email","type":"string"},
{"field":"tier","type":"string"},
{"field":"city","type":"string"},
{"field":"updated_ts","type":"int64","name":"org.apache.kafka.connect.data.Timestamp"}]}'
SCHEMA=$(echo "$SCHEMA" | tr -d '\n')

rand() { echo $(( $(od -An -N3 -tu4 /dev/urandom | tr -d ' ') % $1 )); }
pick() { n=$1; shift; i=$(rand "$n"); shift "$i"; echo "$1"; }

echo "producing to topic '$TOPIC'..."
while true; do
  customer_id=$(( $(rand 50) + 1 ))
  first=$(pick 8 ava noah mia liam zoe arjun maya kai)
  last=$(pick 8 smith patel garcia chen kumar brown silva khan)
  name="$first $last"
  email="$first.$last$customer_id@example.com"
  tier=$(pick 3 bronze silver gold)
  city=$(pick 8 bangalore london berlin austin tokyo paris toronto sydney)
  ts=$(( $(date +%s) * 1000 ))

  printf '{"schema":%s,"payload":{"customer_id":%s,"name":"%s","email":"%s","tier":"%s","city":"%s","updated_ts":%s}}\n' \
    "$SCHEMA" "$customer_id" "$name" "$email" "$tier" "$city" "$ts"

  sleep 2
done | "$BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"
