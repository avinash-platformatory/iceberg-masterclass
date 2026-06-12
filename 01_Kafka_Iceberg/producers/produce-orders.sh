#!/bin/sh
# Continuously produces fake order events (Connect JSON envelope with schema).
set -eu

BOOTSTRAP="${BOOTSTRAP_SERVERS:-kafka:9092}"
TOPIC=orders
BIN=/opt/kafka/bin

until "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  echo "waiting for kafka..."; sleep 2
done
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "$TOPIC" --partitions 3 --replication-factor 1 >/dev/null

# Schema lets the sink auto-create a typed Iceberg table (order_ts -> timestamp)
SCHEMA='{"type":"struct","fields":[
{"field":"order_id","type":"string"},
{"field":"customer_id","type":"int32"},
{"field":"product","type":"string"},
{"field":"quantity","type":"int32"},
{"field":"price","type":"double"},
{"field":"currency","type":"string"},
{"field":"status","type":"string"},
{"field":"order_ts","type":"int64","name":"org.apache.kafka.connect.data.Timestamp"}]}'
SCHEMA=$(echo "$SCHEMA" | tr -d '\n')

rand() { echo $(( $(od -An -N3 -tu4 /dev/urandom | tr -d ' ') % $1 )); }
pick() { n=$1; shift; i=$(rand "$n"); shift "$i"; echo "$1"; }

echo "producing to topic '$TOPIC'..."
while true; do
  order_id="ord-$(printf '%05d' "$(rand 100000)")"
  customer_id=$(( $(rand 50) + 1 ))
  product=$(pick 8 laptop phone headphones monitor keyboard mouse webcam dock)
  quantity=$(( $(rand 4) + 1 ))
  price="$(( $(rand 900) + 10 )).$(printf '%02d' "$(rand 100)")"
  currency=$(pick 4 USD EUR INR GBP)
  status=$(pick 3 created paid shipped)
  ts=$(( $(date +%s) * 1000 ))

  printf '{"schema":%s,"payload":{"order_id":"%s","customer_id":%s,"product":"%s","quantity":%s,"price":%s,"currency":"%s","status":"%s","order_ts":%s}}\n' \
    "$SCHEMA" "$order_id" "$customer_id" "$product" "$quantity" "$price" "$currency" "$status" "$ts"

  sleep 0.5
done | "$BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"
