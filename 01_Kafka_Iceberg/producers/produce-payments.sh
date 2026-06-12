#!/bin/sh
# Continuously produces fake payment events. order_id values share the
# same id space as the orders topic, so joins return matches.
set -eu

BOOTSTRAP="${BOOTSTRAP_SERVERS:-kafka:9092}"
TOPIC=payments
BIN=/opt/kafka/bin

until "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  echo "waiting for kafka..."; sleep 2
done
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "$TOPIC" --partitions 3 --replication-factor 1 >/dev/null

SCHEMA='{"type":"struct","fields":[
{"field":"payment_id","type":"string"},
{"field":"order_id","type":"string"},
{"field":"method","type":"string"},
{"field":"amount","type":"double"},
{"field":"status","type":"string"},
{"field":"payment_ts","type":"int64","name":"org.apache.kafka.connect.data.Timestamp"}]}'
SCHEMA=$(echo "$SCHEMA" | tr -d '\n')

rand() { echo $(( $(od -An -N3 -tu4 /dev/urandom | tr -d ' ') % $1 )); }
pick() { n=$1; shift; i=$(rand "$n"); shift "$i"; echo "$1"; }

echo "producing to topic '$TOPIC'..."
while true; do
  payment_id="pay-$(printf '%06d' "$(rand 1000000)")"
  order_id="ord-$(printf '%05d' "$(rand 100000)")"
  method=$(pick 4 card upi paypal bank_transfer)
  amount="$(( $(rand 900) + 10 )).$(printf '%02d' "$(rand 100)")"
  status=$(pick 3 authorized captured failed)
  ts=$(( $(date +%s) * 1000 ))

  printf '{"schema":%s,"payload":{"payment_id":"%s","order_id":"%s","method":"%s","amount":%s,"status":"%s","payment_ts":%s}}\n' \
    "$SCHEMA" "$payment_id" "$order_id" "$method" "$amount" "$status" "$ts"

  sleep 0.7
done | "$BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"
