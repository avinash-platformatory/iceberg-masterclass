ARG CONNECT_VERSION=7.9.7
FROM confluentinc/cp-kafka-connect:${CONNECT_VERSION}

ARG ICEBERG_CONNECTOR_VERSION=1.9.2
RUN confluent-hub install --no-prompt iceberg/iceberg-kafka-connect:${ICEBERG_CONNECTOR_VERSION}
