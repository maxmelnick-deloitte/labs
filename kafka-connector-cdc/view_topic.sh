#!/bin/bash

set -ex

TOPIC=demo-topic

if [ -z "$1" ]
  then
    echo "No topic supplied, using default of $TOPIC"
else
    TOPIC=$1
    echo "Topic set to $TOPIC"
fi

docker run --tty \
  --network kafka-connector-cdc_default \
  edenhill/kafkacat:1.5.0 \
  kafkacat -b broker:29092 -C -K: \
    -f '\nKey (%K bytes): %k\t\nValue (%S bytes): %s\n\Partition: %p\tOffset: %o\n--\n' \
    -t ${TOPIC}