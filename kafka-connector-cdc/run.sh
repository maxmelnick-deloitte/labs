#!/bin/bash

set -ex

sudo service dse stop || true

docker-compose up -d

echo "Waiting for DSE to be available"
while ! docker-compose exec dse cqlsh -e 'describe cluster' ; do
    sleep 5
done
echo "DSE is now available"

docker-compose exec dse cqlsh -f /tmp/create_schema.cql
sleep 2

docker-compose exec dse dse advrep destination create --name demo_destination --transmission-enabled true
sleep 10
docker-compose exec dse dse advrep destination list


docker-compose exec dse dse advrep channel create --data-center-id dc1 --source-keyspace demo_ks --source-table demo_table_udt --destination demo_destination --transmission-enabled true --collection-enabled true
sleep 10
docker-compose exec dse dse advrep channel status
sleep 5

docker-compose exec broker kafka-topics --create --topic demo-topic --zookeeper zookeeper:2181 --partitions 10 --replication-factor 1
sleep 2

curl -X POST -H "Content-Type: application/json" -d @source.json "http://localhost:8083/connectors"
sleep 5
curl -X GET "http://localhost:8083/connectors/dse-source/status" | jq -c -M '[.name,.tasks[].state]' || true


docker-compose exec dse cqlsh -f /tmp/insert_data.cql

sleep 2

# docker-compose exec broker kafka-console-consumer --topic demo-topic --from-beginning --bootstrap-server localhost:9092

