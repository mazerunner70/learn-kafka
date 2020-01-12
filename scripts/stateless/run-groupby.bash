#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

mkdir -p $DIR/logs


pushd $KAFKA_HOME

echo "Resetting application state"
rm -r /tmp/kafka-streams/my-first-streams-application

echo "Starting Zookeeper"
(bin/zookeeper-server-start.sh config/zookeeper.properties) 2>&1 >${DIR}/logs/zoo.log &
sleep 10



echo "Starting Broker"
(bin/kafka-server-start.sh config/server.properties) 2>&1 >${DIR}/logs/broker.log &
sleep 10

bin/kafka-streams-application-reset.sh --application-id "my-first-streams-application"

echo " Deleting all consumer groups"
bin/kafka-consumer-groups.sh --delete --all-groups --bootstrap-server localhost:9092
echo "Deleting old topics (may fail if broker setup was slow, just rerun"
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic fib
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic uncounted
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic aggregated
echo "Creating topics (may fail if broker setup was slow, just rerun"
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic fib
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic uncounted
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic aggregated
popd

echo "Running Scala code"
cd ${DIR}/../..
sbt "runMain demo.kafka.transforms.stateless.groupby.Main"

