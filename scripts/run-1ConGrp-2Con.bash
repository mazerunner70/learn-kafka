#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

mkdir -p $DIR/logs


pushd $KAFKA_HOME

echo "Starting Zookeeper"
(bin/zookeeper-server-start.sh config/zookeeper.properties) 2>&1 >${DIR}/logs/zoo.log &
sleep 10

echo "Starting Broker"
(bin/kafka-server-start.sh config/server.properties) 2>&1 >${DIR}/logs/broker.log &
sleep 10

echo "Creating topic (may fail if broker setup was slow, just rerun"
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic test-1
popd

echo "Running Scala code"
cd ${DIR}/..
sbt "runMain demo.kafka.scalad.Main"

