#!/bin/bash

echo "Shutting down Zookeper"

pushd $KAFKA_HOME

echo "Stopping Broker"
bin/kafka-server-stop.sh

echo "Stopping Zookeeper"
bin/zookeeper-server-stop.sh



