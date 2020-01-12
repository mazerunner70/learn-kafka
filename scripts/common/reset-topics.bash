#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


cd $KAFKA_HOME


for arg in "$@"
do
  echo "  Deleting old topic '$arg'"
  bin/kafka-topics.sh --if-exists --delete --zookeeper localhost:2181 --topic $arg
  echo "  Recreating topic '$arg'"
  bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic $arg

done
