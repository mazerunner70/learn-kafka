#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd ${DIR}/..

common/server-startup.bash

common/reset-topics.bash fib uncounted counted

#echo "Deleting old topics (may fail if broker setup was slow, just rerun"
#bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic fib
#bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic unaggregated
#bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic aggregated
#echo "Creating topics (may fail if broker setup was slow, just rerun"
#bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic fib
#bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic unaggregated
#bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic aggregated

echo "Running Scala code"
cd ${DIR}/../..
sbt "runMain demo.kafka.minimal.Consumer"

