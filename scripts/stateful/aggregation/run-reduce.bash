#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd ${DIR}/../..

common/server-startup.bash

common/reset-topics.bash fib unreduced reduced

echo "Running Scala code"
cd ${DIR}/../../..
sbt "runMain demo.kafka.transforms.stateful.aggregation.reduce.Main"

