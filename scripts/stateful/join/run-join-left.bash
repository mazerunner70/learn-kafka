#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd ${DIR}/../..

common/server-startup.bash

common/reset-topics.bash fib unjoined filtered1 filtered2 joined

echo "Running Scala code"
cd ${DIR}/../../..
sbt "runMain demo.kafka.transforms.stateful.join.left.Main"

