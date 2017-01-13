#!/usr/bin/env bash

size=$1
itr=$2
p=$3

flink run -m 172.16.0.1:6123 -p $p -c edu.iu.dsc.flink.collectives.Program /N/u/skamburu/projects/flink-apps/target/flink-apps-0.1-jar-with-dependencies.jar -size $size -itr $itr