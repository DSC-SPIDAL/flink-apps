#!/usr/bin/env bash

INPUT_DIR=$1
OUT_DIR=$2
CONF_FILE=/N/u/skamburu/data/flink/experiments/config.properties

points=( 1000 5000 10000 20000 )

for i in "${points[@]}"
do
    mkdir -p $OUT_DIR/$i
    ./damds.sh $INPUT_DIR/$i/dist.bin $INPUT_DIR/$i/weight.bin $OUT_DIR/points $i $OUT_DIR/$i $CONF_FILE $INPUT_DIR/$i/init_points
done
