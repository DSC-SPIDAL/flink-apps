#!/usr/bin/env bash

INPUT_DIR=$1
OUT_DIR=$2

points=( 1000 5000 10000 20000 )

mkdir -p $OUT_DIR
for i in "${points[@]}"
do
	./damds.sh $INPUT_DIR/$i/dist.bin $INPUT_DIR/$i/weight.bin $INPUT_DIR/$i/init_points $i $OUT_DIR
done