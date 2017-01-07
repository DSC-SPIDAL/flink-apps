#!/usr/bin/env bash

OUT_DIR=$1

points=( 1000 5000 10000 20000 )

for i in "${points[@]}"
do
    mkdir -p $OUT_DIR/$i
    sh datagen.sh $i $i $OUT_DIR/$i/dist.bin m
    sh datagen.sh $i $i $OUT_DIR/$i/weight.bin m
    sh datagen.sh $i 3 $OUT_DIR/$i/init_points p
done