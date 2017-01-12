#!/usr/bin/env bash

cp=../../target/flink-apps-0.1-jar-with-dependencies.jar
outbase=/N/u/skamburu/data/kmeans/small_input
k=$2
p=$1
d=$3
outfolder=$outbase/$p/$k

mkdir -p $outfolder

java -cp $cp edu.iu.dsc.flink.kmeans.utils.KMeansDataGenerator -points $p -k $k -d $d -output $outfolder
