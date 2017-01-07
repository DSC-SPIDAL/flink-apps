#!/usr/bin/env bash

cp=../../target/flink-apps-0.1-jar-with-dependencies.jar
outbase=
k=$1
p=$2
outfolder=$outbase/$p_$k

mkdir -p $outfolder

java -cp $cp edu.iu.dsc.flink.kmeans.utils.KMeansDataGenerator -points $p -k $k -output $outfolder