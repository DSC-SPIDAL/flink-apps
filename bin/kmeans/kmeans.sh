#!/usr/bin/env bash

flink run -m 172.16.0.1:6123 -p $1 -c edu.iu.dsc.flink.kmeans.KMeansOriginal /N/u/skamburu/projects/flink-apps/target/flink-apps-0.1-jar-with-dependencies.jar -points $2 -centroids $3 -output $4 -iterations $5 -k $6
