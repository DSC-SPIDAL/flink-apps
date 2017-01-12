#!/usr/bin/env bash

p=$3
centers=$1
tasks_per_node=20
parallel=$(($2 * $tasks_per_node))
echo "Parallel" $parallel
points=hdfs://j-001:9001/kmeans/small/points/$p
echo "POints: " $points
centroids=hdfs://j-001:9001/kmeans/small/centers/$centers
echo "Centroids: " $centroids
#output=hdfs://j-001:9001/kmeans/out
output=/N/u/skamburu/data/kmeans/output/small/${p}_${centers}_${parallel}
echo "Output: " $output
iterations=100
k=$centers
d=2

./kmeans.sh $parallel $points $centroids $output $iterations $k $d 2>&1 | tee ${p}_${centers}_${parallel}_${tasks_per_node}.txt
