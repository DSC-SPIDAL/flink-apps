#!/usr/bin/env bash

parallel=16
points=/points
centroids=/centroids
output=/out
iterations=10
k=1000

./kmeans.sh $parallel $points $centroids $output $iterations $k