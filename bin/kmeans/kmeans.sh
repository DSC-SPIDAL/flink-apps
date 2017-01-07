#!/usr/bin/env bash

flink run -p $1 -points $2 -centroids $3 -output $4 -iterations $5 -k $6