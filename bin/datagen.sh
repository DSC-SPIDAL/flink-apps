#!/usr/bin/env bash

java -cp ../target/flink-apps-0.1.jar edu.iu.dsc.flink.mm.MatrixFileGenerator -n $1 -m $2 -f $3 -t $4