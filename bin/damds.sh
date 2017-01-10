#!/usr/bin/env bash

flink run /N/u/skamburu/projects/flink-apps/target/flink-apps-0.1.jar -c $6 -dFile $1 -wFile $2 -pFile $3 -points $4 -outFolder $5 -initPFile $7