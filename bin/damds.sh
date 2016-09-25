#!/usr/bin/env bash

./flink run flink-apps-0.1.jar -dFile $1 -wFile $2 -pFile $3 -points $4 -outFolder $5