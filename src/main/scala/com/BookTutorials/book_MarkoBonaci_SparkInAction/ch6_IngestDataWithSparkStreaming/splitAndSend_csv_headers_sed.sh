#!/bin/bash

if [ -z "$1" ]; then
        echo "Missing output folder name"
        exit 1
fi

split -l 10000 --additional-suffix=.csv orders.txt orders

#! TODO - must add titles to each column, how?
sed  -i '1i A,B,C,D,E,F,G' orders.txt



for f in `ls *.csv`; do
        if [ "$2" == "local" ]; then
                mv $f $1
        else
                hdfs dfs -copyFromLocal $f $1
                rm -f $f
        fi
        sleep 3
done
