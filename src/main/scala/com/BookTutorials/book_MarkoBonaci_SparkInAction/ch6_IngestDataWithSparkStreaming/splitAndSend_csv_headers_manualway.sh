#!/bin/bash


ORDERFILE_NAME="orders.txt"


if [ -z "$1" ]; then
        echo "Missing output folder name"
        exit 1
fi

split -l 10000 --additional-suffix=.csv "${ORDERFILE_NAME}" orders



for f in `ls *.csv`; do

	# Adding headers for each column, to the start of the generated file
	sed -i '1i Timestamp,OrderID,ClientID,StockSymbol,NumStocks,Price,BuyOrSell' $f
	
        if [ "$2" == "local" ]; then
                mv $f $1
        else
                hdfs dfs -copyFromLocal $f $1
                rm -f $f
        fi
        sleep 3
done
