#!/bin/bash


# ORDERSFILE_PATH="/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/BookTutorials/book_MarkoBonaci_SparkInAction/ch6_IngestDataWithSparkStreaming"


ORDERFILE_NAME="orders_SHORT.txt"


if [ -z "$1" ]; then
        echo "Missing output folder name"
        exit 1
fi


# SPlit into 10 lines (orders_short has 30 lines) so 3 files will be generated
split -l 10 --additional-suffix=.csv "${ORDERFILE_NAME}" orders



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
