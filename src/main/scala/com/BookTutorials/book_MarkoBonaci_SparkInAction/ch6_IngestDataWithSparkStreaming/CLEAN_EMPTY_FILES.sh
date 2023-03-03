#!/bin/bash


book_PATH="/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/BookTutorials/book_MarkoBonaci_SparkInAction/ch6_IngestDataWithSparkStreaming"

# GOAL: Removing superfluous non-zero files -- the ._SUCCESS.crc, .part-0000, and .SUCCESS.crc

# rm -rf ./.part*/.*.crc
# rm -rf ./STREAMRESULT*/.*.crc
# rm -rf ./output*/.*.crc

# MORE GENERIC:
rm -rf "${book_PATH}/*/._*.crc"  		# e.g.  ./folder/._SUCCESS.crc
rm -rf "${book_PATH}/*/.S*.crc"		# e.g   ./folder/.SUCCESS.crc
rm -rf "${book_PATH}/*/.part-*.crc" 	# e.g.  ./folder/.part-###.crc
rm -rf "${book_PATH}/*-*/.part-*.crc" 	# e.g.  ./folder-####/.part-###.crc


# Final check
find . -type f -not -empty
