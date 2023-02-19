#!/bin/bash


# GOAL: Removing superfluous non-zero files -- the ._SUCCESS.crc, .part-0000, and .SUCCESS.crc


rm -rf ./STREAMRESULT*/.*.crc
rm -rf ./output*/.*.crc

# Final check
find . -type f -not -empty
