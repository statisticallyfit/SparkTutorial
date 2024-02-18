** NOTE: tati's idea


## MOTIVATION:
- Mimic the streaming nature of trading candles in the stock market. 
- Mimic how the candles have open-high-low-close (open = first of the list of incoming prices, low = lowest price that landed within the candle window, high = highest price that landed in the candle window, close = last of the list of incoming prices) 
- Count how many orders land within the window of a particular candle. 
- Sort the orders that come out of the window and assert they come in order (that processing time model was faithful?) 
- Show understanding of processing vs. event time.

## Application Features:
- Candle type
- Candle has height, window (start,end time), and number of orders
- streaming app has start/end window
- streaming feature should be modeled by processing / event time. 
  - NOTE: borrow from the streaming count example. https://github.com/statisticallyfit/SparkTutorial/blob/787cae15d5702e1e927baa0f68f6196805fb76dd/src/test/scala/com/sparkstreaming/OnlineTutorials/EXPLORE_StreamingWindowCount_VIA_GROUPBY.scala
