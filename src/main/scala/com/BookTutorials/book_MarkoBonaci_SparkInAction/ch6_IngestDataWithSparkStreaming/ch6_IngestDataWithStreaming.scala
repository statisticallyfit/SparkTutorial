package com.BookTutorials.book_MarkoBonaci_SparkInAction.ch6_IngestDataWithSparkStreaming

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream
/**
 *
 */
object ch6_IngestDataWithStreaming extends App {

	/**
	 * 6.1.2
	 *
	 * Create StreamingContext
	 *
	 */

	// This part is done automatically if in the repl
	val sparkSession: SparkSession = SparkSession.builder()
		.master("local[1]")
		.appName("IngestDataWithStreaming")
		.getOrCreate();
	// REPL
	// val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("IngestDataWithStreaming").getOrCreate();

	val sparkStreamingContext: StreamingContext = new StreamingContext(sparkContext = sparkSession.sparkContext,
		batchDuration = Seconds(5))
	// REPL
	// val sparkStreamingContext: StreamingContext = new StreamingContext(sparkContext = sparkSession.sparkContext,	batchDuration = Seconds(5))

	// Alternate way: SparkStreaming can start a new SparkContext if given a spark configuration object instead:
	val conf: SparkConf = new SparkConf()
		.setMaster("local[4]")
		.setAppName("App name")
	// REPL
	// val conf = new SparkConf().setMaster("local[4]").setAppName("App name")

	// val sparkStreamingContextViaConfig: StreamingContext = new StreamingContext(conf = conf, batchDuration =Seconds(5))
	// NOTE - comment out since error otherwise: org.apache.spark.SparkException: Only one SparkContext should be  running in this JVM (see SPARK-2243).


	// -------------------

	/**
	 * 6.1.3
	 *
	 * Create discretized stream
	 *
	 * Goal: stream data from a file
	 * Download the data to be streamed: file has 500,000 lines representing buy and sell orders.
	 * Randomly generated
	 * >Each line contains the comma separated elements:
	 * 	- Order timestamp - yyy-mm-dd hh:MM:ss
	 * 	- Order ID - serially incrementing integer
	 *  	- client ID - integer randomly picked from 1 to 100
	 *  	- Stock symbol - randomly picked from list of 80 stock symbols
	 *  	- Number of stocks to be bought or sold - random number from 1 to 1000
	 *  	- Price at which to buy or sell - random number from  1 to 100
	 *  	- character B or S - whether the even tis an order to buy or sell
	 */

	// ---> Download data to be streamed
	// [DONE] by opening tar.gz. file



	// ---> Create DStream object
	// NOTE - DStream = discretized stream = sequence of RDDS created periodically from the input stream. Lazily
	// evaluated like RDDs so when you create a DStream object, nothing happens yet, the RDDS "come in" only after
	// you start the streaming context.
	// NOTE> - Must choose a folder where the splits will be copied to and from where your streaming application will
	//  read them
	// NOTE - `textFileStream` method - to stream incoming textual data directly from files, using `StreamingContext`
	val filestream: DStream[String] = sparkStreamingContext.textFileStream(directory =
		"/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/BookTutorials/book_MarkoBonaci_SparkInAction/ch6_IngestDataWithSparkStreaming")


	/**
	 * 6.1.4
	 *
	 * Using Discretized Streams
	 *
	 * Must now use the DStream object to calculate the number of selling and buying orders per second
	 */

	// ---> Parsing the lines
	// NOTE - must transform each line in the file to something more manageable, like scala case class:

	/**
	 * Order class to hold data on buy/sell orders
	 */
	import java.sql.Timestamp

	/**
	 *
	 * @param time
	 * @param orderID
	 * @param clientID
	 * @param symbol
	 * @param amount
	 * @param price
	 * @param buyOrSell = true BUY, false if SELL
	 */
	case class Order(time: Timestamp, orderID: Long, clientID: Long, symbol: String, amount: Int, price: Double,
				  buyOrSell: Boolean)

	// Parsing lines from the filestream DStream to obtain a new DStream containing Order objects
	// Using: `flatMap` transformation to operate on all elements of all RDDS in a DStream (flatMap not map because
	// want to ignore lines that don't match the format we expect; if th eline can be parsed, the function returns a
	// list with a single element, else an empty list)

	import java.text.SimpleDateFormat


	// NOTE: using the flatMap way
	//  flatMap(String => TraversableOnce[U]) ---> flatMap(String => List[U])
	val ordersByFlatMap: DStream[Order] = filestream.flatMap((line: String) => {
		// Parse the time stamps
		val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyy-MM-dd hh:mm:ss")

		// Split each line by commas
		val lineSplit: Array[String] = line.split(",")

		try {
			// Checking seventh field is either B = buy or S = sell
			assert(lineSplit(6) == "B" || lineSplit(6) == "S", "Check: the seventh field should be either 'B' (buy) or 'S' (sell)")

			val tryMakeOrderObject: Order = Order(
				time = new Timestamp(dateFormat.parse(lineSplit(0)).getTime()),
				orderID = lineSplit(1).toLong,
				clientID = lineSplit(2).toLong,
				symbol = lineSplit(3),
				amount = lineSplit(4).toInt,
				price = lineSplit(5).toDouble,
				buyOrSell = lineSplit(6) == "B"
			)

			List(tryMakeOrderObject)
		} catch {
			// If anything goes wrong during parsing, error is logged
			case err: Throwable => println("Wrong line format ('+e+'): " + line)

			// Return empty list
			List()
		}
	})



	// NOTE: using the map way:
	//  map(String => U)
	val dstreamOrdersWrappedInOption: DStream[Option[Order]] = filestream.map((line: String) => {
		// Parse the time stamps
		val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyy-MM-dd hh:mm:ss")

		// Split each line by commas
		val lineSplit: Array[String] = line.split(",")

		try {
			// Checking seventh field is either B = buy or S = sell
			assert(lineSplit(6) == "B" || lineSplit(6) == "S", "Check: the seventh field should be either 'B' (buy) or 'S' (sell)")

			val tryMakeOrderObject: Order = Order(
				time = new Timestamp(dateFormat.parse(lineSplit(0)).getTime()),
				orderID = lineSplit(1).toLong,
				clientID = lineSplit(2).toLong,
				symbol = lineSplit(3),
				amount = lineSplit(4).toInt,
				price = lineSplit(5).toDouble,
				buyOrSell = lineSplit(6) == "B"
			)

			//List(tryMakeOrderObject)
			Some(tryMakeOrderObject)
		} catch {
			// If anything goes wrong during parsing, error is logged
			case err: Throwable => println("Wrong line format ('+e+'): " + line)

				None
		}
	})
	val ordersByMap: DStream[Order] = dstreamOrdersWrappedInOption.filter(_ != None).map(_.get)


	// Checking both ways (flatMap vs. map way) yielded the same results
	// assert(ordersByFlatMap.t)
	// TODO - how to compare elements within each DStream?

	// TODO 2 - how to convert dstream to a list??? to see what is inside?





	// ---> TASK 1: Counting the numbers of buy and sell orders
}
