package com.BookTutorials.book_MarkoBonaci_SparkInAction.ch6_IngestDataWithSparkStreaming

import org.apache.spark._
import org.apache.spark.rdd.RDD
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

	// convenient abbreviations:
	val ssc = sparkStreamingContext
	val sc = sparkSession.sparkContext

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
	//
	val PATH = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/BookTutorials/book_MarkoBonaci_SparkInAction/ch6_IngestDataWithSparkStreaming/"
	val inputDir: String = "inputDir/"
	val outputDir: String = "outputDir/"
	val filestream: DStream[String] = sparkStreamingContext.textFileStream(directory = PATH + inputDir)

	/**
	 * KEY ABOUT STREAMING THE DATA MANUALLY HERE:
	 *
	 * (1) SPLIT DATA
	 *  Unrealistic to say that all 500,000 events will arrive to our system all at once
	 *
	 * So have prepared linux shell script named `splitAndSend.sh` to split the data in a a streaming-kind-of-way:
	 * FUNCTIONS OF `splitAndSend.sh`:
	 * 	- splits the unzipped file (orders.txt) into 50 files, each containing 10,000 lines.
	 * 	- periodically moves the splits to an HDFS directory (or local dir) (supplied as argument), waiting for 3
	 * 	seconds after copying each split.
	 * 	- this simulates streaming data in a real environment.
	 *
	 */


	/**
	 * (2) STREAMING STARTING TRIGGER
	 *
	 * `textFileStream` reads each **newly created** file in the directory
	 * NOTE: newly created => means `textFileStream`
	 * 	1) doesn't process the files already in the folder when the streaming context starts,
	 * 	2) nor does it react to data that is added to a file,
	 * 	3) will only process the files copied to the folder AFTER processing starts
	 * TODO - does 'processing start' mean executing the splitAndSend.sh script or executing sss.start() ?
	 *
	 */

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
	 * @param isBuyOrSell = true BUY, false if SELL
	 */
	case class Order(time: Timestamp, orderID: Long, clientID: Long, symbol: String, amount: Int, price: Double,
				  isBuyOrSell: Boolean)

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
				isBuyOrSell = lineSplit(6) == "B"
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
				isBuyOrSell = lineSplit(6) == "B"
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
	// TODO - use forech println / use slice() time interval methods





	// ---> TASK 1: Counting the numbers of buy and sell orders

	// NOTE: DStreams containing two-element tuples get implicitly converted to `PairDStreamFunctions` objects
	//  (similar to RDDS converting to `PairRDDFunctions` if they contain two-element tuples)

	val orders: DStream[Order] = ordersByFlatMap // simpler name

	val numPerType: DStream[(Boolean, Long)] = orders
		.map((order: Order) => (order.isBuyOrSell, 1L))
		.reduceByKey((c1: Long, c2: Long) => c1 + c2) //TODO understand better via repl

	// REPL
	// val numPerType: DStream[(Boolean, Long)] = orders.map((order: Order) => (order.isBuyOrSell, 1L)).reduceByKey((c1: Long, c2: Long) => c1 + c2) //TODO understand better via repl


	/**
	 * 6.1.5
	 *
	 * Saving Results to a File
	 *
	 * `saveAsTextFiles` - given string prefix arg and optional string suffix arg, uses them to construct path at
	 * which data should be periodically saved.
	 *
	 * Each mini-batch RDD is saved to a folder called `<prefix><time-in-millilseconds>.<suffix>` or just `<suffix>
	 *      .<time-in-millilseconds>`
	 * MEANING:
	 * 	---> every 5 seconds a new directory is created
	 * 	---> each of these directories contains one file, named `part-xxxxx` for each partition in the RDD where
	 * 	xxxxx is the partition's number
	 * 	---> must repartition the `DStream` to one partition before saving it to a file in order to have only ONE
	 * 	part-xxxx file per RDD folder
	 *
	 * NOTE - output file can be a local file or a file on a distributed Hadoop-compatible filesystem such as HDFS
	 */
	val textFilePath: String = s"$PATH/$outputDir/output"
	numPerType.repartition(numPartitions = 1)
		.saveAsTextFiles(
			prefix = textFilePath, // textfilepath
			suffix = "" //nothing since is still a folder
		)

	/**
	 * ---> SENDING DATA TO SPARK STREAMING
	 *  The application is running but doens't have data to process
	 *  CUE: to give the app data using `splitAndSend.sh` script
	 *
	 *  STEPS (cmd line):
	 *
	 *  STEP (1) - Make script executable
	 * 		chmod +x PATH/splitAndSend.sh
	 *
	 *  STEP (2) - Start the script and specify input folder that you used in the spark streaming code
	 *  This will start copying parts of the orders.txt file to this folder and the app will start counting buy and
	 *  sell orders in the copied files.
	 * 		./splitAndSend.sh /PATH/ch6_input local
	 *
	 * TODO must do this step before starting the streaming context? 	(pg 154)
	 */




	/**
	 * 6.1.6
	 * STARTING AND STOPPING THE STREAMING COMPUTATION
	 *
	 * Only when starting the streaming computation does output start to actually show.
	 *
	 * This starts the streaming context
	 * 	--> evaluates the `DStream`s it was used to create
	 * 	--> starts the receivers of the `DStream`s
	 * 	--> starts running the programs that the `DStream`s represent.
	 */

	ssc.start()



	// ---> STOPPING THE SPARK STREAMING CONTEXT
	// You can wait for all the files to be processed (2.5 min) or stop the streaming context:
	// NOTE - want to stop the streaming context but NOT the spark context

	ssc.awaitTermination()
	// TODO why does book say that main thread will exit until telling it to await termination? (pg 154)

	//sparkStreamingContext.stop(stopSparkContext = false)
	// NOTE - must wait for all the files to finish processing otherwise this will stop them.





	// ---> EXAMINING THE GENERATED OUTPUT
	// 	`saveAsTextFiles` creates one folder per mini-batch. If you look at your output folders, you will find two
	// 	files in each of them, named part-00000 and _SUCCESS
	// 		- _SUCCESS means writing has finished successfully
	// 		- part-00000 contains the counts that were calculated
	// 	The contents of part-00000 may look like:
	// 		(false, 9969)
	// 		(true, 10031)

	// Next task: read the outputted data into data-frame using `textFile`
	// NOTE: Can read several text files all at once using asterisks when specifying paths for `SparkContext`'s
	//  `textFile` method.
	// 	EXAMPLE: to read all the files you just generated (ni output) into a single RDD you can write:

	// HELP why is there no output in the output folder files?

	import sparkSession.implicits._

	val allOutputCounts: RDD[String] = sc.textFile(path = PATH + outputDir + "output*.txt")
	Console.println(s"\nAll output counts as RDD:")

	val allOutputCountsDF = allOutputCounts.toDF()
	Console.println(s"number of rows = ${allOutputCountsDF.count()}")
	assert(allOutputCountsDF.count() == 50000, "Check: output num rows should equal num rows from original file")
	allOutputCountsDF.show()

	println("numPerType: " + numPerType)

	// TODO check these methods once file processing starts
	// NOTE - Method show 1 = println each line
	Console.println("\nMethod show 1 = println each line")

	numPerType.foreachRDD(rdd => println(rdd))
	numPerType.count() // num rows

	// NOTE - Method show 2 = println via for-comprehension inside
	Console.println("\nMethod show 2 = println via for-comprehension inside")

	numPerType.foreachRDD( rdd => {
		for(item <- rdd.collect().toArray) {
			println(item)
		}
	})

	// NOTE - Method show 3 = slice dstream with time interval
	Console.println("\nMethod show 3 = slice dstream with time interval")

	numPerType.slice(Time(0), Time(1000)) // first 10 seconds



	// NOTE - Method show 4 - use print(num rdds in the dstream)
	numPerType.print(10)


}