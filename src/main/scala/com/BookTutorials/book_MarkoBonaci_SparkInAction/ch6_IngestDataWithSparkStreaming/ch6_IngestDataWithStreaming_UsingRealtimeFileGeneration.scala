package com.BookTutorials.book_MarkoBonaci_SparkInAction.ch6_IngestDataWithSparkStreaming

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream


// for the readstream/writestream code
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.streaming.OutputMode.{Append, Complete, Update}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.BufferedSource

/**
 *
 */
object ch6_IngestDataWithStreaming_UsingRealtimeFileGeneration extends App {



	// This part is done automatically if in the repl
	/*val sparkSession: SparkSession = SparkSession.builder()
		.master("local[2]")
		.appName("IngestDataWithStreaming")
		.getOrCreate();*/
	// REPL
	val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("IngestDataWithStreaming").getOrCreate();


	// NOTE local[n] must have n > 1 because of this warning:
	// WARN StreamingContext: spark.master should be set as local[n], n > 1 in local mode if you have receivers to get data, otherwise Spark jobs will not get resources to process the received data.

	val sc = sparkSession.sparkContext
	val sparkStreamingContext: StreamingContext = new StreamingContext(sparkContext = sc,
		batchDuration = Seconds(5))
	val ssc = sparkStreamingContext


	val PATH: String = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/BookTutorials/book_MarkoBonaci_SparkInAction/ch6_IngestDataWithSparkStreaming"
	val inputStreamFolder: String = "inputStreamFolder"
	val inputStreamFolderCSV: String = "inputStreamFolderCSV"

	val inputStreamFolderCSV_headers_cmdlineway: String = "inputStreamFolderCSV_headers_cmdlineway"
	val inputStreamFolderCSV_headers_cmdlineway_SHORT:String = "inputStreamFolderCSV_headers_cmdlineway_SHORT"
	val inputStreamFolderCSV_headers_programway: String = "inputStreamFolderCSV_headers_programway"
	val inputStreamFolderCSV_headers_programway_SHORT: String = "inputStreamFolderCSV_headers_programway_SHORT"

	val outputStreamFolder: String = "outputStreamFolder"


	val filestreamCMD: DStream[String] = ssc.textFileStream(directory = s"$PATH/$inputStreamFolderCSV_headers_cmdlineway_SHORT")
	val filestreamPRG: DStream[String] = ssc.textFileStream(directory = s"$PATH/$inputStreamFolderCSV_headers_programway_SHORT")

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
	case class Order(time: Timestamp, orderID: Long, clientID: Long, symbol: String, amount: Int, price: Double, isBuyOrSell: Boolean)

	// Parsing lines from the filestream DStream to obtain a new DStream containing Order objects
	// Using: `flatMap` transformation to operate on all elements of all RDDS in a DStream (flatMap not map because
	// want to ignore lines that don't match the format we expect; if th eline can be parsed, the function returns a
	// list with a single element, else an empty list)

	import java.text.SimpleDateFormat


	// NOTE: using the flatMap way
	//  flatMap(String => TraversableOnce[U]) ---> flatMap(String => List[U])
	val ordersByFlatMap: DStream[Order] = filestreamPRG.flatMap((line: String) => {
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




	val orders: DStream[Order] = ordersByFlatMap // simpler name

	/*val numPerType: DStream[(Boolean, Long)] = orders
		.map((order: Order) => (order.isBuyOrSell, 1L))
		.reduceByKey((c1: Long, c2: Long) => c1 + c2) //TODO understand better via repl
	*/
	// REPL
	val numPerType: DStream[(Boolean, Long)] = orders.map((order: Order) => (order.isBuyOrSell, 1L)).reduceByKey((c1: Long, c2: Long) => c1 + c2) //TODO understand better via repl



	/*import java.io.File
	import sparkSession.implicits._
	import scala.io.Source

	val outputDirObj = new File(s"$PATH/$outputStreamFolder")
	outputDirObj.setWritable(true)
	val outputDir: String = outputDirObj.getAbsolutePath()
	Console.println(s"outputDir = $outputDir")

	val textFilePathObj: File = new File(s"$PATH/$outputStreamFolder/") //buySellOutput
	textFilePathObj.setWritable(true)
	val textFilePath: String = textFilePathObj.getAbsolutePath()
	Console.println(s"textFilePath = $textFilePath")

	*/


	// --------------------- SCHEMA OF SHOWING THE RESULTS ---------------------

	// data outputting
	// output is the file name
	val extraFolderName = "buySellOutput"
	val filename = "output"
	// TODO start here - removed the 'buySellOutput' foldername but now nothing gets sent to the output folder -- to refix?
	numPerType.repartition(numPartitions = 1).saveAsTextFiles(prefix = s"$PATH/$outputStreamFolder/$extraFolderName/$filename", suffix = "txt")


	// NOTE: trying to get non empty file results and see them in console.
	// NOTE TODO trying initially to get num batches because some files' counts are combined
	// TODO how to change batch size to other than 2?

	// way 1 = textFileStream filter non empty
	// SOURCE:
	// - (google) https://george-jen.gitbook.io/data-science-and-apache-spark/untitled-92
	// - (hyp.is) https://hyp.is/W14IZKuAEe27mQshCmpsZg/george-jen.gitbook.io/data-science-and-apache-spark/untitled-92
	//sc.setCheckpointDir(s"$PATH/$outputStreamFolder") // TODO understand better: pg 278 Gerard Maas for explanation of need for checkpoint

	// TODO see here what is the type for the arg in filter()
	// TODO: see page 39 Zubair Nabi - to try method 2 of reading existing files using fileStream instead of textfilestream  (or snagit)
	val res = ssc.textFileStream(s"$PATH/$outputStreamFolder/$extraFolderName/").filter(_.nonEmpty) //.map(x => (x,x))
	Console.println("way 1: textFileStream filtering non empty")
	res.print()
	res.saveAsTextFiles(s"$PATH/$outputStreamFolder/$extraFolderName/STREAMRESULT.txt")


	// way 2 = foreach rdd show
	// SOURCE = https://george-jen.gitbook.io/data-science-and-apache-spark/foreachrdd-func
	Console.println("way 2: foreach rdd show")
	var counter = 0


	import sparkSession.implicits._

	numPerType.foreachRDD{rdd => if(!rdd.isEmpty){
		println(s"way 2: foreach rdd show\n----------------------------------------------------------" +
			s"|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n" +
			s"|\n# counter: $counter \n" +
			s"|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n|\n" +
			s"----------------------------------------------------------")
		rdd.toDF().show()
		counter += 1
	}}


	// way 3 = foreach rdd collect mkstring
	Console.println("way 3: foreach rdd collect mkstring")
	numPerType.foreachRDD{ rdd => if (!rdd.isEmpty) { println(s"rdd collect = ${rdd.collect().mkString}")}}



	// --------------------- STARTING THE COMPUTATIONS (executing the schemas above) ---------------------

	// TODO LEFT OFF HERE SPECIFIC PLAN FOR CH6:
	/**
	 * 0) repartition BEFORE
	 * 1) start streaming context
	 * 2) generate the input files in a streamingway programmatically (FIRST VIA COMMAND LINE) here
	 * 3) sleep for a few (just a few files not all)
	 * 4) stop streaming context
	 * 5) check directory if the output is there (num, buy/sell) tuple
	 */



	ssc.start()
	ssc.awaitTermination() // 60 seconds

		/// DO IN COMMAND LINE
		/*./splitAndSend_csv_headers_programway_SHORT.sh
	/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/BookTutorials/book_MarkoBonaci_SparkInAction/ch6_IngestDataWithSparkStreaming/inputStreamFolderCSV_headers_programway_SHORT/ local*/




	// OR
	/*import scala.sys.process._

	val bashfile = "splitAndSend_csv_headers_programway_SHORT.sh"
	// technique 1
	//s"$PATH/$bashfile $PATH/$inputStreamFolderCSV_headers_programway_SHORT/ local" !
	// technique 2
	val p = Process(s"$PATH/$bashfile.sh $PATH/$inputStreamFolderCSV_headers_programway_SHORT/ local").!!
	p
	*/

	//-----
	// textfilepath == $PATH/$outputStreamFolder/$extraFolderName/
	val allcounts = sc.textFile(s"$PATH/$outputStreamFolder/$extraFolderName/output*.txt")
	val counts1 = sc.textFile(s"$PATH/$outputStreamFolder/$extraFolderName/outputsaa.txt")
	val counts2 = sc.textFile(s"$PATH/$outputStreamFolder/$extraFolderName/outputsab.txt")
	val counts3 = sc.textFile(s"$PATH/$outputStreamFolder/$extraFolderName/outputsac.txt")

	allcounts.toDF().show()
	counts1.toDF().show()
	counts2.toDF().show()
	counts3.toDF().show()


	val resstream = sc.textFile(s"$PATH/$outputStreamFolder/$extraFolderName/STREAMTIMERESULT.txt")
	resstream.toDF().show()


}
