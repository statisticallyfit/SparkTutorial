package com.BookTutorials.book_MarkoBonaci_SparkInAction.ch6_IngestDataWithSparkStreaming


import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream


/**
 * <GOAL 1/>: read in file using spark streaming context
 * <GOAL 2/>: compare reading in file that was generated manually versus one that was generated using a streaming
 * method (from the ch6_IngestData.scala file)
 *
 * To see how streaming methods can read what is in the file - does it work or not?
 *
 */
object snippet_ReadFileWithSparkStreaming extends App {

	val sparkSession: SparkSession = SparkSession.builder()
		.master("local[2]")
		.appName("snippet_ReadFileWithSparkContext")
		.getOrCreate();
	// REPL
	// val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("snippet_ReadFileWithSparkContext").getOrCreate();

	// NOTE local[n] must have n > 1 because of this warning:
	// WARN StreamingContext: spark.master should be set as local[n], n > 1 in local mode if you have receivers to get data, otherwise Spark jobs will not get resources to process the received data.

	// To be able to convert to df
	import sparkSession.implicits._


	val sc: SparkContext = sparkSession.sparkContext
	val sparkStreamingContext: StreamingContext = new StreamingContext(sparkContext = sc,
		batchDuration = Seconds(5))
	val ssc: StreamingContext = sparkStreamingContext


	// PATH
	val PATH: String = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/BookTutorials/book_MarkoBonaci_SparkInAction/ch6_IngestDataWithSparkStreaming"
	val inputStreamFolderCSV: String = "inputStreamFolderCSV"
	val inputManualFolderCSV: String = "inputManualFolderCSV"
	val outputStreamFolderCSV: String = "test_outputStreamFolderCSV"
	//val fileStreamGen: String = "ordersaa.csv" // the name of the file generated using a streaming method
	//val fileManualGen: String = "manualFile.csv" // the name of the file created manually

	// Read in the file from the folder, the file that was created (from ch6_ingestdata.scala) using a streaming method
	val textFileStreamGen: DStream[String] = ssc.textFileStream(directory =
		s"$PATH/$inputStreamFolderCSV") ///*/$fileStreamGen*/")
	val textFileManualGen: DStream[String] = ssc.textFileStream(directory = s"$PATH/$inputManualFolderCSV")


	// Read in the data (straeming-way) and place it in dstream
	val dstreamManual: DStream[String] = textFileManualGen.flatMap((line: String) => {
		val linesplit: Array[String] = line.split("[\\r\\n]+") //split at new line
		//linesplit(0) //TODO meaning - first column? or first element?
		/*val pairs = linesplit.map(l => (l, 1))
		pairs.toList*/
		linesplit
	})

	// Folder = manualOutput, filenames = "manualOutput-1239....txt"
	dstreamManual
		.repartition(1)
		.saveAsTextFiles(
			prefix = s"$PATH/$outputStreamFolderCSV/manualOutput/manualOutput",
			suffix = "txt"
		)

	ssc.start()
	/*Console.println(s"Show stream df output: ")
	textFileStreamGen.take(10).foreach(println)

	Console.println(s"\nShow manual df output")
	textFileManualGen.toDF().show()*/
}
