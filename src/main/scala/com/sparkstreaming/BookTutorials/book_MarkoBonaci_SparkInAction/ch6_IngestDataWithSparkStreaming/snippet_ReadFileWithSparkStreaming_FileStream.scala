package com.BookTutorials.book_MarkoBonaci_SparkInAction.ch6_IngestDataWithSparkStreaming

// Sources where I got these library names to pass the argument types to filestream() method:
// ---> spark tests: https://github// .com/apache/spark/blob/ec424c5b0e392acc57e825fb94a21d6963ebece9/streaming/src/main/scala/org/apache/spark/streaming/StreamingContext.scala#L31-L33
// ---> zubair file ch6: https://github.com/ZubairNabi/prosparkstreaming/blob/master/Chap4/src/main/scala/org/apress/prospark/L4-4Kryo.scala#L3-L6
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.{DStream, InputDStream}


/**
 * <GOAL 1/>: read in file using spark streaming context
 * <GOAL 2/>: compare reading in file that was generated manually versus one that was generated using a streaming
 * method (from the ch6_IngestData.scala file)
 *
 * To see how streaming methods can read what is in the file - does it work or not?
 *
 */
object snippet_ReadFileWithSparkStreaming_FileStream extends App {

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


	final val NUM_STREAM_SECONDS: Int = 1


	val sc: SparkContext = sparkSession.sparkContext
	val ssc: StreamingContext = new StreamingContext(sparkContext = sc, batchDuration = Seconds(NUM_STREAM_SECONDS))



	// PATH
	val PATH: String = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com" +
		"/BookTutorials/book_MarkoBonaci_SparkInAction/ch6_IngestDataWithSparkStreaming"
	val inputStreamFolderCSV: String = "inputStreamFolderCSV"
	val inputManualFolderCSV: String = "inputManualFolderCSV"
	val outputStreamFolderCSV: String = "snippet_outputStreamFolderCSV"
	val manualOutputFolder: String = "manualOutput"
	val manualOutputFilename: String = "manualOutput"
	val streamOutputFolder: String = "streamOutput"
	val streamOutputFilename: String = "streamOutput-A-"


	// Read in the files from the folder, the file that was created (from ch6_ingestdata.scala) using a streaming
	// method

	// Get the dstream of strings for the streaming-way-created input
	val fs1: InputDStream[(LongWritable, Text)] = ssc.fileStream[LongWritable, Text, TextInputFormat](
		directory = s"$PATH/$inputStreamFolderCSV",
		filter = (f: Path) => true,
		newFilesOnly = false // so that it doesn't ignore old files
	)
	val textFileStreamGen: DStream[String] = fs1.map{ case (longWritable, text) => text.toString }


	// Get the dstream of strings for the manually-created input
	val fs2: InputDStream[(LongWritable, Text)] = ssc.fileStream[LongWritable, Text, TextInputFormat](
		directory = s"$PATH/$inputManualFolderCSV",
		filter = (f: Path) => true,
		newFilesOnly = false // so that it doesn't ignore old files
	)
	val textFileManualGen: DStream[String] = fs2.map{ case (longWritable, text) => text.toString }




	// Task 2: convert InputDStream to DStream
	// Read in the data that was generated manually and place it in dstream
	// TODO try regex \r?\n|\r
	// [\r\n]+
	val dstreamManual_way2: DStream[List[String]] = textFileManualGen.map((line: String) => {
		val linesplit: Array[String] = line.split("[\\r\\n]+") //split at new line
		//linesplit(0) //TODO meaning - first column? or first element?
		/*val pairs = linesplit.map(l => (l, 1))
		pairs.toList*/
		linesplit.toList
	})


	val dstreamManual_way1: DStream[Array[String]] = textFileManualGen.map(line => line.split("\\s+"))

	// TODO way 1 of seeing output of the dstream
	// Folder = manualOutput, filenames = "manualOutput-1239....txt"
	dstreamManual_way1.repartition(1).saveAsTextFiles(
			prefix = s"$PATH/$outputStreamFolderCSV/$manualOutputFolder/$manualOutputFilename",
			suffix = "txt"
		)

	// Way 2 of seeing output of the dstream
	dstreamManual_way1.print() // first 10 elements


	// ---- Do the stream
	/*val dstreamStream_way1: DStream[Array[String]] = textFileStreamGen.map(line => line.split("\\s+"))
	// print out?
	dstreamStream_way1.foreachRDD((rddArrStr, time) => {
		val inputsCount  = rddArrStr.count()
		if(inputsCount > 0){
			val resultRDD = rddArrStr.repartition(2)
		}
	})*/


	ssc.start()
	ssc.awaitTermination()

	// TODO HELP how to fix all files generated in manual output area are all EMPTY

}
