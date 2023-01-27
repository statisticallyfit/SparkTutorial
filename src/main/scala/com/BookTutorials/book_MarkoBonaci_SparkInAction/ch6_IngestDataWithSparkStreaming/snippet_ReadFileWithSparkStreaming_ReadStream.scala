package com.BookTutorials.book_MarkoBonaci_SparkInAction.ch6_IngestDataWithSparkStreaming


import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
 * Sources:
 * 	https://sparkbyexamples.com/spark/spark-streaming-read-json-files-from-directory/
 * 	https://hyp.is/j1IZFJ2WEe2hNWNJi9b6LA/spark.apache.org/docs/latest/structured-streaming-programming-guide.html
 */
object snippet_ReadFileWithSparkStreaming_ReadStream extends App {


	val sparkSession: SparkSession = SparkSession.builder()
		.master("local[2]")
		.appName("snippet_ReadFileWithSparkContext")
		.getOrCreate();
	// REPL
	// val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("snippet_ReadFileWithSparkContext").getOrCreate();

	// NOTE local[n] must have n > 1 because of this warning:
	// WARN StreamingContext: spark.master should be set as local[n], n > 1 in local mode if you have receivers to get data, otherwise Spark jobs will not get resources to process the received data.

	sparkSession.sparkContext.setLogLevel("ERROR")

	// To be able to convert to df
	import sparkSession.implicits._


	// TODO this readStream tryout below, even if it works, won't be categorized as under Streaming ebcause it
	//  doesn't use the StreamingContext ? true or false?
	/*val sc: SparkContext = sparkSession.sparkContext
	val sparkStreamingContext: StreamingContext = new StreamingContext(sparkContext = sc,
		batchDuration = Seconds(5))
	val ssc: StreamingContext = sparkStreamingContext*/


	// PATH
	val PATH: String = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/BookTutorials/book_MarkoBonaci_SparkInAction/ch6_IngestDataWithSparkStreaming"
	val inputStreamFolderCSV: String = "inputStreamFolderCSV"
	val inputManualFolderCSV: String = "inputManualFolderCSV"
	val outputStreamFolderCSV: String = "snippet_outputStreamFolderCSV"
	val manualOutputFolder: String = "manualOutput"
	val manualOutputFilename: String = "manualOutput"
	val streamOutputFolder: String = "streamOutput"
	val streamOutputFilename: String = "streamOutput-B-"



	import org.apache.spark.sql.types.{TimestampType, LongType, DoubleType, BooleanType, IntegerType, StringType,
		StructField, StructType}

	val schema = StructType(
		List(
			StructField("Timestamp", TimestampType, true),
			StructField("OrderID", LongType, true),
			StructField("ClientID", LongType, true),
			StructField("StockSymbol", StringType, true),
			StructField("NumStocks", IntegerType, true),
			StructField("Price", DoubleType, true),
			StructField("BuyOrSell", BooleanType, true)
		)
	)
	val df: DataFrame = sparkSession.readStream
		/*.schema(schema)*/
		.csv(s"$PATH/$inputStreamFolderCSV")


	df.printSchema()

	/*val groupDF: DataFrame = df.select("Price")
		.groupBy("BuyOrSell")
		.count()*/
	println(df.count())
	println(df.head(10))
	println(df.show(10))

	/*groupDF*/df.writeStream
		.format("console")
		.outputMode("complete")
		.start()
		.awaitTermination(3000) // timeout after 30 seconds

	//Console.println(s"df.count() = ${df.count()}")
}
