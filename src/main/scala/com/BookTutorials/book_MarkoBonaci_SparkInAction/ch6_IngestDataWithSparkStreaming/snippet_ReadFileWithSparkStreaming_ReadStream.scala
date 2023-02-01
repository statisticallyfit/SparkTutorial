package com.BookTutorials.book_MarkoBonaci_SparkInAction.ch6_IngestDataWithSparkStreaming


import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.OutputMode.{Append, Update, Complete}
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
	val inputStreamFolderCSV_headers: String = "inputStreamFolderCSV_headers"
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
			StructField("BuyOrSell", StringType, true)
		)
	)
	val df: DataFrame = sparkSession.readStream
		.schema(schema)
		.option("header", true)
		.option("maxFilesPerTrigger", 1)
		.csv(s"$PATH/$inputStreamFolderCSV_headers")


	df.printSchema()


	val selectDF: DataFrame = df.select("BuyOrSell")

	val groupDF: DataFrame = df.select("BuyOrSell")
		.groupBy("BuyOrSell")
		.count()
	// NOTE count = counts the number of rows for each group: https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/RelationalGroupedDataset.html#count():org.apache.spark.sql.DataFrame
	// TODO - what are the groups?

	val query1 = selectDF
		.writeStream
		.format("console")
		.outputMode(Update()) // TODO worked with update
		//.trigger(Trigger.Continuous("2 seconds")) // gives error AnalysisException: Continuous processing does not support StreamingRelation operations
		.trigger(Trigger.Once()) // interval = 3 seconds
		.start()
		.awaitTermination()

	val query2 = groupDF
		.writeStream
		.format("console")
		.outputMode(Complete())
		//.trigger(Trigger.Continuous("2 seconds")) // gives error AnalysisException: Continuous processing does not support StreamingRelation operations
		.trigger(Trigger.ProcessingTime("5 seconds")) // interval = 5 seconds
		.start()
		.awaitTermination()

	//val tq = query.awaitTermination(3000) // timeout after 30 seconds

	//println(groupDF.head(10))
	/*
	//NOTE count() operation is not defined for streaming data because it is always arriving (pg. 90 Gerard Maas)
	// NOTE error thrown will be: "org.apache.spark.sql.AnalysisException: Queries with streaming sources must be
	    executed with writeStream.start();;"
	println(groupDF.count())
	println(groupDF.head(10))
	println(groupDF.show(10))*/

	//Console.println(s"df.count() = ${df.count()}")
}
