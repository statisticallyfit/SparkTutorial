package com.sparkstreaming.OnlineTutorials.BlogMadhukar_SparkStructuredStreaming

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}

import java.sql.Timestamp

/**
 * Source = https://blog.madhukaraphatak.com/introduction-to-spark-structured-streaming-part-11
 */
object BlogMadhukar_Part11_EventTime_USING_MEMORYSTREAM extends App {

	val sparkSession: SparkSession = SparkSession
		.builder()
		.master("local[1]")
		.appName("BlogMadhukar_part11")
		.getOrCreate()
	// REPL
	// val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("BlogMadhukar11").getOrCreate()

	import sparkSession.implicits._

	implicit val sparkContext: SQLContext = sparkSession.sqlContext // for memory stream


	// Step 1: Reading data
	case class Stock(timeGenerated: Timestamp /*Long*/ , stockSymbol: String, stockValue: Double)

	val memoryStream: MemoryStream[Stock] = MemoryStream[Stock]


	// TODO compare between putting watermark here at memorystream (like in bartosz) vs. at windowedcount (like in madhukar)
	// answer: no difference because the withwatermark gets called after toDS() and that is true for both cases.
	val queryStockDS: Dataset[Stock] = memoryStream.toDS()
	//.withWatermark(eventTime = "timeGenerated", delayThreshold = "15 seconds")
	//.dropDuplicatesWithinWatermark(col1 = "id")

	//val writeQuery: StreamingQuery = queryStockDS.writeStream.format(source = "console").option(key = "truncate", value = false).start()


	// Batch 0 ---------------------------------------------------------------------------------

	// Inputs
	// 1461756862000,"aapl",500.0  // 2016-04-27 14:34:22.0
	// 1461756867001,"aapl",600.0 // 2016-04-27 14:34:27.001
	// 1461756872000,"aapl",400.0 //2016-04-27 14:34:32.0
	// 1461756867001,"aapl",200.0 // 2016-04-27 14:34:27.001 // late event for Wed, 27 Apr, 2016, 11:34:27

	// TODO NEXT TASKS:
	// 1. see how to inter-delay events between each other
	// 2. see if can import from streaming tests spark =


	// Step 2: adding data to memory stream

	memoryStream.addData(
		Seq(
			Stock(Timestamp.valueOf("2016-04-27 14:34:22.0"), "aapl", 500),
			Stock(Timestamp.valueOf("2016-04-27 14:34:27.001"), "aapl", 600),
			Stock(Timestamp.valueOf("2016-04-27 14:34:32.0"), "aapl", 400),
			Stock(Timestamp.valueOf("2016-04-27 14:34:27.001"), "aapl", 200),
		)
	)


	// Step 3 - Defining window on event time
	// Defining window which aggregates the stock value for the last 10 seconds
	// Like partitioning (stock prices) in groups of 10 seconds
	val windowedCount: DataFrame = queryStockDS
		.groupBy(
			window(timeColumn = $"timeGenerated", windowDuration = "10 seconds")
		)
		.sum(colNames = "stockValue") // TODO meaning of sum


	val dataStreamWriter: DataStreamWriter[Row] /*: DataStreamWriter[Row]*/ = windowedCount.writeStream
		.format(source = "console")
		.option(key = "truncate", value = false)
		.outputMode(outputMode = OutputMode.Complete())

	dataStreamWriter.start().awaitTermination(timeoutMs = 40000) // 40,000 ms = 40 s

}
