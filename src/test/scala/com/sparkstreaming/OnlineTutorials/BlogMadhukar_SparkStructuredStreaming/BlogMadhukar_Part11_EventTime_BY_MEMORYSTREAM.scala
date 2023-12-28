package com.sparkstreaming.OnlineTutorials.BlogMadhukar_SparkStructuredStreaming

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}

import java.sql.Timestamp


import com.sparkstreaming.OnlineTutorials.TimeConsts._


/**
 * Source = https://blog.madhukaraphatak.com/introduction-to-spark-structured-streaming-part-11
 */
object BlogMadhukar_Part11_EventTime_BY_MEMORYSTREAM extends App {

	val sparkSession: SparkSession = SparkSession
		.builder()
		.master("local[1]")
		.appName("BlogMadhukar_part11")
		.getOrCreate()
	// REPL
	// val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("BlogMadhukar11").getOrCreate()

	import sparkSession.implicits._

	implicit val sparkContext: SQLContext = sparkSession.sqlContext // for memory stream


	// Step 1: Preparing for ingestion of data
	val memoryStream: MemoryStream[Stock] = MemoryStream[Stock]


	val stockDS: Dataset[Stock] = memoryStream.toDS()
	//.withWatermark(eventTime = "timeGenerated", delayThreshold = "15 seconds")
	//.dropDuplicatesWithinWatermark(col1 = "id")

	//val writeQuery: StreamingQuery = queryStockDS.writeStream.format(source = "console").option(key = "truncate", value = false).start()


	// Batch 0 ---------------------------------------------------------------------------------
	memoryStream.addData(
		Seq(
			Stock(Timestamp.valueOf("2016-04-27 14:34:22.0"), "aapl", 500),
			Stock(Timestamp.valueOf("2016-04-27 14:34:27.001"), "aapl", 600),
			Stock(Timestamp.valueOf("2016-04-27 14:34:32.0"), "aapl", 400),
			Stock(Timestamp.valueOf("2016-04-27 14:34:33.0"), "aapl", 100),
			Stock(Timestamp.valueOf("2016-04-27 14:34:34.0"), "aapl", 100),
			Stock(Timestamp.valueOf("2016-04-27 14:34:27.001"), "aapl", 200),
		)
	)


	// Step 2 - Defining window on event time
	// Defining window which aggregates the stock value for the last 10 seconds
	// Like partitioning (stock prices) in groups of 10 seconds
	val windowedCount: DataFrame = stockDS
		.groupBy(
			window(timeColumn = $"timeGenerated", windowDuration = toWord(TEN_SEC))
		)
		.sum(colNames = "stockValue")


	val dataStreamWriter: DataStreamWriter[Row] = windowedCount /*animalDS.toDF()*/ .writeStream
		.format(source = "console")
		.option(key = "truncate", value = false)
		.outputMode(OutputMode.Complete())

	dataStreamWriter.start().awaitTermination(120000) // 120,000 ms == 120 s == 2 min

}
