package com.sparkstreaming.OnlineTutorials.BlogMadhukar_SparkStructuredStreaming



import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._ //window
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery}

import java.sql.Timestamp

import com.sparkstreaming.OnlineTutorials.TimeConsts._
/**
 * Blog code = https://blog.madhukaraphatak.com/introduction-to-spark-structured-streaming-part-9
 * Source code = https://github.com/phatak-dev/spark2.0-examples/blob/master/src/main/scala/com/madhukaraphatak/examples/sparktwo/streaming/ProcessingTimeWindow.scala
 */
object BlogMadhukar_Part9_ProcessingTime extends App {


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


	// Converting memory stream to data frame
	// Adding timestamp column to data frame
	// Converting data frame back to dataset
	val stockDS: Dataset[Stock] = memoryStream.toDF()
		.withColumn(colName = "processingTime", col = current_timestamp())
		.as[Stock]
		/*.toDF("timeGenerated", "stockSymbol", "stockValue")
		.withWatermark(eventTime = "timeGenerated", delayThreshold = toWord(TEN_SEC))
		.groupBy(window(timeColumn = $"timeGenerated", windowDuration = toWord(FIFTY_SEC)))
		.agg(sum("stockValue"))
		.withColumn(colName = "processingTime", col = current_timestamp())
		//.orderBy("window")
		.as[Stock]*/

	// TODO left off here error continue with this example = https://www.waitingforcode.com/apache-spark-structured-streaming/apache-spark-structured-streaming-watermarks/read

		//.dropDuplicatesWithinWatermark(col1 = "id")

	val stockStreamingQuery: StreamingQuery = stockDS.writeStream.format(source = "console").option(key = "truncate", value = false).start()



	// Step 2: add data to memory stream (aka reading data)
	memoryStream.addData(
		Seq(
			Stock(Timestamp.valueOf("2016-04-27 14:34:22.0"), "aapl", 500),
			Stock(Timestamp.valueOf("2016-04-27 14:34:27.001"), "aapl", 600),
		)
	)
	// Process this batch of data
	stockStreamingQuery.processAllAvailable()


	// Add more data for next 10-second batch
	memoryStream.addData(
		Seq(
			Stock(Timestamp.valueOf("2016-04-27 14:34:32.0"), "aapl", 400),
			Stock(Timestamp.valueOf("2016-04-27 14:34:33.0"), "aapl", 100),
			Stock(Timestamp.valueOf("2016-04-27 14:34:34.0"), "aapl", 100),
			Stock(Timestamp.valueOf("2016-04-27 14:34:39.0"), "aapl", 50),
			Stock(Timestamp.valueOf("2016-04-27 14:34:50.0"), "aapl", 7)
		)
	)
	// Process this batch of data
	stockStreamingQuery.processAllAvailable()


	// Add more data for next 10-second batch
	memoryStream.addData(
		Seq(
			Stock(Timestamp.valueOf("2016-04-27 14:34:27.001"), "aapl", 200),
		)
	)
	// Process this batch of data
	stockStreamingQuery.processAllAvailable()

	// Add more data for next 10-second batch
	memoryStream.addData(
		Seq(
			Stock(Timestamp.valueOf("2016-04-27 14:34:42.0"), "aapl", 500),
			Stock(Timestamp.valueOf("2016-04-27 14:34:47.0"), "aapl", 300),
		)
	)
	// Process this batch of data
	stockStreamingQuery.processAllAvailable()


	// Step 3 - Defining window on event time
	// Defining window which aggregates the aminal type value for the last 10 seconds
	// Meaning: partitioning (animal type ) in groups of 10 seconds
	val tumblingWindow: DataFrame = stockDS
		.groupBy(window(timeColumn = $"timeGenerated", windowDuration = toWord(FIFTY_SEC)))
		.sum("stockValue")
	//.orderBy("window")
	// TODO research - what makes this a "tumbling" window? - the orderby?

	val dataStreamWriter: DataStreamWriter[Stock] = stockDS.writeStream
		.format(source = "console")
		.option(key = "truncate", value = false)
		.outputMode(OutputMode.Append())

	// TODO - error if put "Complete" - says complete output mode not supported when there are no streaming aggregations on streaming dfs/dss ---- but I made a .agg(sum) above on streaming DS... understand better what is streaming aggregation to know when complete output mode applies
	// TODO - see "Output modes" article by Bartosz Konieckzny

	dataStreamWriter.start().awaitTermination(120000) // 120,000 ms == 120 s == 2 min

	//stockStreamingQuery.processAllAvailable()
}
