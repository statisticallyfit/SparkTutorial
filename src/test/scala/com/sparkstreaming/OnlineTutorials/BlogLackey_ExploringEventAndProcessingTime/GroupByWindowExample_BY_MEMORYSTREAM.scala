package com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime




import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}

import org.apache.spark.sql.execution.streaming.MemoryStream


/**
 * Source = https://github.com/buildlackey/spark-streaming-group-by-event-time/blob/master/src/main/scala/com/lackey/stream/examples/GroupByWindowExample.scala
 */
object GroupByWindowExample_BY_MEMORYSTREAM extends App {


	val sparkSession: SparkSession = SparkSession.builder
		.master("local[1]")
		.appName("example")
		.getOrCreate()

	import sparkSession.implicits._
	implicit val sparkContext: SQLContext = sparkSession.sqlContext // for memory stream

	Logger.getLogger("org").setLevel(Level.OFF)
	//Logger.getLogger(classOf[MicroBatchExecution]).setLevel(Level.DEBUG)


	// Step 1: Reading Data
	case class AnimalView(timeSeen: Timestamp, animal: String, howMany: Integer)

	val fmt: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

	val memoryStream: MemoryStream[AnimalView] = MemoryStream[AnimalView]

	val queryAnimalDS: Dataset[AnimalView] = memoryStream.toDS()

	// TODO meaning of this?
	val writeQuery: StreamingQuery = queryAnimalDS.writeStream
		.format(source = "console")
		.option(key = "truncate", value = false)
		.start()


	// Step 2: adding data as memory stream

	memoryStream.addData(
		Seq(
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:18"), "rat", 1),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:19"), "hog", 2),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:19"), "rat", 2),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:22"), "dog", 5),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:21"), "pig", 2),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:22"), "duck", 4),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:22"), "dog", 4),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:24"), "dog", 4),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:26"), "mouse", 2),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:27"), "horse", 2),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:27"), "bear", 2),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:29"), "lion", 2),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:31"), "tiger", 4),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:31"), "tiger", 4),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:32"), "fox", 4),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:33"), "wolf", 4),
			AnimalView(Timestamp.valueOf("2019-06-18 16:33:34"), "sheep", 4),
		)
	)
	writeQuery.processAllAvailable()

	// Step 3 - Defining window on event time
	// Defining window which aggregates the aminal type value for the last 10 seconds
	// Like partitioning (animal type ) in groups of 10 seconds
	val windowedCount: DataFrame = queryAnimalDS
		//.withWatermark("timeSeen", "5 seconds")
		.groupBy(
			window(timeColumn = $"timeSeen", windowDuration = "10 seconds"), // group by time
			$"animal" 												 // group by animal type
		)
		.sum(colNames = "howMany")

	val dataStreamWriter: DataStreamWriter[Row] = windowedCount.writeStream
		.trigger(Trigger.ProcessingTime(5 * 1000))
		.format(source = "console")
		.option(key = "truncate", value = false)
		.outputMode(outputMode = OutputMode.Complete())


	/*val dataStreamWriter: DataStreamWriter[Row] = windowedCount.writeStream
		.format(source = "console")
		.option(key = "truncate", value = false)
		.outputMode(outputMode = OutputMode.Complete())*/

	dataStreamWriter.start().awaitTermination(timeoutMs = 40000) // 40,000 ms = 40 s


}
