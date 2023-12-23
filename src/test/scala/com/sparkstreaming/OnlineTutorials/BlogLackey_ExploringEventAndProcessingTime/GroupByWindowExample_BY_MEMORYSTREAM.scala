package com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime




import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}

import org.apache.spark.sql.execution.streaming.MemoryStream


import org.apache.spark.sql.streaming.{StreamTest, StreamSuite}

import org.apache.spark.sql.streaming.util.StreamManualClock

/**
 * Source = https://github.com/buildlackey/spark-streaming-group-by-event-time/blob/master/src/main/scala/com/lackey/stream/examples/GroupByWindowExample.scala
 */
object GroupByWindowExample_BY_MEMORYSTREAM extends StreamSuite with StreamTest with App {


	val sparkSession: SparkSession = SparkSession.builder
		.master("local[1]")
		.appName("example")
		.getOrCreate()

	import sparkSession.implicits._
	implicit val sc: SQLContext = sparkSession.sqlContext // for memory stream

	Logger.getLogger("org").setLevel(Level.OFF)
	//Logger.getLogger(classOf[MicroBatchExecution]).setLevel(Level.DEBUG)


	// Step 1: Reading Data
	case class AnimalView(timeSeen: Timestamp, animal: String, howMany: Integer)

	//val fmt: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

	val animalMemoryStream: MemoryStream[AnimalView] = MemoryStream[AnimalView]

	val animalDS: Dataset[AnimalView] = animalMemoryStream.toDS()
		//.withWatermark(eventTime = "timeSeen", delayThreshold = "5 seconds") // TODO attempt to batch the memorystream groups



	// TODO meaning vs. window 10 seconds?
	val triggerProcFiveSeconds: Trigger = Trigger.ProcessingTime("5 seconds")

	// Step 2: adding data as memory stream

	val animals: Seq[AnimalView] = Seq(
		// window: 16:33:15 - 16:33:20
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:18"), "rat", 1),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:19"), "hog", 2),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:19"), "rat", 2),
		// window: 16:33:20 - 16:33:25
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:22"), "dog", 5),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:21"), "pig", 2),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:22"), "duck", 4),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:22"), "dog", 4),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:24"), "dog", 4),
		// window: 16:33:25 - 16:33:30
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:26"), "mouse", 2),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:27"), "horse", 2),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:27"), "bear", 2),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:29"), "lion", 2),
		// window: 16:33:30 - 16:33:35
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:31"), "tiger", 4),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:31"), "tiger", 4),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:32"), "fox", 4),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:33"), "wolf", 4),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:34"), "sheep", 4),
	)
	val animalsBatch0: Seq[AnimalView] = animals.take(3)
	val animalsBatch1: Seq[AnimalView] = animals.slice(3, 8)
	val animalsBatch2: Seq[AnimalView] = animals.slice(8, 12)
	val animalsBatch3: Seq[AnimalView] = animals.slice(12, 17)

	final val TEN_SECONDS: Long = 10*1000


	// NOTE: can make batches arrive as they should, erasing this for now to test the spark way of doing it
	val writeQuery: StreamingQuery = animalDS.writeStream
		.format(source = "console")
		.option(key = "truncate", value = false)
		.start()

	/*animalMemoryStream.addData(animalsBatch0)
	writeQuery.processAllAvailable()

	animalMemoryStream.addData(animalsBatch1)
	writeQuery.processAllAvailable()*/


	// SOURCE = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/streaming/StreamSuite.scala#L319
	// For each batch, we would log the state change during the execution
	// This checks whether the key of the state change log is the expected batch id
	def CheckIncrementalExecutionCurrentBatchId(expectedId: Int): AssertOnQuery =
		AssertOnQuery(_.lastExecution.currentBatchId == expectedId,
			s"lastExecution's currentBatchId should be $expectedId")

	// SOURCE example = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/streaming/StreamSuite.scala#L330
	testStream(animalDS)(
		StartStream(triggerProcFiveSeconds, new StreamManualClock),

		// --- batch 0 ------------------------
		// Add some data in batch 0
		AddData(animalMemoryStream, animalsBatch0:_*),
		// 10 seconds // TODO does this have to equal the trigger time?
		AdvanceManualClock(timeToAdd = TEN_SECONDS ),

		// --- batch 1 ------------------------
		// Check the results of batch 0
		CheckAnswer(animalsBatch0:_*),
		CheckIncrementalExecutionCurrentBatchId(0),
		// Add data in batch 1
		AddData(animalMemoryStream, animalsBatch1:_*),
		AdvanceManualClock(TEN_SECONDS),

		// --- batch 2 ------------------------
		// check results of batch 1
		CheckAnswer((animalsBatch0 ++ animalsBatch1):_*),
		CheckIncrementalExecutionCurrentBatchId(1),
		AdvanceManualClock(TEN_SECONDS),
		AdvanceManualClock(TEN_SECONDS),

		// Check the results of batch 1 again; this is to make sure that, when there's no new data,
		// the currentId does not get logged (e.g. as 2) even if the clock has advanced many times
		CheckAnswer((animalsBatch0 ++ animalsBatch1):_*),
		CheckIncrementalExecutionCurrentBatchId(1),

		StopStream
	)
	writeQuery.processAllAvailable() // TODO how to show outputs of above like this line does? 


	// Step 3 - Defining window on event time
	// Defining window which aggregates the aminal type value for the last 10 seconds
	// Like partitioning (animal type ) in groups of 10 seconds
	val windowedCount: DataFrame = animalDS
		//.withWatermark("timeSeen", "5 seconds")
		.groupBy(
			window(timeColumn = $"timeSeen", windowDuration = "10 seconds"), // group by time
			$"animal" 												 // group by animal type
		)
		.sum(colNames = "howMany")

	val dataStreamWriter: DataStreamWriter[Row] = windowedCount.writeStream
		.trigger(triggerProcFiveSeconds) // 5 seconds
		.format(source = "console")
		.option(key = "truncate", value = false)
		.outputMode(outputMode = OutputMode.Complete())



	/*val dataStreamWriter: DataStreamWriter[Row] = windowedCount.writeStream
		.format(source = "console")
		.option(key = "truncate", value = false)
		.outputMode(outputMode = OutputMode.Complete())*/

	dataStreamWriter.start().awaitTermination(timeoutMs = 40000) // 40,000 ms = 40 s

	/*
	 -------------------------------------------
	Batch: 0
	-------------------------------------------
	+-------------------+------+-------+
	|timeSeen           |animal|howMany|
	+-------------------+------+-------+
	|2019-06-18 16:33:18|rat   |1      |
	|2019-06-18 16:33:19|hog   |2      |
	|2019-06-18 16:33:19|rat   |2      |
	|2019-06-18 16:33:22|dog   |5      |
	|2019-06-18 16:33:21|pig   |2      |
	|2019-06-18 16:33:22|duck  |4      |
	|2019-06-18 16:33:22|dog   |4      |
	|2019-06-18 16:33:24|dog   |4      |
	|2019-06-18 16:33:26|mouse |2      |
	|2019-06-18 16:33:27|horse |2      |
	|2019-06-18 16:33:27|bear  |2      |
	|2019-06-18 16:33:29|lion  |2      |
	|2019-06-18 16:33:31|tiger |4      |
	|2019-06-18 16:33:31|tiger |4      |
	|2019-06-18 16:33:32|fox   |4      |
	|2019-06-18 16:33:33|wolf  |4      |
	|2019-06-18 16:33:34|sheep |4      |
	+-------------------+------+-------+

	-------------------------------------------
	Batch: 0
	-------------------------------------------
	+------------------------------------------+------+------------+
	|window                                    |animal|sum(howMany)|
	+------------------------------------------+------+------------+
	|{2019-06-18 16:33:30, 2019-06-18 16:33:40}|wolf  |4           |
	|{2019-06-18 16:33:20, 2019-06-18 16:33:30}|horse |2           |
	|{2019-06-18 16:33:10, 2019-06-18 16:33:20}|hog   |2           |
	|{2019-06-18 16:33:20, 2019-06-18 16:33:30}|dog   |13          |
	|{2019-06-18 16:33:20, 2019-06-18 16:33:30}|lion  |2           |
	|{2019-06-18 16:33:20, 2019-06-18 16:33:30}|duck  |4           |
	|{2019-06-18 16:33:20, 2019-06-18 16:33:30}|bear  |2           |
	|{2019-06-18 16:33:30, 2019-06-18 16:33:40}|tiger |8           |
	|{2019-06-18 16:33:20, 2019-06-18 16:33:30}|pig   |2           |
	|{2019-06-18 16:33:30, 2019-06-18 16:33:40}|fox   |4           |
	|{2019-06-18 16:33:10, 2019-06-18 16:33:20}|rat   |3           |
	|{2019-06-18 16:33:30, 2019-06-18 16:33:40}|sheep |4           |
	|{2019-06-18 16:33:20, 2019-06-18 16:33:30}|mouse |2           |
	+------------------------------------------+------+------------+
	 */

}
