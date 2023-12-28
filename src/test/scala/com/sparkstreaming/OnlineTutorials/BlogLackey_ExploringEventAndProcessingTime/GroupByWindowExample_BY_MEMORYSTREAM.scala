package com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime




import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}

import org.apache.spark.sql.execution.streaming.MemoryStream


import org.apache.spark.sql.streaming.{StreamTest, StreamSuite}

import org.apache.spark.sql.streaming.util.StreamManualClock

import com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime.AnimalData._

import com.sparkstreaming.OnlineTutorials.TimeConsts._


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


	// Step 1: Reading Data

	val animalMemoryStream: MemoryStream[AnimalView] = MemoryStream[AnimalView]

	val animalDS: Dataset[AnimalView] = animalMemoryStream.toDS()
		//.withWatermark(eventTime = "timeSeen", delayThreshold = "50 seconds") // TODO attempt to batch the memorystream groups

	// TODO meaning vs. window 10 seconds?
	val triggerProcFiveSeconds: Trigger = Trigger.ProcessingTime(toWord(FIFTY_SEC))




	/**
	 * SUMMARIZE: ways of adding data to straem to make batches appear intermittently
	 *
	 * 1) memory stream - add every batch intermittently + writequery.processallavailable
	 * 2) memory stream add using spark teststream + writequery DEFINED only
	 *
	 * TODO
	 * 	- how to sum all batches at the end?
	 * 	- just with writequery vs. just with datastreamwriter? - Evaluate effect on batch appearance + summing at end.
	 *   - see how to inter-delay events between each other
	 */

	// NOTE: can make batches arrive as they should, erasing this for now to test the spark way of doing it
	val writeQuery: StreamingQuery = animalDS.writeStream
		.trigger(triggerProcFiveSeconds)
		.format(source ="console")
		.option(key = "truncate", value = false)
		//.outputMode(OutputMode.Complete()) //error
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
		AdvanceManualClock(timeToAdd = FIVE_MILLISEC ),
		// ProcessAllAvailable(),


		// --- batch 1 ------------------------
		// Check the results of batch 0
		//CheckAnswer(animalsBatch0:_*),
		CheckIncrementalExecutionCurrentBatchId(0),
		// Add data in batch 1
		AddData(animalMemoryStream, animalsBatch1:_*),
		AdvanceManualClock(FIVE_MILLISEC),
		//ProcessAllAvailable(),

		// --- batch 2 ------------------------
		// check results of batch 1
		//CheckAnswer((animalsBatch0 ++ animalsBatch1):_*),
		CheckIncrementalExecutionCurrentBatchId(1),
		AdvanceManualClock(FIVE_MILLISEC),
		AdvanceManualClock(FIVE_MILLISEC),

		// Check the results of batch 1 again; this is to make sure that, when there's no new data,
		// the currentId does not get logged (e.g. as 2) even if the clock has advanced many times
		//CheckAnswer((animalsBatch0 ++ animalsBatch1):_*),
		CheckIncrementalExecutionCurrentBatchId(1),
		// Add data in batch 2
		AddData(animalMemoryStream, animalsBatch2:_*),
		AdvanceManualClock(FIVE_MILLISEC),
		//ProcessAllAvailable(),


		// --- batch 3 ------------------------
		// check results of batch 2
		//CheckAnswer((animalsBatch0 ++ animalsBatch1 ++ animalsBatch2):_*),
		CheckIncrementalExecutionCurrentBatchId(2),
		// Add data in batch 3
		AddData(animalMemoryStream, animalsBatch3:_*),
		AdvanceManualClock(FIVE_MILLISEC),
		//ProcessAllAvailable(),


		StopStream
	)
	writeQuery.processAllAvailable() // TODO how to show outputs of above like this line does?


	// Append + watermark --> prints all batches + BUT does not sum at end
	// Complete + watermark (even 50 sec) --> prints all batches + only sums last batch
	// Complete + NO watermark --> prints all batches + only sums last batch


	// Step 3 - Defining window on event time
	// Defining window which aggregates the aminal type value for the last 10 seconds
	// Meaning: partitioning (animal type ) in groups of 10 seconds
	val windowedCount: DataFrame = animalDS
		//.withWatermark("timeSeen", "5 seconds")
		.groupBy(
			window(timeColumn = $"timeSeen", windowDuration = toWord(FIVE_SEC)), // group by time
			$"animal" // group by animal type
		)
		.sum(colNames = "howMany")


	val dataStreamWriter: DataStreamWriter[Row] = windowedCount/*animalDS.toDF()*/.writeStream
		.trigger(triggerProcFiveSeconds)
		.format(source = "console")
		.option(key = "truncate", value = false)
		.outputMode(OutputMode.Complete())

	dataStreamWriter.start().awaitTermination(120000) // 120,000 ms == 120 s == 2 min


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
