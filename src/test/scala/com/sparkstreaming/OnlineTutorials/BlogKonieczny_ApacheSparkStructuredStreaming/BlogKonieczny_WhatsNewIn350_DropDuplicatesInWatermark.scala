package com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming

import utilities.SparkSessionWrapper

import com.sparkstreaming.OnlineTutorials.TimeConsts._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

import java.sql.Timestamp

/**
 * SOURCE:
 * 	- blog = https://www.waitingforcode.com/apache-spark-structured-streaming/what-new-apache-spark-3.5.0-structured-streaming/read
 * 	- code = https://github.com/bartosz25/spark-playground/blob/master/spark-3.5.0-features/structured_streaming/src/main/scala/com/waitingforcode/DropDuplicatesWithinWatermark.scala
 */
object BlogKonieczny_WhatsNewIn350_DropDuplicatesInWatermark extends SparkSessionWrapper with App {

	//val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

	import sparkSessionWrapper.implicits._

	implicit val sparkContext: SQLContext = sparkSessionWrapper.sqlContext



	// --------------------------------------------------------------------------------------------

	case class Event(id: Int, eventTime: Timestamp)


	val memoryStream: MemoryStream[Event] = MemoryStream[Event]


	val eventDS: Dataset[Event] = memoryStream.toDS()
		.withWatermark(eventTime = "eventTime", delayThreshold = toWord(TWENTY_SEC))
		//.dropDuplicatesWithinWatermark(col1 = "id")
	// REPL
	// val query: Dataset[Event] = memoryStream.toDS().withWatermark(eventTime = "eventTime", delayThreshold = "20 seconds").dropDuplicatesWithinWatermark(col1 = "id")

	val writeQuery: StreamingQuery = eventDS.writeStream.format(source = "console").option(key = "truncate", value = false).start()

	// Batch 0 ---------------------------------------------------------------------------------
	memoryStream.addData(
		Seq(
			Event(id = 1, eventTime = Timestamp.valueOf("2023-06-10 10:20:40")),
			Event(id = 1, eventTime = Timestamp.valueOf("2023-06-10 10:20:30")),
			Event(id = 2, eventTime = Timestamp.valueOf("2023-06-10 10:20:50")),
			Event(id = 3, eventTime = Timestamp.valueOf("2023-06-10 10:20:45")),
			Event(id = 0, eventTime = Timestamp.valueOf("2023-06-10 10:20:55")), //out of 20 sec watermark?
		)
	)
	// RESULT
	/**
	 * Batch: 0
	 * -------------------------------------------
	 * +---+-------------------+
	 * |id |eventTime          |
	 * +---+-------------------+
	 * |1  |2023-06-10 10:20:40|
	 * |3  |2023-06-10 10:20:45|
	 * |2  |2023-06-10 10:20:50|
	 * +---+-------------------+
	 *
	 *
	 * Batch: 1
	 * -------------------------------------------
	 * +---+---------+
	 * |id |eventTime|
	 * +---+---------+
	 * +---+---------+
	 *
	 */

	writeQuery.processAllAvailable()
	val json0: String = writeQuery.lastProgress.prettyJson // returns most recent StreamQueryProgress of this query

	/*assert(json0 ==
		"""
		  |{
		  |  "id" : "bb6f28d1-ae2f-4419-bb79-a03f1d53cad4",
		  |  "runId" : "bfc6c9c7-1987-42ef-93aa-c5be188c3056",
		  |  "name" : null,
		  |  "timestamp" : "2023-12-20T09:58:04.564Z",
		  |  "batchId" : 2,
		  |  "numInputRows" : 0,
		  |  "inputRowsPerSecond" : 0.0,
		  |  "processedRowsPerSecond" : 0.0,
		  |  "durationMs" : {
		  |    "latestOffset" : 0,
		  |    "triggerExecution" : 0
		  |  },
		  |  "eventTime" : {
		  |    "watermark" : "2023-06-10T07:20:30.000Z"
		  |  },
		  |  "stateOperators" : [ {
		  |    "operatorName" : "dedupeWithinWatermark",
		  |    "numRowsTotal" : 3,
		  |    "numRowsUpdated" : 0,
		  |    "allUpdatesTimeMs" : 23,
		  |    "numRowsRemoved" : 0,
		  |    "allRemovalsTimeMs" : 1,
		  |    "commitTimeMs" : 27315,
		  |    "memoryUsedBytes" : 85664,
		  |    "numRowsDroppedByWatermark" : 0,
		  |    "numShufflePartitions" : 200,
		  |    "numStateStoreInstances" : 200,
		  |    "customMetrics" : {
		  |      "loadedMapCacheHitCount" : 200,
		  |      "loadedMapCacheMissCount" : 0,
		  |      "numDroppedDuplicateRows" : 0,
		  |      "stateOnCurrentVersionSizeBytes" : 24528
		  |    }
		  |  } ],
		  |  "sources" : [ {
		  |    "description" : "MemoryStream[id#2,eventTime#3]",
		  |    "startOffset" : 0,
		  |    "endOffset" : 0,
		  |    "latestOffset" : 0,
		  |    "numInputRows" : 0,
		  |    "inputRowsPerSecond" : 0.0,
		  |    "processedRowsPerSecond" : 0.0
		  |  } ],
		  |  "sink" : {
		  |    "description" : "org.apache.spark.sql.execution.streaming.ConsoleTable$@31a46479",
		  |    "numOutputRows" : 0
		  |  }
		  |}""".stripMargin, "Json batch 0")*/


	// Batch 1 ---------------------------------------------------------------------------------
	memoryStream.addData(
		Seq(
			Event(id = 1, eventTime = Timestamp.valueOf("2023-06-10 10:22:40")),
			Event(id = 1, eventTime = Timestamp.valueOf("2023-06-10 10:20:10")),
			Event(id = 4, eventTime = Timestamp.valueOf("2023-06-10 10:21:50")),
			Event(id = 5, eventTime = Timestamp.valueOf("2023-06-10 10:21:45")),
		)
	)

	/** RESULT
	 *
	 * Batch: 2
	 * -------------------------------------------
	 * +---+-------------------+
	 * |id |eventTime          |
	 * +---+-------------------+
	 * |5  |2023-06-10 10:21:45|
	 * |4  |2023-06-10 10:21:50|
	 * +---+-------------------+
	 *
	 * -------------------------------------------
	 * Batch: 3
	 * -------------------------------------------
	 * +---+---------+
	 * |id |eventTime|
	 * +---+---------+
	 * +---+---------+
	 */


	writeQuery.processAllAvailable()
	// NOTE only prints whole output if writing println(json2)
	val json2 = writeQuery.lastProgress.prettyJson // returns most recent StreamQueryProgress of this query

	/*assert(json2 ==
		"""
		  |{
		  |  "id" : "bb6f28d1-ae2f-4419-bb79-a03f1d53cad4",
		  |  "runId" : "bfc6c9c7-1987-42ef-93aa-c5be188c3056",
		  |  "name" : null,
		  |  "timestamp" : "2023-12-20T10:03:21.665Z",
		  |  "batchId" : 4,
		  |  "numInputRows" : 0,
		  |  "inputRowsPerSecond" : 0.0,
		  |  "processedRowsPerSecond" : 0.0,
		  |  "durationMs" : {
		  |    "latestOffset" : 0,
		  |    "triggerExecution" : 0
		  |  },
		  |  "eventTime" : {
		  |    "watermark" : "2023-06-10T07:22:20.000Z"
		  |  },
		  |  "stateOperators" : [ {
		  |    "operatorName" : "dedupeWithinWatermark",
		  |    "numRowsTotal" : 0,
		  |    "numRowsUpdated" : 0,
		  |    "allUpdatesTimeMs" : 3,
		  |    "numRowsRemoved" : 5,
		  |    "allRemovalsTimeMs" : 249,
		  |    "commitTimeMs" : 44191,
		  |    "memoryUsedBytes" : 90480,
		  |    "numRowsDroppedByWatermark" : 0,
		  |    "numShufflePartitions" : 200,
		  |    "numStateStoreInstances" : 200,
		  |    "customMetrics" : {
		  |      "loadedMapCacheHitCount" : 600,
		  |      "loadedMapCacheMissCount" : 0,
		  |      "numDroppedDuplicateRows" : 0,
		  |      "stateOnCurrentVersionSizeBytes" : 24000
		  |    }
		  |  } ],
		  |  "sources" : [ {
		  |    "description" : "MemoryStream[id#2,eventTime#3]",
		  |    "startOffset" : 1,
		  |    "endOffset" : 1,
		  |    "latestOffset" : 1,
		  |    "numInputRows" : 0,
		  |    "inputRowsPerSecond" : 0.0,
		  |    "processedRowsPerSecond" : 0.0
		  |  } ],
		  |  "sink" : {
		  |    "description" : "org.apache.spark.sql.execution.streaming.ConsoleTable$@31a46479",
		  |    "numOutputRows" : 0
		  |  }
		  |}""".stripMargin)*/



	// Batch 2 ---------------------------------------------------------------------------------
	memoryStream.addData(
		Seq(
			Event(id = 1, eventTime = Timestamp.valueOf("2023-06-10 10:24:40"))
		)
	)

	/** RESULT
	 * -------------------------------------------
	 * Batch: 4
	 * -------------------------------------------
	 * +---+-------------------+
	 * |id |eventTime          |
	 * +---+-------------------+
	 * |1  |2023-06-10 10:24:40|
	 * +---+-------------------+
	 *
	 * -------------------------------------------
	 * Batch: 5
	 * -------------------------------------------
	 * +---+---------+
	 * |id |eventTime|
	 * +---+---------+
	 * +---+---------+
	 */

	writeQuery.processAllAvailable()
	println(writeQuery.lastProgress.prettyJson) // returns most recent StreamQueryProgress of this query
}
