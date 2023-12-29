package com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming

import com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming.util.{InMemoryKeyedStore, NoopForeachWriter}
import org.apache.spark.sql.{AnalysisException, Column, ColumnName, DataFrame, Dataset, ForeachWriter, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import com.SparkSessionForTests
import org.scalatest.TestSuite

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
//import org.scalatest.funspec.AnyFunSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.Assertions._

import java.sql.Timestamp // intercept

import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, Trigger}


import com.sparkstreaming.OnlineTutorials.TimeConsts._

/**
 * SOURCE = https://www.waitingforcode.com/apache-spark-structured-streaming/apache-spark-structured-streaming-watermarks/read#watermark_api
 */

object BlogKonieczny_ApacheSparkStructStreamAndWatermarks extends AnyFlatSpec with Matchers  with SparkSessionForTests{

	import sparkTestsSession.implicits._
	implicit val sparkContext: SQLContext = sparkTestsSession.sqlContext

	val NOW = 5000L
	val TIME_OUT_OF_WATERMARK = 1000L

	val batch1: Seq[(Timestamp, String)] = Seq(
		(new Timestamp(NOW), "a1"),
		(new Timestamp(NOW), "a2"),
		(new Timestamp(NOW - 4000L), "b1")
	)
	val batch2: Seq[(Timestamp, String)] = Seq(
		(new Timestamp(TIME_OUT_OF_WATERMARK), "b2"),
		(new Timestamp(TIME_OUT_OF_WATERMARK), "b3"),
		(new Timestamp(TIME_OUT_OF_WATERMARK), "b4"),
		(new Timestamp(NOW), "a3")
	)



	"watermark" should "discard late data and accept 1 late but within watermark with window aggregation" in {

		val TEST_KEY = "watermark-window-test"

		val numPartitions: Int = 10 // TODO how many???
		val inputStream: MemoryStream[(Timestamp, String)] = new MemoryStream[(Timestamp, String)](id = 1, sqlContext = sparkContext, numPartitions = numPartitions)

		val aggregatedStream = inputStream.toDS().toDF("timeCreated", "name")
			.withWatermark(eventTime = "timeCreated", delayThreshold = toWord(Seconds(2)))
			.groupBy(window($"timeCreated", toWord(Seconds(2))))
			.count() // TODO understand what this does


		val dataStreamWriter: DataStreamWriter[Row] = aggregatedStream.writeStream
			.outputMode(OutputMode.Update())
			.foreach(new ForeachWriter[Row](){
				def open(partitionId: Long, epochId: Long): Boolean = true

				def process(processedRow: Row): Unit = {
					val window = processedRow.get(0)
					val rowRepresentation = s"${window.toString} -> ${processedRow.getAs[Long]("count")}"
					InMemoryKeyedStore.addValue(TEST_KEY, rowRepresentation)
				}

				def close(errorOrNull: Throwable): Unit = {}
			})

		val query: StreamingQuery = dataStreamWriter.start()


		// Create event sequence using thread
		val mainEventsThread: Thread = new Thread(new Runnable() {
			def run(): Unit = {
				inputStream.addData(batch1)

				while(!query.isActive){
					// wait the query to activate
				}
				// The watermark is now computed as: MAX(eventTime) - watermark
				// EX: 5000 - 2000 = 3000
				// Thus among the values sent above only "a6" should be accepted because it's within the watermark
				// TODO see reality... there is no a6

				Thread.sleep(7000) // TODO compare to spark's AdvanceManualClock thingy

				inputStream.addData(batch2)

				inputStream.addData()
			}
		})

		mainEventsThread.start()

		query.awaitTermination(Seconds(25).milliseconds)

		val readValues: ListBuffer[String] = InMemoryKeyedStore.getValues(key = TEST_KEY)

		/**
		 * NOTE: As you can notice, the count for the window 0-2 wasn't updated with 3 fields (b2, b3 and b4) because they fall
		 * before the watermark
		 * Please see how this behavior changes in the next test where the watermark is defined to 10 seconds
		 */
		println(s"All data = ${readValues}")


		readValues should have size 3
		readValues should contain allOf(
			"[1970-01-01 01:00:00.0,1970-01-01 01:00:02.0] -> 1",
			"[1970-01-01 01:00:04.0,1970-01-01 01:00:06.0] -> 2",
			"[1970-01-01 01:00:04.0,1970-01-01 01:00:06.0] -> 3"
		)
	}
}
