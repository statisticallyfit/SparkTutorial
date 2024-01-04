package com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming

import com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming.util.{InMemoryKeyedStore, NoopForeachWriter}
import org.apache.spark.sql.{AnalysisException, Column, ColumnName, DataFrame, Dataset, ForeachWriter, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
import com.SparkSessionForTests
import com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming.util.InMemoryKeyedStore.WindowToOccurrencesMap
//import org.scalatest.funspec.AnyFunSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.Assertions._


import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, Trigger}

import org.apache.spark.streaming.{Duration, Seconds}
import java.sql.Timestamp
import com.sparkstreaming.OnlineTutorials.TimeConsts._
import com.util.StreamingUtils
import com.util.StreamingUtils.IntervalWindow
import com.util.GeneralUtils

/**
 * SOURCE (blog) = https://www.waitingforcode.com/apache-spark-structured-streaming/apache-spark-structured-streaming-watermarks/read#watermark_api
 *
 * SOURCE (code) = https://github.com/bartosz25/spark-scala-playground/blob/d4dae02098169e9f4241f3597dc4864421237881/src/test/scala/com/waitingforcode/structuredstreaming/WatermarkTest.scala#L22
 */

class BlogKonieczny_ApacheSparkStructStreamAndWatermarks_INMEMORYKEYSTORE extends AnyFlatSpec with Matchers  with SparkSessionForTests{

	import sparkTestsSession.implicits._
	implicit val sparkContext: SQLContext = sparkTestsSession.sqlContext

	type Time = Timestamp
	type Letter = String
	type Count = Long

	val NOW = Seconds(5).milliseconds // 5000L
	val TIME_OUT_OF_WATERMARK = Seconds(1).milliseconds // 1000L
	val AFTER_SLEEP = Seconds(15).milliseconds

	val batch1: Seq[(Time, Letter)] = Seq(
		(new Timestamp(NOW), "a1"),
		(new Timestamp(NOW), "a2"),
		(new Timestamp(NOW - Seconds(4).milliseconds), "b1")
		// (new Timestamp(NOW - 4000L), "b1")
	)
	/*val batch2: Seq[(Time, Letter)] = Seq(
		(new Timestamp(AFTER_SLEEP), "b2"),
		(new Timestamp(AFTER_SLEEP), "b3"),
		(new Timestamp(AFTER_SLEEP), "b4"),
		(new Timestamp(NOW), "a3")
	)*/
	val batch2: Seq[(Time, Letter)] = Seq(
		(new Timestamp(TIME_OUT_OF_WATERMARK), "b2"),
		(new Timestamp(TIME_OUT_OF_WATERMARK), "b3"),
		(new Timestamp(TIME_OUT_OF_WATERMARK), "b4"),
		(new Timestamp(NOW), "a3")
	)



	"watermark" should "discard late data and accept 1 late but within watermark with window aggregation" in {

		val TIME_WATERMARK: Duration = Seconds(2)
		val TIME_WINDOW: Duration = Seconds(2)

		val TEST_KEY: String = "watermark-window-test"
		val FORMAT_REAL_LETTER: String = "-real-letter"
		val FORMAT_REAL_CNT: String = "-real-cnt"
		val FORMAT_STR_LETTER: String = "-str-letter"
		val FORMAT_STR_CNT: String = "-str-cnt"

		val inputStream: MemoryStream[(Time, Letter)] = new MemoryStream[(Time, Letter)](id = 1, sqlContext = sparkContext)


		val aggregatedStreamWithCount = inputStream.toDS().toDF("created", "name")
			.withWatermark("created", toWord(TIME_WATERMARK))
			.groupBy(window($"created", toWord(TIME_WINDOW)))
			.count()

		val aggregatedStreamWithName: DataFrame = inputStream.toDS().toDF("created", "letterName")
			.withWatermark(eventTime = "created", delayThreshold = toWord(TIME_WATERMARK))
			.groupBy(window($"created", toWord(TIME_WINDOW)), $"letterName")
			.count()

		val dataStreamWriterForCounts: DataStreamWriter[Row] = aggregatedStreamWithCount.writeStream
			.outputMode(OutputMode.Update())
			.foreach(new ForeachWriter[Row](){
				override def open(partitionId: Long, epochId: Long): Boolean = true

				override def process(processedRow: Row): Unit = {
					println(s"window row: $processedRow")
					println(processedRow.schema)

					val timeWindow: IntervalWindow = StreamingUtils.parseWindow(processedRow.get(0).toString)
					val cnt: Count = processedRow.getAs[Count]("count") // TODO figure out why not same as results in test


					val rowReprCnt: String = s"${timeWindow.toString} -> ${cnt}"
					InMemoryKeyedStore.addValue(TEST_KEY + FORMAT_STR_CNT, rowReprCnt)
					InMemoryKeyedStore.addValueC(TEST_KEY + FORMAT_REAL_CNT, (timeWindow, cnt))

				}

				override def close(errorOrNull: Throwable): Unit = {}
			})

		val dataStreamWriterForName: DataStreamWriter[Row] = aggregatedStreamWithName.writeStream
			.outputMode(OutputMode.Update())
			.foreach(new ForeachWriter[Row](){
				override def open(partitionId: Long, epochId: Long): Boolean = true

				override def process(processedRow: Row): Unit = {
					println(s"window row: $processedRow")
					println(processedRow.schema)

					val timeWindow: IntervalWindow = StreamingUtils.parseWindow(processedRow.get(0).toString)
					val letter: Letter = processedRow.getAs[Letter]("letterName")
					val cnt: Count = processedRow.getAs[Count]("count") // TODO figure out why not same as results in test


					val rowReprName: String = s"${timeWindow.toString} -> ${letter} : ${cnt}"
					InMemoryKeyedStore.addValue(TEST_KEY + FORMAT_STR_LETTER, rowReprName)
					InMemoryKeyedStore.addValueL(TEST_KEY + FORMAT_REAL_LETTER, (timeWindow, letter))

				}

				override def close(errorOrNull: Throwable): Unit = {}
			})

		val queryForCount = dataStreamWriterForCounts.start()
		val queryForLetter: StreamingQuery = dataStreamWriterForName.start()
		//val queryForCount: StreamingQuery = dataStreamWriterForCounts.start()
		println("START: DATASTREAMWRITER")


		// Create event sequence using thread
		val threadOfStreamingData: Thread = new Thread(new Runnable() {

			override def run(): Unit = {


				println("QUERY: NOT ACTIVE")
				while(!(queryForLetter.isActive && queryForCount.isActive)){
					// wait the query to activate // TODO instead use thread sleep?
				}
				println("QUERY: IS ACTIVE NOW")

				inputStream.addData(batch1)
				println("ADD DATA (1)")

				// The watermark is now computed as: MAX(eventTime) - watermark
				// EX: 5000 - 2000 = 3000
				// Thus among the values sent above only "a6" should be accepted because it's within the watermark
				// TODO see reality... there is no a6

				// TODO why after sleeping the thread does the memory stream get empty? (doesn't remember first batch)
				Thread.sleep(Seconds(7).milliseconds) // TODO compare to spark's AdvanceManualClock thingy
				println("SLEEEEEEEPING")

				inputStream.addData(batch2)
				println("ADD DATA (2)")
			}
		})
		threadOfStreamingData.start()
		println("START: THREAD")

		queryForCount.awaitTermination(Seconds(50).milliseconds)
		queryForLetter.awaitTermination(Seconds(50).milliseconds)
		//queryForCount.awaitTermination(Seconds(50).milliseconds)



		val elemsGrouped_L: Option[Map[IntervalWindow, Seq[Letter]]] = InMemoryKeyedStore.getElementsGrouped(key = TEST_KEY + FORMAT_REAL_LETTER)
		val elemArrivalsStr_L: Option[ListBuffer[String]] = InMemoryKeyedStore.getElementsAsArrivals_StrFormat(key = TEST_KEY + FORMAT_STR_LETTER)
		val elemArrivals_L: Option[ListBuffer[(IntervalWindow, List[Letter])]] = InMemoryKeyedStore.getElementsAsArrivals(key = TEST_KEY + FORMAT_REAL_LETTER)

		println(s"All data (elemsGrouped_L) = \n${elemsGrouped_L.get.mkString("\n")}")
		println(s"All data (elemArrivalsStr_L) = \n${elemArrivalsStr_L.get.mkString("\n")}")
		println(s"All data (elemArrivals_L) = \n${elemArrivals_L.get.mkString("\n")}")


		val elemsGrouped_C: Option[Map[IntervalWindow, Seq[Count]]] = InMemoryKeyedStore.getElementsGroupedC(key = TEST_KEY + FORMAT_REAL_CNT)
		val elemArrivalsStr_C: Option[ListBuffer[String]] = InMemoryKeyedStore.getElementsAsArrivals_StrFormat(key = TEST_KEY + FORMAT_STR_CNT)
		val elemArrivals_C: Option[ListBuffer[(IntervalWindow, Count)]] = InMemoryKeyedStore.getElementsAsArrivalsC(key = TEST_KEY + FORMAT_REAL_CNT)

		println(s"All data (elemsGrouped_C) = \n${elemsGrouped_C.get.mkString("\n")}")
		println(s"All data (elemArrivalsStr_C) = \n${elemArrivalsStr_C.get.mkString("\n")}")
		println(s"All data (elemArrivals_C) = \n${elemArrivals_C.get.mkString("\n")}")

		/*readValues should have size 3
		readValues should contain allOf(
			"[1970-01-01 02:00:00.0,1970-01-01 02:00:02.0] -> 1",
			"[1970-01-01 02:00:04.0,1970-01-01 02:00:06.0] -> 2",
			"[1970-01-01 02:00:04.0,1970-01-01 02:00:06.0] -> 3"
		)*/
	}


	/*"late data but within watermark" should "be aggregated in correct windows" in {


		val TIME_WATERMARK: Duration = Seconds(10)
		val TIME_WINDOW: Duration = Seconds(2)

		val TEST_KEY: String = "watermark-window-test-accepted-data"

		val inputStream: MemoryStream[(Timestamp, String)] = new MemoryStream[(Timestamp, String)](1, sqlContext = sparkContext)

		val aggregatedStream: DataFrame = inputStream.toDS().toDF("timeCreated", "letterName")
			.withWatermark("timeCreated", toWord(TIME_WATERMARK))
			.groupBy(window($"timeCreated", windowDuration = toWord(TIME_WINDOW)), $"letterName")
			.count()

		val dataStreamWriter: DataStreamWriter[Row] = aggregatedStream.writeStream
			.outputMode(OutputMode.Update())
			.foreach(new ForeachWriter[Row]() {
				override def open(partitionId: Long, epochId: Long): Boolean = true

				override def process(processedRow: Row): Unit = {
					val window: Any = processedRow.get(0)
					val rowRepresentation: String = s"${window.toString} -> ${processedRow.getAs[Long]("count")}"
					InMemoryKeyedStore.addValue(TEST_KEY, rowRepresentation)
				}

				override def close(errorOrNull: Throwable): Unit = {}
			})
		val query: StreamingQuery = dataStreamWriter.start()

		val threadOfStreamingData: Thread = new Thread(new Runnable() {

			override def run(): Unit = {

				inputStream.addData(batch1)

				while (!query.isActive) {
					// wait the query to activate // TODO instead use thread sleep?
				}
				// The watermark is now computed as: MAX(eventTime) - watermark
				// EX: 5000 - 2000 = 3000
				// Thus among the values sent above only "a6" should be accepted because it's within the watermark
				// TODO see reality... there is no a6


				Thread.sleep(Seconds(7).milliseconds) // TODO compare to spark's AdvanceManualClock thingy

				inputStream.addData(batch2)
			}
		})

		threadOfStreamingData.start()

		query.awaitTermination(Seconds(25).milliseconds)

		val readValues: ListBuffer[String] = InMemoryKeyedStore.getElementsGrouped(key = TEST_KEY)

		/**
		 * NOTE: here the count for the window 0-2 IS updated with 3 fields (b2, b3, b4) because they come after the watermark of 10 seconds.
		 */
		println(s"All data = ${readValues}")


		readValues should have size 4
		readValues should contain allOf(
			"[1970-01-01 02:00:00.0,1970-01-01 02:00:02.0] -> 1", // 0 - 2
			"[1970-01-01 02:00:00.0,1970-01-01 02:00:02.0] -> 4", // 0 - 2
			"[1970-01-01 02:00:04.0,1970-01-01 02:00:06.0] -> 2", // 4 - 6
			"[1970-01-01 02:00:04.0,1970-01-01 02:00:06.0] -> 3" // 4 - 6
		)
	}*/
}
