package com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming

import com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming.util.{InMemoryKeyedStore, NoopForeachWriter}
import org.apache.spark.sql.{AnalysisException, Column, ColumnName, DataFrame, Dataset, ForeachWriter, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import com.SparkSessionForTests
import org.apache.spark.streaming.Duration
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

class BlogKonieczny_ApacheSparkStructStreamAndWatermarks extends AnyFlatSpec with Matchers  with SparkSessionForTests{

	import sparkTestsSession.implicits._
	implicit val sparkContext: SQLContext = sparkTestsSession.sqlContext


	val NOW = Seconds(5).milliseconds // 5000L
	val TIME_OUT_OF_WATERMARK = Seconds(1).milliseconds // 1000L

	val batch1: Seq[(Timestamp, String)] = Seq(
		(new Timestamp(NOW), "a1"),
		(new Timestamp(NOW), "a2"),
		(new Timestamp(NOW - Seconds(4).milliseconds), "b1")
		// (new Timestamp(NOW - 4000L), "b1")
	)
	val batch2: Seq[(Timestamp, String)] = Seq(
		(new Timestamp(TIME_OUT_OF_WATERMARK), "b2"),
		(new Timestamp(TIME_OUT_OF_WATERMARK), "b3"),
		(new Timestamp(TIME_OUT_OF_WATERMARK), "b4"),
		(new Timestamp(NOW), "a3")
	)



	"watermark" should "discard late data and accept 1 late but within watermark with window aggregation" in {

		val TIME_WATERMARK: Duration = Seconds(2)
		val TIME_WINDOW: Duration = Seconds(2)

		val TEST_KEY: String = "watermark-window-test"

		val inputStream: MemoryStream[(Timestamp, String)] = new MemoryStream[(Timestamp, String)](id = 1, sqlContext = sparkContext)

		val aggregatedStream: DataFrame = inputStream.toDS().toDF("timeCreated", "letterName")
			.withWatermark(eventTime = "timeCreated", delayThreshold = toWord(TIME_WATERMARK))
			.groupBy(window($"timeCreated", toWord(TIME_WINDOW)))
			.count()

		val aggregatedStreamWithName: DataFrame = inputStream.toDS().toDF("timeCreated", "letterName")
			.withWatermark(eventTime = "timeCreated", delayThreshold = toWord(TIME_WATERMARK))
			.groupBy(window($"timeCreated", toWord(TIME_WINDOW)), $"letterName")
			.count()
			// .count()
			// .withColumn(colName = "theLetterName", count($"letterName"))/
			//.agg("$letterName", count("letterName"))
			// .count() // TODO understand what the count does

			//.withColumn(colName = "letterName")


		// NOTE; declaring Map as var makes it mutable
		var letterFreqs: Map[String, Seq[String]] = Map()
		var cnts: Map[String, Seq[Long]] = Map()

		val dataStreamWriterWithName: DataStreamWriter[Row] = aggregatedStreamWithName.writeStream
			.outputMode(OutputMode.Update())
			.foreach(new ForeachWriter[Row](){
				override def open(partitionId: Long, epochId: Long): Boolean = true

				override def process(processedRow: Row): Unit = {
					println(s"window row: $processedRow")
					println(processedRow.schema)


					val timeWindow: String = processedRow.get(0).toString //As[String]("window")
					//val timeWindow: Timestamp = Timestamp.valueOf(processedRow.get(0).asInstanceOf[Window].leftSide.toString) //.asInstanceOf[Timestamp] //As[Timestamp]("timeCreated")// first part is timestamp, second part is count
					//val timeWindow: Window = processedRow.getAs[Window]("window") //.get(0).asInstanceOf[Window] // As[Window]("timeCreated")
					//val timeWindow: Window = processedRow.get(0).asInstanceOf[Window]

					val letter: String = processedRow.getAs[String]("letterName")
					val cnt: Long = processedRow.getAs[Long]("count")

					// Adding to this timestamp, the letter that appears (adding the letters as list)
					letterFreqs.isDefinedAt(timeWindow) match {
						case true => {
							val oldElem: (String, Seq[String]) = (timeWindow -> List(letter))
							val newElem: (String, Seq[String]) = (timeWindow -> (letterFreqs.get(timeWindow).get :+ letter))

							letterFreqs -= oldElem._1
							letterFreqs += newElem
						}

						case false => {
							val firstElem: (String, Seq[String]) = (timeWindow -> List(letter))
							letterFreqs += firstElem
						}
					}

					val rowRepresentation: String = s"${timeWindow.toString} -> ${letter} : ${cnt}"
					InMemoryKeyedStore.addValue(TEST_KEY + "-NAME", rowRepresentation)


					InMemoryKeyedStore.addValue(TEST_KEY + "-NAME-seq", letterFreqs.mkString(", "))
				}


				override def close(errorOrNull: Throwable): Unit = {}
			})


		val dataStreamWriter: DataStreamWriter[Row] = aggregatedStream.writeStream
			.outputMode(OutputMode.Update())
			.foreach(new ForeachWriter[Row]() {
				override def open(partitionId: Long, epochId: Long): Boolean = true

				override def process(processedRow: Row): Unit = {
					println(s"window row: $processedRow")
					println(processedRow.schema)

					//val timeWindow: Window = processedRow.get(0).asInstanceOf[Window]// getAs[Timestamp]("timeCreated")// first part is timestamp, second part is count
					//val timeWindow: String = processedRow.getAs[String]("window")
					val timeWindow: String = processedRow.get(0).toString
					val cnt: Long = processedRow.getAs[Long]("count")

					// Adding to this timestamp, the count that appears
					// Adding to this timestamp, the letter that appears (adding the letters as list)
					cnts.isDefinedAt(timeWindow) match {
						case true => {
							val oldElem: (String, Seq[Long]) = (timeWindow -> List(cnt))
							val newElem: (String, Seq[Long]) = (timeWindow -> (cnts.get(timeWindow).get :+ cnt))

							cnts -= oldElem._1 // TODO understand why replacement not happening, why have to remove manually old element
							cnts += newElem
						}

						case false => {
							val firstElem: (String, Seq[Long]) = (timeWindow -> List(cnt))
							cnts += firstElem
						}
					}


					val rowRepresentation: String = s"${timeWindow.toString} -> ${cnt}"
					InMemoryKeyedStore.addValue(TEST_KEY, rowRepresentation)
					InMemoryKeyedStore.addValue(TEST_KEY + "-seq", cnts.mkString(", "))
				}

				override def close(errorOrNull: Throwable): Unit = {}
			})
		val queryName: StreamingQuery = dataStreamWriterWithName.start()
		val query: StreamingQuery = dataStreamWriter.start()



		// Create event sequence using thread
		val threadOfStreamingData: Thread = new Thread(new Runnable() {
			override def run(): Unit = {
				inputStream.addData(batch1)


				while(!(query.isActive && queryName.isActive)){
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

		queryName.awaitTermination(Seconds(25).milliseconds)
		query.awaitTermination(Seconds(25).milliseconds)

		val readValues: ListBuffer[String] = InMemoryKeyedStore.getValues(key = TEST_KEY + "-seq")
		val readValuesName: ListBuffer[String] = InMemoryKeyedStore.getValues(key = TEST_KEY + "-NAME-seq")

		/**
		 * NOTE: As you can notice, the count for the window 0-2 wasn't updated with 3 fields (b2, b3 and b4) because they fall
		 * before the watermark
		 * Please see how this behavior changes in the next test where the watermark is defined to 10 seconds
		 */
		println(s"All data = ${readValues}")
		println(s"All data = ${readValuesName}")


		/*readValues should have size 3
		readValues should contain allOf(
			"[1970-01-01 02:00:00.0,1970-01-01 02:00:02.0] -> 1",
			"[1970-01-01 02:00:04.0,1970-01-01 02:00:06.0] -> 2",
			"[1970-01-01 02:00:04.0,1970-01-01 02:00:06.0] -> 3"
		)*/
	}


	"late data but within watermark" should "be aggregated in correct windows" in {


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

		val readValues: ListBuffer[String] = InMemoryKeyedStore.getValues(key = TEST_KEY)

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
	}
}
