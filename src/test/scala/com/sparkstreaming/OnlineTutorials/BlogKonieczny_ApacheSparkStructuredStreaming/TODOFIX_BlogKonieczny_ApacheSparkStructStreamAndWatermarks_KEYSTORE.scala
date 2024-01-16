package com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming

import com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming.utilStore.{InMemoryKeyedStore, NoopForeachWriter}
import org.apache.spark.sql.{AnalysisException, Column, ColumnName, DataFrame, Dataset, ForeachWriter, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
import com.SparkSessionForTests
import com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming.utilStore.InMemoryKeyedStore.WindowToOccurrencesMap
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
import utils.StreamingUtils


/**
 * SOURCE (blog) = https://www.waitingforcode.com/apache-spark-structured-streaming/apache-spark-structured-streaming-watermarks/read#watermark_api
 *
 * SOURCE (code) = https://github.com/bartosz25/spark-scala-playground/blob/d4dae02098169e9f4241f3597dc4864421237881/src/test/scala/com/waitingforcode/structuredstreaming/WatermarkTest.scala#L22
 *
 *
 *
 * GOAL: get list of letters and counts next to each other but as timestamp windows come in - so not all the 0-2 windows together but instead have 0-2, 4-6, 0-2 as they come in
 *
 * scala> df.groupBy("Name").agg(collect_list("Letter").alias("lst")).withColumn("len", size($"lst")).show
 * +----+---------------+---+
 * |Name|            lst|len|
 * +----+---------------+---+
 * |Kate|      [A, B, J]|  3|
 * |Mary|         [A, E]|  2|
 * |John|[A, B, C, E, H]|  5|
 * +----+---------------+---+
 */

class TODOFIX_BlogKonieczny_ApacheSparkStructStreamAndWatermarks_KEYSTORE extends AnyFlatSpec with Matchers  with SparkSessionForTests{


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
	val batch2: Seq[(Time, Letter)] = Seq(
		(new Timestamp(TIME_OUT_OF_WATERMARK), "b2"),
		(new Timestamp(TIME_OUT_OF_WATERMARK), "b3"),
		(new Timestamp(TIME_OUT_OF_WATERMARK), "b4"),
		(new Timestamp(NOW), "a3")
	)


	// NOTE: correctness issue
	//
	/*Detected pattern of possible 'correctness
	'issue due to global watermark.The query contains stateful operation which can emit rows older than the current watermark plus allowed late record delay
	, which are "late rows" in downstream stateful operations and these rows can be discarded.Please refer the programming guide doc
	for more details
	.If you understand the possible risk of correctness issue and still need to run the query
	, you can disable
	this check by setting the config `spark.sql.streaming.statefulOperator.checkCorrectness.enabled` to false.;*/

	"watermark" should "discard late data and accept 1 late but within watermark with window aggregation" in {

		val TIME_WATERMARK: Duration = Seconds(10)
		val TIME_WINDOW: Duration = Seconds(2)

		val TEST_KEY: String = "watermark-window-test"
		val FORMAT_REAL_LETTER: String = "-real-letter"
		val FORMAT_REAL_CNT: String = "-real-cnt"
		val FORMAT_STR_LETTER: String = "-str-letter"
		val FORMAT_STR_CNT: String = "-str-cnt"

		// val inputStream = MemoryStream[(Time, Letter, Int)]


		val inputStream = MemoryStream[(Time, Letter)]

		val tempW = Window.partitionBy("new_col").orderBy(lit("A"))

		case class SoFarType(timestamp: Timestamp, window: Window, letter: Letter, newCol: String)



		val sourcedf = inputStream.toDS().toDF("timestamp", "letter")
			.withWatermark("timestamp", toWord(TIME_WATERMARK))
			.withColumn("window", window($"timestamp", toWord(TIME_WINDOW)))
			//.withColumn("new_col", lit("ABC"))
			.groupBy("window")
			.agg(collect_list("letter").alias("lst"))
			.withColumn("len", sqlSize($"lst"))
			//.as[SoFarType]

			//.withColumn("row_num", row_number().over(tempW)).drop("new_col")
			//.as[Row]

		/*val foreachwriter = new ForeachWriter[SoFarType] {
			override def open(partitionId: Long, version: Long): Boolean = true

			override def process(sofartype: SoFarType): Unit = {
				// sofartype.
				println(s"window row: $sofartype")
				println(sofartype.schema)

				val timeWindow: IntervalWindow = StreamingUtils.parseWindow(sofartype.get(0).toString)
				val cnt: Count = sofartype.getAs[Count]("count(1)") // TODO figure out why not same as results in test


				val rowReprCnt: String = s"${timeWindow.toString} -> ${cnt}"
				InMemoryKeyedStore.addValue(TEST_KEY + FORMAT_STR_CNT, rowReprCnt)
				InMemoryKeyedStore.addValueC(TEST_KEY + FORMAT_REAL_CNT, (timeWindow, cnt))
			}

			override def close(errorOrNull: Throwable): Unit = {}
		}*/

		/*val saveWithWindowFunction = (sourceDf: DataFrame, batchId: Long) => {
			val tempW = Window.partitionBy("new_col").orderBy(lit("A"))

			sourceDf.as
				.withColumn("row_num", row_number().over(tempW)).drop("new_col")
				//.orderBy()
				.groupBy("row_num", "window")
				//.groupBy(window($"timestamp", toWord(TIME_WINDOW)))
				.agg(collect_list("letter").alias("letterlist"))

			//... save the dataframe using: sourceDf.write.save()
		}*/


		sourcedf.printSchema



		val dw: DataStreamWriter[Row/*SoFarType*/] = sourcedf.writeStream
			.outputMode(OutputMode.Update())
			.option("truncate", false)
			.format("console")
			//.foreach()
			//.foreachBatch(saveWithWindowFunction)
			//.foreach(foreachwriter)
		val qs: StreamingQuery = dw.start()

		inputStream.addData(batch1)
		qs.processAllAvailable()
		inputStream.addData(batch2)
		qs.processAllAvailable()


		qs.awaitTermination()



		// ------------------------

		/*val aggregatedStreamCount = inputStream.toDS().toDF("timeCreated", "letterName")
			.withWatermark("timeCreated", toWord(TIME_WATERMARK))
			.groupBy($"letterName", window($"timeCreated", toWord(TIME_WINDOW)))
			//.withColumn("letterName", $"letterName")
			//.withColumn("window", window($"timeCreated", toWord(TIME_WINDOW)))
			//.withColumn("count", count("window"))

			.agg(count("*")).agg(sum("count(1)"))

		aggregatedStreamCount.printSchema*/
			//.count() // count, agg(count), agg(count over w), withcol (count over w)
			//.groupBy($"window", $"letterName")
			//.withColumn("occs", count("window").over(w))
			//.agg(count("window").over(w))
			//.count() //.withColumn("occurrences", sum(col("count")))

			//.agg(count("*"))
			//.count()
			//.withColumn("numLetterOccurrences", sum("count").over(w))
			/*.withColumn("window", window($"timeCreated", toWord(TIME_WINDOW)))
			.groupBy("window", "letterName")
			.count()*/
			//.groupBy(window($"timeCreated", toWord(TIME_WINDOW)), $"letterName")
			//.agg(count($"letterName").as("count"))
		// TODO  cumulative sum?
			/*.groupBy(window($"timeCreated", toWord(TIME_WINDOW)))
			.count()*/



		// ----------------------------------


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
