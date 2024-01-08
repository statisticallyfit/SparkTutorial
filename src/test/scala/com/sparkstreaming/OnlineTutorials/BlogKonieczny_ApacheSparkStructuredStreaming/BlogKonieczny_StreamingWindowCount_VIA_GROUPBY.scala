package com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming


import org.apache.spark.sql.{AnalysisException, Column, ColumnName, DataFrame, Dataset, ForeachWriter, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.streaming.{Duration, Seconds}

import java.sql.Timestamp

import com.sparkstreaming.OnlineTutorials.TimeConsts._

import com.SparkSessionForTests
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._

/**
 * This is the same source code as this blog below
 *
 * ---> SOURCE (blog) = https://www.waitingforcode.com/apache-spark-structured-streaming/apache-spark-structured-streaming-watermarks/read#watermark_api
 *
 * ---> SOURCE (code) = https://github.com/bartosz25/spark-scala-playground/blob/d4dae02098169e9f4241f3597dc4864421237881/src/test/scala/com/waitingforcode/structuredstreaming/WatermarkTest.scala#L22
 *
 * but is adapted to not use a InMemoryKeyStore / foreachwriter, instead just memory stream
 * in order to do streaming count of elements within partitioned windows
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
 *
 * // TODO - can do this using window partition instead of groupby?
 *
 *
 * Follow-up blogs (other methods):
 *
 * ---> Overall method: groupby methodof counting arrivals within window
 * ------> https://medium.com/@evermanisha/peering-through-the-window-of-structured-spark-streaming-d45da63d5821
 * ------> https://dvirgiln.github.io/windowing-using-spark-structured-streaming/
 * ------> https://community.cloudera.com/t5/Support-Questions/Spark-Streaming-Aggregation-On-last-5-Minutes-complete-data/td-p/376120
 * ------> https://itecnote.com/tecnote/apache-spark-how-to-use-lag-and-rangebetween-functions-on-timestamp-values/
 *
 * ---> Overall method: (windowspec + rownum way of grouping) adding row number to df then grouping by row num then summing somehow the counts that way???
 *
 * ---> resource: foreach writer = https://stackoverflow.com/questions/46147095/how-to-count-items-per-time-window
 *      (methodology: to take in input timestamp, letterlist and output the count...)
 *      (question: but then how to output the entire df / row/??)
 *
 * ---> resource: foreachBatch = https://stackoverflow.com/questions/63490147/adding-a-row-number-column-to-a-streaming-dataframe
 * ---> resource: stub-column-way of adding row-num using window partition = https://stackoverflow.com/questions/53082891/adding-a-unique-consecutive-row-number-to-dataframe-in-pyspark
 *
 * ---> Overall Method: window partition (instead of groupby) = find out how to use windowspec partition instead of groupby in order to find the count of letters per time window (arrivals)
 *
 * // TODO just categorize these in the wiki (the above under "misc: stackoverflow")
 *
 */
class BlogKonieczny_StreamingWindowCount_VIA_GROUPBY extends AnyFunSpec with Matchers  with SparkSessionForTests {


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


	describe("adding elements in windows and counting their occurrences") {
		it("should show the amount of elements per window as a function of time window and batch"){

			val TIME_WATERMARK: Duration = Seconds(10)
			val TIME_WINDOW: Duration = Seconds(2)


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


			sourcedf.printSchema


			val dw: DataStreamWriter[Row /*SoFarType*/ ] = sourcedf.writeStream
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


			// println(s"df: ${sourcedf.collect()} ") // TODO how to get dataframe results?

			qs.awaitTermination(Seconds(30).milliseconds)





			// TODO now check the online blogs to figure out how to test the memory stream for these parameters:

			/*readValues should have size 3
			readValues should contain allOf(
				"[1970-01-01 02:00:00.0,1970-01-01 02:00:02.0] -> 1",
				"[1970-01-01 02:00:04.0,1970-01-01 02:00:06.0] -> 2",
				"[1970-01-01 02:00:04.0,1970-01-01 02:00:06.0] -> 3"
			)*/

		}
	}
}
