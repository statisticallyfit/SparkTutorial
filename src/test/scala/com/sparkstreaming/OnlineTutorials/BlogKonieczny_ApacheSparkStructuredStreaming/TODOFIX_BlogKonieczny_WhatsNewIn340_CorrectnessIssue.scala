package com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming



import com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming.utilStore.{InMemoryKeyedStore, NoopForeachWriter}
import org.apache.spark.sql.{AnalysisException, Column, ColumnName, DataFrame, Dataset, ForeachWriter, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import scala.collection.mutable.ListBuffer

import scala.reflect.runtime.universe._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.Assertions._
import utilities.SparkSessionWrapper

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, Trigger}

import org.apache.spark.streaming.{Duration, Seconds}
import java.sql.Timestamp
import com.sparkstreaming.OnlineTutorials.TimeConsts._
import utilities.{GeneralMainUtils, StreamingUtils}
import utilities.GeneralMainUtils.implicits._


/**
 * SOURCE blog = https://www.waitingforcode.com/apache-spark-structured-streaming/what-new-apache-spark-3.4.0-structured-streaming-correctness-issue/read
 *
 * SOURCE code = https://github.com/bartosz25/spark-playground/blob/master/spark-3.4.0-features/structured_streaming/src/main/scala/com/waitingforcode/CorrectnessIssueFixFor3_4_0.scala
 */
class TODOFIX_BlogKonieczny_WhatsNewIn340_CorrectnessIssue extends AnyFlatSpec with Matchers  with SparkSessionWrapper {

	import sparkSessionWrapper.implicits._
	implicit val sparkContext: SQLContext = sparkSessionWrapper.sqlContext


	type Time = Timestamp
	type Letter = String
	//val memoryStream: MemoryStream[Int] = MemoryStream[Int]
	val memoryStream: MemoryStream[(Time, Letter)] = MemoryStream[(Time, Letter)] //(id = 1, sqlContext = sparkContext)

	val query = memoryStream.toDS().toDF("event_time", "letterName")
		//.toDF.withColumn("event_time", $"value".cast(TimestampType))
		.withWatermark("event_time", "200 seconds")
		.withColumn("letterName", $"letterName")
		.groupBy(window($"event_time", "2 seconds").as("first_window"))
		.count()
		//.groupBy(window($"first_window", "5 seconds").as("second_window"))
		.agg(count("*"), sum("count").as("sum_of_counts"))

	val writeQuery: StreamingQuery = query.writeStream
		.outputMode(OutputMode.Complete())
		.format("console")
		.option("truncate", false)
		.start()



	val NOW = Seconds(5).milliseconds // 5000L
	val TIME_OUT_OF_WATERMARK = Seconds(1).milliseconds // 1000L
	val AFTER_SLEEP = Seconds(15).milliseconds

	val batch1: Seq[(Time, Letter)] = Seq(
		(new Timestamp(NOW*2), "a1"),
		(new Timestamp(NOW*2), "a2"),
		(new Timestamp(NOW*4) , "b1")
		// (new Timestamp(NOW - 4000L), "b1")
	)
	val batch2: Seq[(Time, Letter)] = Seq(
		(new Timestamp(NOW*5), "b2"),
		(new Timestamp(NOW*5), "b3"),
		(new Timestamp(NOW*5), "b4"),
		(new Timestamp(NOW*2), "a3")
	)

	val batch3: Seq[(Time, Letter)] = Seq(
		(new Timestamp(NOW ), "c2"),
		(new Timestamp(NOW ), "c3"),
		(new Timestamp(NOW ), "c4"),
		(new Timestamp(NOW ), "d3")
	)

	memoryStream.addData(batch1)
	writeQuery.processAllAvailable()

	memoryStream.addData(batch2)
	writeQuery.processAllAvailable()

	memoryStream.addData(batch3)
	writeQuery.processAllAvailable()

	writeQuery.awaitTermination()
}
