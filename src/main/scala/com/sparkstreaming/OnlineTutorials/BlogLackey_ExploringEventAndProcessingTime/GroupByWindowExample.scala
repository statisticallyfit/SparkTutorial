package com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime




import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}

// TODO adjusting imoprts - check if still working?
//import org.apache.spark.sql.execution.streaming.TextSocketSource2
//import com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime.sparkSocketExtension.TextSocketSource2
import org.apache.spark.sql.execution.streaming.sources.TextSocketMicroBatchStream

import com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime.sparkSocketExtension.TextSocketSource2
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime.DataStreamWriterFunction._

/**
 * Source = https://github.com/buildlackey/spark-streaming-group-by-event-time/blob/master/src/main/scala/com/lackey/stream/examples/GroupByWindowExample.scala
 */
object GroupByWindowExample extends App {


	case class AnimalView(timeSeen: Timestamp, animal: String, howMany: Integer)

	val fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

	val sparkSession = SparkSession.builder
		.master("local[1]")
		.appName("example")
		.getOrCreate()


	Logger.getLogger("org").setLevel(Level.OFF)
	//Logger.getLogger(classOf[MicroBatchExecution]).setLevel(Level.DEBUG)

	import sparkSession.implicits._

	val socketStreamDs: Dataset[String] =
		sparkSession.readStream
			//.format("org.apache.spark.sql.execution.streaming.TextSocketSourceProvider2")
			.format("com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime.sparkSocketExtension.TextSocketSourceProvider2")
			.option("host", "localhost")
			.option("port", 9999)
			.load()
			.as[String] // Each string in dataset == 1 line sent via socket

	val writer: DataStreamWriter[Row] = getDataStreamWriter(sparkSession, socketStreamDs)

	Future {
		Thread.sleep(4 * 5 * 1000)
		val msgs: List[String] = TextSocketSource2.getMsgs()
		System.out.println("msgs:" + msgs.mkString("\n"))
	}

	writer
		.format("console")
		.option("truncate", "false")
		.outputMode(OutputMode.Update())
		.start()
		.awaitTermination()


}
