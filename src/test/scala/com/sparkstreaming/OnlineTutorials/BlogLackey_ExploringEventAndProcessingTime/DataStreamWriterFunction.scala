//package com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime
//
//
//
//import java.sql.Timestamp
//import java.text.SimpleDateFormat
//import java.util.Date
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.DataFrame
////import org.apache.spark.sql.execution.streaming.TextSocketSource2
//import com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime.sparkSocketExtension.TextSocketSource2
//import org.apache.spark.sql.functions.window
//import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
//import org.apache.spark.sql.{Dataset, Row, SparkSession}
//
//
//import com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime.GroupByWindowExample_BY_SOCKET._
//
///**
// *
// */
//object DataStreamWriterFunction {
//
//
//	def getDataStreamWriter(sparkSession: SparkSession, lines: Dataset[String]): DataStreamWriter[Row] = {
//
//		import sparkSession.implicits._
//
//		// Process each line in 'lines' by splitting on the "," and creating
//		// a Dataset[AnimalView] which will be partitioned into 5 second window
//		// groups. Within each time window we sum up the occurrence counts
//		// of each animal .
//		val animalViewDs: Dataset[AnimalView] = lines.flatMap {
//			line => {
//				try {
//					val columns: Array[String] = line.split(",")
//					val str: String = columns(0)
//					val date: Date = fmt.parse(str)
//					Some(AnimalView(new Timestamp(date.getTime), columns(1), columns(2).toInt))
//				} catch {
//					case e: Exception =>
//						println("ignoring exception : " + e);
//						None
//				}
//			}
//		}
//
//		val windowedCount: DataFrame = animalViewDs
//			//.withWatermark("timeSeen", "5 seconds")
//			.groupBy(
//				window($"timeSeen", "5 seconds"), $"animal")
//			.sum("howMany")
//
//		windowedCount.writeStream
//			.trigger(Trigger.ProcessingTime(5 * 1000))
//	}
//}
