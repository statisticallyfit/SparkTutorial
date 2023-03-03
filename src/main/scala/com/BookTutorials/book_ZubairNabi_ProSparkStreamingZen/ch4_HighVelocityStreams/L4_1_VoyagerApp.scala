package com.BookTutorials.book_ZubairNabi_ProSparkStreamingZen.ch4_HighVelocityStreams

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 */




object L4_1_VoyagerApp extends App {

	val APP_NAME: String = "VoyagerApp"
	val INPUT_PATH: String = ""
	val OUTPUT_PATH: String = ""


	val conf = new SparkConf()
		.setAppName(APP_NAME)
		.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
		.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC")

	val ssc = new StreamingContext(conf, Seconds(10))


	// TODO what is the input data???
	val voyager1 = ssc.fileStream[LongWritable, Text, TextInputFormat](INPUT_PATH, (f: Path) => true, newFilesOnly = false).map(pair => pair._2.toString)
	voyager1.map(rec => {
		val attrs = rec.split("\\s+")
		((attrs(0).toInt), attrs.slice(18, 28).map(_.toDouble))
	}).filter(pflux => pflux._2.exists(_ > 1.0)).map(rec => (rec._1, 1))
		.reduceByKey(_ + _)
		.transform(rec => rec.sortByKey(ascending = false, numPartitions = 1))
		.saveAsTextFiles(OUTPUT_PATH) // TODO see here what is the output like

	ssc.start()
	ssc.awaitTermination()
}
