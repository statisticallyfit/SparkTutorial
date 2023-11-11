package com.BookTutorials.book_GerardMaas_StreamProcessing.ch19_SparkStreamingSources


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.mutable
import scala.collection.mutable.Queue

/**
 *
 */
object Example_StreamWordCountQueue extends App {


	val NUM_STREAM_SECONDS = 1
	val LOCAL_NUM = 2
	val NUM_PARTITIONS = 5

	val sparkSession: SparkSession = SparkSession.builder()
		.master(s"local[$LOCAL_NUM]")
		.appName("IngestDataWithStreaming")
		.getOrCreate()

	val sc: SparkContext = sparkSession.sparkContext


	import sparkSession.implicits._
	val ssc: StreamingContext = new StreamingContext(sc, Seconds(NUM_STREAM_SECONDS))

	val PATH: String = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/BookTutorials/book_MarkoBonaci_SparkInAction/ch6_IngestDataWithSparkStreaming"
	Console.println(s"PATH = $PATH")

	val streamWordCount: DStream[String] => DStream[(String, Long)] = stream =>
		stream.flatMap(sentence => sentence.split(","))
			.map(word => (word.trim, 1L))
			.reduceByKey((count1: Long, count2: Long) => count1 + count2)


	// Create the queue
	// Data must be in RDDs
	val queue: Queue[RDD[String]] = new Queue[RDD[String]]() // mutable queue instance
	val data: List[String] = List(
		"Chimay, Ciney, Corsendonck, Duivel, Chimay, Corsendonck ",
		"Leffe, Ciney, Leffe, Ciney, Grimbergen, Leffe, La Chouffe, Leffe",
		"Leffe, Hapkin, Corsendonck, Leffe, Hapkin, La Chouffe, Leffe"
	)


	// Create list of rdds, each one containing a String of words.
	val rdds: List[RDD[String]] = data.map(sentence => sc.parallelize(Array(sentence)))

	// Enqueue the rdds
	queue.enqueue(rdds:_*)

	// Create the queue DStream
	val testStream: InputDStream[String] = ssc.queueStream(queue = queue, oneAtATime = true)

	// Extract the results from the streaming output. Use queue to capture the results
	val queueOut: Queue[Array[(String, Long)]] = new Queue[Array[(String, Long)]]()

	// Defining the execution of the test:
	streamWordCount(testStream)
		.foreachRDD(rdd => queueOut.enqueue(rdd.collect))


	ssc.start()
	//ssc.awaitTerminationOrTimeout(timeout = 10000) // 3 batch intervals of 1 second
	ssc.awaitTermination()


	// Finally, assert that we received the results we expect
	// First batch:
	println(s"the out queue: ${queueOut.dequeue().toList}")

	assert(queueOut.dequeue.contains("Chimay" -> 2), "missing an expected element (Chimay)")
	assert(queueOut.dequeue.contains("Leffe" -> 2), "missing an expected element (Leffe)")

}
