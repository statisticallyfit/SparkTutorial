package com.NonBookExamples.AnimalExample

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable

/**
 * GOAL:
 * 	1) write a lsit of animals in an RDD to multiple files, in a streaming-fashion
 * 	2) get those groups of animals back (read back) into separate RDDs.
 *
 * 	METHOD for #2: using `foreachRDD` and `saveAsTextFile` for every iteration.
 *
 */
object snippet_AnimalReadStreaming_via_Foreach_saveAsTextFiles extends App {



	val NUM_STREAM_SECONDS = 1
	val LOCAL_NUM = 2
	val NUM_PARTITIONS = 5

	val sparkSession: SparkSession = SparkSession.builder()
		.master("local[2]")
		.appName("snippet_ReadFileWithSparkContext")
		.getOrCreate()

	import sparkSession.implicits._

	val sc: SparkContext = sparkSession.sparkContext
	val ssc: StreamingContext = new StreamingContext(sc, Seconds(NUM_STREAM_SECONDS))

	sc.setLogLevel("ERROR")


	/**
	 * IMPLEMENTING GOAL #1: sending the list of animals into partitioned files at folder `path_TO`
	 */

	// Create variables for path and foldernames
	val PATH: String = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/NonBookExamples/AnimalExample"
	val outputFolder: String = "outputAnimal"
	val output_TO: String = "animal_TO_foreachway"
	val output_FROM: String = "animal_FROM_foreachway"

	val path_TO: String = s"$PATH/$outputFolder/$output_TO"
	val path_FROM: String = s"$PATH/$outputFolder/$output_FROM"

	Console.println(s"PATH = $PATH")
	Console.println(s"folder for outputting the first RDD from original list: $path_TO")
	Console.println(s"folder for outputting dstream contents (after reading from first output location): $path_FROM")


	// Send the list of animals to the folder
	val lst: immutable.Seq[String] = List("giraffe", "hippopotamus", "gazelle", "zebra", "elephant", "crocodile", "alligator", "ostrich", "parrot", "frog", "snake", "cougar", "jaguar", "panther", "cheetah", "lion", "ant", "tucan", "flamingo", "kangaroo", "hyena")

	val rddAnimals: RDD[String] = sc.parallelize(lst, numSlices = NUM_PARTITIONS)
	Console.println("Show rddAnimals:")
	rddAnimals.toDF().show()

	// NOTE: Step 1: SENDING the output to this location so that the information is stored as file in this location
	rddAnimals.saveAsTextFile(path = path_TO) // TODO figure out if this needs to use ssc.saveAsTextFileSSS if you
	// are later going to use dstream to get the output back (for sc.textfile maybe no need)
	Console.println(s"rddAnimals was saved to $path_TO")


	/**
	 * IMPLEMENTING GOAL #2: trying to read from the `path_TO` and output it using `foreachRDD` and `saveAsTextFile`
	 * for every iteration.
	 */
}
