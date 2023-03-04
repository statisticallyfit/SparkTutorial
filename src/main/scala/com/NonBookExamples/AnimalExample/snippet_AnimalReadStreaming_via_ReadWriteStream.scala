package com.NonBookExamples.AnimalExample




import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.streaming.OutputMode.{Append, Complete, Update}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer



/**
 * GOAL:
 * 	1) write a lsit of animals in an RDD to multiple files, in a streaming-fashion
 * 	2) get those groups of animals back (read back) into separate RDDs.
 *
 * 	METHOD for #2: using `readStream`/`writeStream`
 *
 */
object snippet_AnimalReadStreaming_via_ReadWriteStream extends App {

	/**
	 * NOTE IDEA for dstream --> get batched rdds printed out
	 * 1) print dstream contents via saveastextfile
	 * 2) use readstream/writestream method to extract it (documented here)
	 */
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
	val output_TO: String = "animal_TO_readwriteway"
	val output_FROM: String = "animal_FROM_readwriteway"

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

	/**
	 * OUTPUT BY CONSOLE:
	 	+------------+
		|       value|
		+------------+
		|     giraffe|
		|hippopotamus|
		|     gazelle|
		|       zebra|
		|    elephant|
		|   crocodile|
		|   alligator|
		|     ostrich|
		|      parrot|
		|        frog|
		|       snake|
		|      cougar|
		|      jaguar|
		|     panther|
		|     cheetah|
		|        lion|
		|         ant|
		|       tucan|
		|    flamingo|
		|    kangaroo|
		+------------+
		only showing top 20 rows
	 */

	// NOTE: Step 1: SENDING the output to this location so that the information is stored as file in this location
	rddAnimals.saveAsTextFile(path = path_TO) // TODO figure out if this needs to use ssc.saveAsTextFileSSS if you
	// are later going to use dstream to get the output back (for sc.textfile maybe no need)
	Console.println(s"rddAnimals was saved to $path_TO")


	/**
	 * IMPLEMENTING GOAL #2: trying to read from the `path_TO` and output it using `readStream` / `writeStream`
	 */

	// Create the schema in preparation of reading from `path_TO`

	import org.apache.spark.sql.types.{StringType, StructField, StructType}

	val schema: StructType = StructType(
		List(
			StructField("AnimalName", StringType, true)
		)
	)
	// Read in the data after having printed the animal list in files at path_TO location
	val dfAnimals: DataFrame = sparkSession.readStream
		.schema(schema)
		.option("header", true)
		.option("maxFilesPerTrigger", 1)
		.csv(s"$path_TO")

	dfAnimals.printSchema()


	val dfAnimalSelect: DataFrame = dfAnimals.select("AnimalName")
	Console.println("Showing dfAnimalSelect: ")
	val rowsBuf: ListBuffer[Row] = ListBuffer()

	dfAnimalSelect.foreach((row: Row) => rowsBuf += row)
	Console.println(s"Showing listbuffer of rows from dfAnimalSelect: $rowsBuf")


	// NOTE: Step 2: carrying out goal#2 to write the contents (to console here) that we read from path_TO
	val queryAnimal: StreamingQuery = dfAnimalSelect
		.writeStream
		.format("console") // print to console
		.outputMode(Update()) // NOTE for select-type ops using outputMode Update
		//.trigger(Trigger.Continuous("2 seconds")) // gives error AnalysisException: Continuous processing does not support StreamingRelation operations
		.trigger(Trigger.ProcessingTime("5 seconds")) // interval = 3 seconds
		//.trigger(Trigger.Once())
		.start()


	queryAnimal.awaitTermination()


	/**
	 * OUTPUT IN CONSOLE:
	 *
		-------------------------------------------
		Batch: 0
		-------------------------------------------
		+----------+
		|AnimalName|
		+----------+
		|   panther|
		|   cheetah|
		|      lion|
		+----------+

		-------------------------------------------
		Batch: 1
		-------------------------------------------
		+----------+
		|AnimalName|
		+----------+
		| crocodile|
		| alligator|
		|   ostrich|
		+----------+

		-------------------------------------------
		Batch: 2
		-------------------------------------------
		+----------+
		|AnimalName|
		+----------+
		|      frog|
		|     snake|
		|    cougar|
		+----------+

		-------------------------------------------
		Batch: 3
		-------------------------------------------
		+----------+
		|AnimalName|
		+----------+
		|     tucan|
		|  flamingo|
		|  kangaroo|
		|     hyena|
		+----------+

		-------------------------------------------
		Batch: 4
		-------------------------------------------
		+------------+
		|  AnimalName|
		+------------+
		|hippopotamus|
		|     gazelle|
		|       zebra|
		+------------+
	 */
}
