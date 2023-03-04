package com.NonBookExamples.AnimalExample

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable

/**
 *
 */
object snippet_animaloriginal extends App {


	val NUM_STREAM_SECONDS = 1
	val LOCAL_NUM = 2
	val NUM_PARTITIONS = 5

	// This part is done automatically if in the repl
	/*val sparkSession: SparkSession = SparkSession.builder()
		.master("local[2]")
		.appName("IngestDataWithStreaming")
		.getOrCreate();*/
	// REPL
	val sparkSession: SparkSession = SparkSession.builder().master(s"local[$LOCAL_NUM]").appName("IngestDataWithStreaming").getOrCreate();
	val sc = sparkSession.sparkContext


	import sparkSession.implicits._


	val PATH: String = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/BookTutorials/book_MarkoBonaci_SparkInAction/ch6_IngestDataWithSparkStreaming"
	Console.println(s"PATH = $PATH")


	val lst: immutable.Seq[String] = List("giraffe", "hippopotamus", "gazelle", "zebra", "elephant", "crocodile", "alligator", "ostrich", "parrot", "frog", "snake", "cougar", "jaguar", "panther", "cheetah", "lion", "ant", "tucan", "flamingo", "kangaroo", "hyena")

	val animalPATH_TO: String = s"$PATH/outputAnimal/animalPATH_TO"
	Console.println(s"animalPATH_TO = $animalPATH_TO")

	val rddAnimals: RDD[String] = sc.parallelize(lst, numSlices = NUM_PARTITIONS)


	// NOTE: Step 1: SENDING the output to this location so that the information is stored as file in this location
	rddAnimals.saveAsTextFile(path = animalPATH_TO)

	Console.println("Show rddAnimals:")
	rddAnimals.toDF().show()


	// -----
	// NOTE: Step 2a: trying to read the information back from file using textFile (non-streaming way)
	// MOVED
	/*val rdd_byTextFile: RDD[String] = sc.textFile(path = animalPATH_TO) // to-path
	Console.println("\nStep 2a: showing rdd of animals by sc.textFile method (by to-path):")
	rdd_byTextFile.toDF().show()*/


	// NOTE: Step 2b: trying to read the information back from file using SaveAsTextFile (streaming-way), to see how  it works.
	val ssc: StreamingContext = new StreamingContext(sc, Seconds(NUM_STREAM_SECONDS))

	val dstream: DStream[(LongWritable, Text)] = ssc.fileStream[LongWritable, Text, TextInputFormat](
		directory = animalPATH_TO,
		filter = (f: Path) => true,
		newFilesOnly = false
	)
	// TODO in each snippet file, fork this path - filter non empty (method 1) and split by line space (method 2)
	val dstreamStr: DStream[String] = dstream
		.map { case (longWritable, text) => text.toString }
		//.filter(str => str.nonEmpty)



	// NOTE: Now to see how the information is captured in the dstream, must output it in a manner of ways:

	// Way 1 see output - use saveastextfiles again to save in a different spot
	//Console.println("way 1 see output - saveAsTextFiles")
	val animalPATH_FROM = s"$PATH/outputAnimal/animalPATH_FROMdstream"
	Console.println(s"animalPATH_FROM = $animalPATH_FROM")

	// Output result of dstream to this second spot (FROM PATH)
	// HELP this part did not work
	val res1: DStream[String] = dstreamStr.filter(str => str.nonEmpty)
	val res2: DStream[Any] = dstreamStr.map(line => line.split("\\s+"))

	res1.repartition(NUM_PARTITIONS).saveAsTextFiles(animalPATH_FROM, suffix="nonempty")
	res2.repartition(NUM_PARTITIONS).saveAsTextFiles(animalPATH_FROM, suffix="split")
	// next try:
	/*dstreamStrNonEmpty.repartition(NUM_PARTITIONS)
		.foreachRDD((rdd: RDD[String]) => rdd.saveAsTextFile(path = animalPATH_FROM))*/


	// Input result of saving as text file using textFile
	// (textFile reads from the file located at this path and stores it into rdd)
	//val rddAnimalsFromDStream_TWICE: RDD[String] = sc.textFile(animalPATH_FROM) // from-path
	/*val res = ssc.textFileStream(directory = animalPATH_FROM) // NOTE just use fileStream here (since files are
	                                                                not streaming anymore)
		.filter((arg: String) => arg.nonEmpty)*/

	// TODO breaks here to fix
	Console.println("\nStep 2b (1): Showing rdd animals via sc.textFile (from-path):")
	//rddAnimalsFromDStream_TWICE.toDF().show()

	// Way 2 see output
	/*Console.println("way 2 see output - read/writestream")
	// TODO - use readstream / writestream accompanyingly

	dstreamStrNonEmpty.foreachRDD{
		rdd => if(!rdd.isEmpty) {
			val query: StreamingQuery = rdd.toDF()
				.writeStream
				.format("console")
				.outputMode(Update())
				.trigger(Trigger.ProcessingTime("2 seconds"))
				.start()

			query.awaitTermination()
		}
	}*/

	//val dstreamStream_way1: DStream[Array[String]] = textFileStreamGen.map(line => line.split("\\s+"))
	// print out?
	val dstreamStrNonEmpty: DStream[String] = dstreamStr.filter(_.nonEmpty)

	dstreamStrNonEmpty.foreachRDD((rddArrStr, time) => {
		val inputsCount = rddArrStr.count()
		if (inputsCount > 0) {
			val resultRDD = rddArrStr.repartition(NUM_PARTITIONS)
			resultRDD.saveAsTextFile(path = animalPATH_FROM + "/folder_time_" + time.milliseconds.toString)


		}
	})

	// way 3 see output
	// TODO how to see this result? not working?
	/*Console.println("way 3 see output - print dstream")
	dstreamStrNonEmpty.print(20)*/

	ssc.start()
	ssc.awaitTermination()
	//ssc.stop(false)
}
