package com.BookTutorials.book_MarkoBonaci_SparkInAction.ch6_IngestDataWithSparkStreaming


import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream


/**
 * <GOAL 1/>: read in file using spark context (aka non-streaming methods)
 * <GOAL 2/>: compare reading in file that was generated manually versus one that was generated using a streaming
 * method (from the ch6_IngestData.scala file)
 *
 * To see how non-streaming methods can read what is in the file - does it work or not?
 *
 */
object snippet_ReadFileWithSparkContext  extends App {


	val sparkSession: SparkSession = SparkSession.builder()
		.master("local[1]")
		.appName("snippet_ReadFileWithSparkContext")
		.getOrCreate();
	// REPL
	// val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("snippet_ReadFileWithSparkContext").getOrCreate();

	// To be able to convert to df
	import sparkSession.implicits._


	// Create the SparkContext
	val sc: SparkContext = sparkSession.sparkContext

	// PATH
	val PATH = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/BookTutorials/book_MarkoBonaci_SparkInAction/ch6_IngestDataWithSparkStreaming"
	val inputStreamFolderCSV: String = "inputStreamFolderCSV"
	val inputManualFolderCSV: String = "inputManualFolderCSV"
	val fileStreamGen: String = "ordersaa.csv" // the name of the file generated using a streaming method
	val fileManualGen: String = "manualFile.csv" // the name of the file created manually

	// Read in the file from the folder, the file that was created (from ch6_ingestdata.scala) using a streaming method
	val textFileStreamGen: RDD[String] = sc.textFile(path = s"$PATH/$inputStreamFolderCSV/$fileStreamGen")
	val textFileManualGen: RDD[String] = sc.textFile(path = s"$PATH/$inputManualFolderCSV/$fileManualGen")


	Console.println(s"Show stream df output: ")
	textFileStreamGen.take(10).foreach(println)

	Console.println(s"\nShow manual df output")
	textFileManualGen.toDF().show()

}
