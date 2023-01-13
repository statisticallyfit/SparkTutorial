package com.BookTutorials.book_MarkoBonaci_SparkInAction.ch6_IngestDataWithSparkStreaming


import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import java.io.File

import scala.io.Source


/**
 * Source code example from = https://github.com/apache/spark/blob/dbde77856d2e51ff502a7fc1dba7f10316c2211b/core/src/test/scala/org/apache/spark/FileSuite.scala#L60
 */
object scrap_TrySnippetSaveAsTextFile {

	val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("IngestDataWithStreaming").getOrCreate();


	import sparkSession.implicits._


	val ssc: StreamingContext = new StreamingContext(sparkContext = sparkSession.sparkContext,	batchDuration= Seconds(5))
	val sc = sparkSession.sparkContext


	val PATH = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/BookTutorials/book_MarkoBonaci_SparkInAction/ch6_IngestDataWithSparkStreaming"

	val outputDir = new File(s"$PATH/tempdir").getAbsolutePath()

	val nums = sc.makeRDD(1 to 4)
	val textFilePath1 = s"$outputDir/numsFile"
	nums.saveAsTextFile(textFilePath1)
	val nums2 = sc.makeRDD(10 to 20)
	val textFilePath2 = s"$outputDir/numsFile2"
	nums2.saveAsTextFile(textFilePath2)
	val nums3 = sc.makeRDD(20 to 30)
	val textFilePath3 = s"$outputDir/numsFile3"
	nums3.saveAsTextFile(textFilePath3)

	// Just to show contents
	List(nums, nums2, nums3).foreach(_.toDF().show())


	// METHOD 1 (reading back): Read the plain text file and check it's OK
	val outputFile = new File(textFilePath1, "part-00000")
	val bufferSrc = Source.fromFile(outputFile)

	val outputFile2 = new File(textFilePath2, "part-00000")
	val bufferSrc2 = Source.fromFile(outputFile2)

	val outputFile3 = new File(textFilePath3, "part-00000")
	val bufferSrc3 = Source.fromFile(outputFile3)
	// NOTE>: these buffers have to be read each time you want to get the result (after querying, meaning after
	//  applying a function on the bufferSrc object, need to read it again from Source (like above) to get the info
	//  again). Example: calling bufferSrc.mkString makes bufferSrc empty again.

	assert(bufferSrc.mkString == "1\n2\n3\n4\n")
	assert(bufferSrc2.mkString == "10\n11\n12\n13\n14\n15\n16\n17\n18\n19\n20")
	assert(bufferSrc3.mkString == "20\n21\n22\n23\n24\n25\n26\n27\n28\n29\n30")

	// METHOD 2(reading back): read in as text file RDD
	val rdd1 = sc.textFile(textFilePath1).collect().toList
	assert(rdd1 === List("1", "2", "3", "4"))
	val rdd2 = sc.textFile(textFilePath2).collect().toList
	assert(rdd2 === List(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20).map(_.toString))
	val rdd3 = sc.textFile(textFilePath3).collect().toList
	assert(rdd3 === (20 to 30).toList.map(_.toString))

}
