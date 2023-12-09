package com.sparkdataframes.DocumentingSparkByTestScenarios


import org.apache.spark.sql._ //{SparkSession, DataFrame, Dataset, Column}
import org.apache.spark.sql.types._ //{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

/**
 *
 */
object TestData {

	val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("sparkDocumentationByTesting").getOrCreate()


	val PATH: String = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial"


	val dataBillChambers: String = "/src/main/scala/com/sparkdataframes/BookTutorials/BillChambers_SparkTheDefinitiveGuide/data"
	val dataDamji: String = ???
	val dataHolden: String = ???
	val dataJeanGeorgesPerrin: String = ???

	val flightDf: DataFrame = sparkSession.read.format("json").load(PATH + dataBillChambers + "/flight-data")
}
