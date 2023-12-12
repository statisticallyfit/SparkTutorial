package com.sparkdataframes.BookTutorials.BillChambers_SparkTheDefinitiveGuide.Chp5_BasicStructuredOperations_DF

/**
 *
 */

object DB1_Schemas extends App {


	// Databricks notebook source

	import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Column}
	import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
	import org.apache.spark.sql.types.Metadata

	// COMMAND ----------


	val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("sparkBillChambers").getOrCreate()

	import sparkSession.implicits._


	val PATH: String = "/FileStore/tables/Users/statisticallyfit@gmail.com/SparkTutorialRepo/BillChambers_SparkTheDefinitiveGuide/data"

	val dataPath: String = "/flight-data/json/2015_summary.json"

	val flightDf: DataFrame = sparkSession.read.format("json").load(PATH + dataPath)

	//display(flightDf)

	// COMMAND ----------
	// ERROR: colname needs to be a String
	//flightDf.col($"count")

	// COMMAND ----------

	flightDf.col("count")

	// COMMAND ----------

	flightDf.schema

	// COMMAND ----------

	flightDf.printSchema

	// COMMAND ----------

	// How to enforce a specific schema on a dataframe

	val myManualSchema = StructType(Array(
		StructField("DEST_COUNTRY_NAME", StringType, true),
		StructField("ORIGIN_COUNTRY_NAME", StringType, true),
		StructField("count", LongType, false,
			Metadata.fromJson("{\"hello\":\"world\"}"))
	))
	myManualSchema

	// COMMAND ----------

	val flightManualSchemaDf = sparkSession.read.format("json").schema(myManualSchema).load(PATH + dataPath)

	//display(flightManualSchemaDf)

	// COMMAND ----------
}

