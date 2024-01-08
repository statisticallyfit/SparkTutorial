package com.sparkdataframes.BookTutorials.BillChambers_SparkTheDefinitiveGuide.Chp5_BasicStructuredOperations_DF

/**
 *
 */

object DB1_Schemas extends App {


	// Databricks notebook source

	import com.data.util.DataHub.ImportedDataFrames._
	import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._

	import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Column}
	import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
	import org.apache.spark.sql.types.Metadata

	// COMMAND ----------


	val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("sparkBillChambers").getOrCreate()
	import sparkSession.implicits._


//	val db_PATH: String = "/FileStore/tables/Users/statisticallyfit@gmail.com/SparkTutorialRepo/BillChambers_SparkTheDefinitiveGuide"
//	val db_dataPath: String = "/data/flight-data/json/2015_summary.json"


	//display(flightDf)

	// COMMAND ----------
	// ERROR: colname needs to be a String
	//flightDf.col($"count")


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

	val flightManualSchemaDf: DataFrame = sparkSession.read.format(FORMAT_JSON).schema(myManualSchema).load(s"$PATH/$folderBillChambers/flight-data")

	//display(flightManualSchemaDf)
	flightManualSchemaDf.show

	// COMMAND ----------
}

