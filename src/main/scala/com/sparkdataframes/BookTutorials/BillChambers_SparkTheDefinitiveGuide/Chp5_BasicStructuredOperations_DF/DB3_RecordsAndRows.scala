package com.sparkdataframes.BookTutorials.BillChambers_SparkTheDefinitiveGuide.Chp5_BasicStructuredOperations_DF

/**
 *
 */

object DB3_RecordsAndRows extends App {


	// Databricks notebook source

	import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Column}
	import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
	import org.apache.spark.sql.types.Metadata
	import org.apache.spark.sql.functions.{col, column, expr}


	import com.data.util.DataHub.ImportedDataFrames._
	import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._


	// COMMAND ----------

	// MAGIC %md
	// MAGIC * Each row in a dataframe is a single record.
	// MAGIC * Spark represents this record as an object of type `Row`.
	// MAGIC * `Row` objects internally represent arrays of bytes.

	// COMMAND ----------


	val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("sparkBillChambers").getOrCreate()

	import sparkSession.implicits._


	// COMMAND ----------

	flightDf.first() // a row

	// COMMAND ----------

	// MAGIC %md
	// MAGIC ## Creating Rows
	// MAGIC * can create by instantiating `Row` object with values that belong in each column.
	// MAGIC * Rows themselves do not have schemas, only dataframes have schemas
	// MAGIC * Must specify the values in the same order as the schema of the dataframe to which they might be appended.

	// COMMAND ----------

	import org.apache.spark.sql.Row

	val aRow = Row("hello", null, 1, false)

	// COMMAND ----------

	val flightRow = Row("ACountry", "AnotherCountry", 234)

	// COMMAND ----------

	// Accessing
	List(aRow(0), aRow(1), aRow(2), aRow(3))

	// COMMAND ----------

	aRow.getString(0)

	// COMMAND ----------

	aRow.get(0)

	// COMMAND ----------

	aRow.getAs[Float](2)

	// COMMAND ----------

}
