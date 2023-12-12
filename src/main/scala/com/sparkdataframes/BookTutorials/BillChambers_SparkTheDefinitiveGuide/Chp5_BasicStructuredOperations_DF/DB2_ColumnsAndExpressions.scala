package com.sparkdataframes.BookTutorials.BillChambers_SparkTheDefinitiveGuide.Chp5_BasicStructuredOperations_DF

/**
 *
 */

object DB2_ColumnsAndExpressions extends App {


	// Databricks notebook source

	import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Column}
	import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
	import org.apache.spark.sql.types.Metadata
	import org.apache.spark.sql.functions.{col, column, expr}

	// COMMAND ----------

	// MAGIC %md
	// MAGIC * Cannot manipulate a column outside the dataframe.
	// MAGIC * Must use a spark transformation within a dataframe to change the column.

	// COMMAND ----------

	col("some column name")

	// COMMAND ----------

	column("another column name")

	// COMMAND ----------

	// $"column name #3"

	// COMMAND ----------

	'columnNameAgain


	// COMMAND ----------

	// MAGIC %md
	// MAGIC ## Expressions
	// MAGIC * Columns are expressions
	// MAGIC * Expression = set of transformations on one or more values in a record in a DataFrame. Like function that takes as input a series of column names, resolves them, then applies more expressions to create a single value for each record in the dataset.
	// MAGIC
	// MAGIC ### Columns as Expressions
	// MAGIC * using `col()`: must perform transformations on that specific column reference:
	// MAGIC * using `expr()`: takes argument and can parse that from a string

	// COMMAND ----------


	val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("sparkBillChambers").getOrCreate()

	import sparkSession.implicits._


	val PATH: String = "/FileStore/tables/Users/statisticallyfit@gmail.com/SparkTutorialRepo/BillChambers_SparkTheDefinitiveGuide/data"

	val dataPath: String = "/flight-data/json/2015_summary.json"

	val flightDf: DataFrame = sparkSession.read.format("json").load(PATH + dataPath)

	//display(flightDf)

	// COMMAND ----------

	flightDf.col("count") - 5

	// COMMAND ----------

	expr(s"${flightDf.col("count") - 5}")

	// COMMAND ----------

	expr("(((someCol + 5) * 200) - 6) < otherCol")

	// COMMAND ----------

	flightDf.columns

	// COMMAND ----------


}