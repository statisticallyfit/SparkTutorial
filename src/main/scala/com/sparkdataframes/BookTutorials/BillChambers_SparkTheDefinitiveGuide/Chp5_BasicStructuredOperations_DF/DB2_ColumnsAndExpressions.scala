package com.sparkdataframes.BookTutorials.BillChambers_SparkTheDefinitiveGuide.Chp5_BasicStructuredOperations_DF

/**
 *
 */

object DB2_ColumnsAndExpressions extends App {


	// Databricks notebook source

	import com.data.util.DataHub.ImportedDataFrames._
	import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._

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
	// MAGIC * Expression = set of transformations on one or more values in a record in a DataFrame. Like function that takes as input a series of column names, resolves them, then applies more expressions to create a single value for each record in the dataset. T
	// his “single value” can actually be a complex type like a Map or Array.
	// MAGIC
	// MAGIC ### Columns as Expressions
	// MAGIC * using `col()`: must perform transformations on that specific column reference:
	// MAGIC * using `expr()`: takes argument and can parse that from a string

	// COMMAND ----------


	val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("sparkBillChambers").getOrCreate()
	import sparkSession.implicits._

	//display(flightDf)

	// COMMAND ----------

	flightDf.col("count") - 5

	// COMMAND ----------

	val e: Column = expr(s"${flightDf.col("count") - 5}")


	// COMMAND ----------

	expr("(((someCol + 5) * 200) - 6) < otherCol")

	// COMMAND ----------

	flightDf.columns

	// COMMAND ----------

	/**
	 * NOTE: columns vs. expressions
	 *
	 * Columns
	 * 	- provide subset of expression functionality
	 * 	- must perform the transformations on the column reference itself
	 * 	- columns are expressions
	 * 	- columns and their transformations compile to the same logical plan as parsed expressions.
	 *
	 * Expressions:
	 * 	- the `expr` function can parse transformations and column references from a string and can subsequently be passed in to further transformations
	 */
	// Same to do:
	val e1 = expr("someCol - 5")
	val c1 = col("someCol") - 5
	val ec1 = expr("someCol") - 5
	assert(e1 == c1 && e1 == ec1)

	val c2 = (((col("someCol") + 5) * 200) - 6) < col("otherCol")
	val e2 = expr("(((someCol + 5) * 200) - 6) < otherCol")
	assert(c2 == e2)

}