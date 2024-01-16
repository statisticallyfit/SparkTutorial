package com.sparkdataframes.BookTutorials.BillChambers_SparkTheDefinitiveGuide

/**
 *
 */
object Chp5_BasicStructuredOperations extends App {



	// Databricks notebook source

	import com.data.util.DataHub.ImportedDataFrames._
	import com.data.util.DataHub.ImportedDataFrames.FromBillChambersBook._

	import org.apache.spark.sql.functions._
	import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Column, Row}
	import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
	import org.apache.spark.sql.types.Metadata

	// COMMAND ----------


	val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("sparkBillChambers").getOrCreate()
	import sparkSession.implicits._


	/**
	 * SCHEMAS---------------------------------------------------------------------------------------------------------------
	 */

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

	/**
	 * COLUMNS AND EXPRESSIONS ---------------------------------------------------------------------------------------------------------------
	 */

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


	/**
	 * RECORDS AND ROWS COLUMNS AND EXPRESSIONS ---------------------------------------------------------------------------------------------------------------
	 */

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


	/**
	 * DATAFRAME TRANSFORMATIONS ---------------------------------------------------------------------------------------------------------------
	 */

	/**
	 * Creating DataFrames
	 */


	/**
	 * select and selectExpr
	 */

}
