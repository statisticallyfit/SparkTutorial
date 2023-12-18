package com.sparkdataframes.OnlineTutorials.Course_sparkbyexamples.SQLFunctions


// Databricks notebook source

/**
 * Can put the commands here in the REPL too
 *
 * Source =https://sparkbyexamples.com/spark/spark-sql-window-functions/#ranking-functions
 */

object DB7_WindowFunctions extends App {


	// MAGIC %md
	// MAGIC Tutorial sources:
	// MAGIC * https://sparkbyexamples.com/spark/spark-sql-window-functions/

	// COMMAND ----------


	import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Column}
	import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
	import org.apache.spark.sql.types.Metadata
	import org.apache.spark.sql.functions.{col, column, expr, row_number, rank, dense_rank, percent_rank, ntile, cume_dist, lag, lead, min, max, avg, sum, count}
	// rangeBetween, rowsBetween

	import org.apache.spark.sql.expressions.{Window, WindowSpec}

	// COMMAND ----------

	val spark: SparkSession = SparkSession.builder()
		.master("local[1]")
		.appName("SparkByExamples.com")
		.getOrCreate()
	// REPL
	//val spark: SparkSession = SparkSession.builder().master("local[1]").appName("SparkByExamples.com").getOrCreate()

	import spark.implicits._

	// COMMAND ----------

	val data = Seq(
		("James", "Sales", 3000),
		("Michael", "Sales", 4600),
		("Robert", "Sales", 4100),
		("Maria", "Finance", 3000),
		("James", "Sales", 3000),
		("Scott", "Finance", 3300),
		("Jen", "Finance", 3900),
		("Jeff", "Marketing", 3000),
		("Kumar", "Marketing", 2000),
		("Saif", "Sales", 4100)
	)

	val colnames: Seq[String] = Seq("employee_name", "department", "Salary")

	val df: DataFrame = data.toDF(colnames: _*)
	// display(df)



	// COMMAND ----------

	// MAGIC %md
	// MAGIC For ranking and analytic functions, we need to partition the data using `Window.partitionBy()` followed by ordering on this partitioned data using `orderBy()`. For non-ranking, non-analytic functions, such as the grouping (aggregate) functions, we don't need to order the partitioned data, so we can leave the window spec defined by just `Window.partitionBy()`.
	// MAGIC Source: https://hyp.is/9RptnJjaEe6UwKNP6co7aA/sparkbyexamples.com/spark/spark-sql-window-functions/
	// MAGIC

	// COMMAND ----------

	// Row number window function
	val windowPartOrdSpec: WindowSpec = Window.partitionBy("department").orderBy("salary")
	windowPartOrdSpec

	// COMMAND ----------

	// MAGIC %md
	// MAGIC ## Ranking Functions
	// MAGIC * `row_number()`
	// MAGIC * `rank()`
	// MAGIC * `dense_rank()`
	// MAGIC * `percent_rank()`
	// MAGIC * `ntile()`

	// COMMAND ----------

	// MAGIC %md
	// MAGIC #### `row_number`
	// MAGIC
	// MAGIC `row_number()` gives a number determined by the partition (orders rows within a partition)
	// MAGIC

	// COMMAND ----------

	// NOTE: row number relates only to the partition
	val rowNumberDf: DataFrame = df.withColumn("RowNumberResult", row_number().over(windowPartOrdSpec))

	// display(rowNumberDf)

	// COMMAND ----------

	// MAGIC %md
	// MAGIC #### `rank`
	// MAGIC
	// MAGIC `rank()` window function is used to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties.

	// COMMAND ----------

	val rankDf: DataFrame = df.withColumn("RankResult",
		rank().over(windowPartOrdSpec))
	// display(rankDf)

	// COMMAND ----------

	df.printSchema

	// COMMAND ----------


	rowNumberDf.schema.map(_.dataType.typeName)

	// COMMAND ----------


	// COMMAND ----------

	// MAGIC %md
	// MAGIC #### Replicating the rank function

	// COMMAND ----------

	// Little experiment here - making an algorithm to write the rank id
	// case 1 - within a partition, if the second col has a tie, then keep the same id, else skip to next id + 1
	// case 2 - within a partition if there is no tie (if different item following), then increment the id

	val tupsInOrder = rowNumberDf.select($"department", $"Salary", $"RowNumberResult").collect().toSeq.map(row => row.toSeq match {
		case Seq(dept, sal, id) => (dept, sal, id).asInstanceOf[(String, Integer, Integer)]
	})

	// COMMAND ----------

	tupsInOrder
		.groupBy { case (dept, sal, id) => dept }
		.values.map(_.toList).toList

	// COMMAND ----------

	def groupMid(lst: List[(String, Integer, Integer)]) = lst.groupBy { case (dept, sal, id) => sal }

	val tupsSal = tupsInOrder
		.groupBy { case (dept, sal, id) => dept }
		.values.map(_.toList).toList
		.map(groupMid(_).values.toList).flatten


	// COMMAND ----------

	import scala.collection.mutable.ListBuffer

	val buf: ListBuffer[Integer] = ListBuffer()

	def getNewId(lst: List[(String, Integer, Integer)]) = {
		buf += lst.head._3;
		lst.head._3
	}

	// COMMAND ----------

	tupsSal.map(lst => lst.length match {
		case n if n > 1 => {
			val newId = getNewId(lst)
			lst.map { case (dept, sal, id) => (dept, sal, newId) }
		}
		case _ => lst

	})

	// COMMAND ----------

	// display(rankDf)

	// COMMAND ----------


	// COMMAND ----------

	// MAGIC %md
	// MAGIC #### `dense_rank`
	// MAGIC `dense_rank()` window function is used to get the result with rank of rows within a window partition without any gaps. This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.

	// COMMAND ----------

	val denseRankDf = df.withColumn("DenseRankResult",
		dense_rank().over(windowPartOrdSpec))
	// display(denseRankDf)

	// COMMAND ----------

	// MAGIC %md
	// MAGIC #### `percent_rank`
	// MAGIC `percent_rank()` gives the percentile of each item

	// COMMAND ----------

	val percentRankDf = df.withColumn("DenseRankResult", percent_rank().over(windowPartOrdSpec))
	// display(percentRankDf)

	// COMMAND ----------

	// MAGIC %md
	// MAGIC #### `ntile`
	// MAGIC `ntile()` window function returns the relative rank of result rows within a window partition. In below example we have used 2 as an argument to ntile hence it returns ranking between 2 values (1 and 2)

	// COMMAND ----------

	val ntileDf = df.withColumns(Map("ntile2_result" -> ntile(2).over(windowPartOrdSpec),
		"ntile3_result" -> ntile(3).over(windowPartOrdSpec),
		"ntile4_result" -> ntile(4).over(windowPartOrdSpec),
		"ntile5_result" -> ntile(5).over(windowPartOrdSpec)
	))
	// display(ntileDf)

	// COMMAND ----------


	// COMMAND ----------

	// MAGIC %md
	// MAGIC ## Analytic Functions
	// MAGIC * `cume_dist()`
	// MAGIC * `lag()`
	// MAGIC * `lead()`

	// COMMAND ----------

	// MAGIC %md
	// MAGIC #### `cume_dist`
	// MAGIC `cume_dist()` function returns the cumulative distribution of values within a partition ordering.

	// COMMAND ----------

	val cumeDistDf: DataFrame = df.withColumn("CumeDistResult", cume_dist().over(windowPartOrdSpec))
	// display(cumeDistDf)

	// COMMAND ----------


	// COMMAND ----------


	// COMMAND ----------

	// MAGIC %md
	// MAGIC #### `lag`
	// MAGIC `lag()` will return "lag" value x places before current row - it helps to build a sequence between rows
	// MAGIC * Intuition: links to the col-value from the PREVIOUS row.

	// COMMAND ----------

	val lagDf: DataFrame = df.withColumns(Map(
		"Lag_ColumnObj_offset1" -> lag(e = $"Salary", offset = 1).over(windowPartOrdSpec),
		"Lag_ColumnName_offset2" -> lag(columnName = "Salary", offset = 2).over(windowPartOrdSpec),

	))
	// display(lagDf)

	// COMMAND ----------

	// Assert: will get nullpointerexception if the offset amount is larger than any of the counts in the partition.
	// EXAMPLE: here offset = 3 is larger than the amoutn of Marketing so will throw null pointer exception

	/*import org.scalatest.Assertions._
	import scala.reflect.runtime.universe._

	val resultOffset3 = intercept[NullPointerException] {

		// display(df.withColumn("Lag_Column_offset3", lag($"Salary", offset = 3).over(windowSpec_partitionDeptOrdSal)))
		///theDf.show
	}*/
	//resultOffset3.isInstanceOf[NullPointerException]

	// TODO why was no exception thornw when here with this single line it throws exception?
	// df.withColumn("Lag_Column_offset3", lag($"Salary", offset = 3).over(windowSpec_partitionDeptOrdSal)).show


	// COMMAND ----------


	// COMMAND ----------

	// MAGIC %md
	// MAGIC #### `lead` function
	// MAGIC `lead` returns a value x places after current row - does exactly the same kind of shift like lag() except in the opposite direction.
	// MAGIC * Intuition: links to the col-value from the NEXT row.

	// COMMAND ----------

	val leadDf: DataFrame = df.withColumns(Map(
		"Lead_ColumnObj_offset1" -> lead(e = $"Salary", offset = 1).over(windowPartOrdSpec),
		"Lead_ColumnName_offset2" -> lead(columnName = "Salary", offset = 2).over(windowPartOrdSpec),
		"Lead_ColumnName_offset3" -> lead(columnName = "Salary", offset = 3).over(windowPartOrdSpec),
		"Lead_ColumnObj_offset4" -> lead(e = $"Salary", offset = 4).over(windowPartOrdSpec),
		"Lead_ColumnObj_offset5" -> lead(e = $"Salary", offset = 5).over(windowPartOrdSpec)
	))
	// display(leadDf) // weird this doesn't return any nullpointer exception while lag kinds do?

	// COMMAND ----------


	// COMMAND ----------

	// MAGIC %md
	// MAGIC ## Aggregate Functions
	// MAGIC * `min()`
	// MAGIC * `max()`
	// MAGIC * `avg()`
	// MAGIC * `count()`
	// MAGIC * `sum()`

	// COMMAND ----------

	// Need to create a window spec that is not ordered when using aggregate functions
	val windowPartSpec = Window.partitionBy("department")


	val aggDf = df.withColumn("row", row_number.over(windowPartOrdSpec))
		.withColumn("avg", avg(col("salary")).over(windowPartSpec))
		.withColumn("sum", sum(col("salary")).over(windowPartSpec))
		.withColumn("min", min(col("salary")).over(windowPartSpec))
		.withColumn("max", max(col("salary")).over(windowPartSpec))

	// display(aggDf)

	// COMMAND ----------

	// Showing just the simple results
	// display(aggDf.where(col("row") === 1).select("department", "avg", "sum", "min", "max"))

	// COMMAND ----------

	// Another way
	// display(aggDf.select("department", "avg", "sum", "min", "max").dropDuplicates())

	// COMMAND ----------

	// TODO NEXT: add examples from the other website-tutorials (other data)

}