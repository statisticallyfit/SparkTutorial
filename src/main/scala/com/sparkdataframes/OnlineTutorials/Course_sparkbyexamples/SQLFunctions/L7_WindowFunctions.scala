package com.sparkscalaexamples.SQLFunctions


import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{row_number}
import org.apache.spark.sql.expressions.{Window, WindowSpec}

/**
 *
 */
object L7_WindowFunctions extends App {



	val spark: SparkSession = SparkSession.builder()
		.master("local[1]")
		.appName("SparkByExamples.com")
		.getOrCreate()
	// REPL
	//val spark: SparkSession = SparkSession.builder().master("local[1]").appName("SparkByExamples.com").getOrCreate()

	import spark.implicits._

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

	val df: DataFrame = data.toDF(colnames:_*)
	df.show()


	/**
	 * NOTE: To perform an operation on a group first, we need to partition the data using Window.partitionBy() , and for row number and rank function we need to additionally order by on partition data using orderBy clause.
	 */

	// Row number window function
	val windowSpec_partitionDeptOrdSal: WindowSpec = Window.partitionBy("department").orderBy("salary")


	// NOTE: row number relates only to the partition
	val rowNumberDf: DataFrame = df.withColumn("RowNumberResult", row_number.over(windowSpec_partitionDeptOrdSal))

	rowNumberDf.show()
}
