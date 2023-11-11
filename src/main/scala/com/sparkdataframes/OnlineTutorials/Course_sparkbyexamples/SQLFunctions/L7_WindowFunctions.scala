package com.sparkscalaexamples.SQLFunctions


import org.apache.spark.sql.{SparkSession, DataFrame, Row}

/**
 *
 */
object L7_WindowFunctions extends App {



	val spark: SparkSession = SparkSession.builder()
		.master("local[1]")
		.appName("SparkByExamples.com")
		.getOrCreate()

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


}
