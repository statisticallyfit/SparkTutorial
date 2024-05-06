package com.sparkdataframes.BookTutorials.Damji_LearningSpark.Chp2_DownloadingApacheSpark


import org.apache.spark.sql.{Column, ColumnName, DataFrame, DataFrameReader, DataFrameWriter, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
/**
 *
 */


object MnMCount extends App {


	val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("MnMCount").getOrCreate()

	import utilities.DataHub.ImportedDataFrames.fromDamjiBook._

	val countMnMDf: Dataset[Row] = (mnmDf
		.select("State", "Color", "Count")
		.groupBy("State", "Color")
		.agg(count("Count").alias("TotalCount"))
		//.orderBy("State")
		)
		//.orderBy(desc("TotalCount"))) // TODO, ascending = false)

	// Show the resulting aggregations for all the states and colors; a total count of each color per state.
	countMnMDf.show(numRows = 60, truncate = false)

	println(s"Total rows = ${countMnMDf.count()}")


	 // Showing data just for single state, e.g. CA
	// 1. select from all rows in the dataframes
	// 2. filter only CA state
	// 3. groupby state and color
	// 4. aggregate the counts for each color
	// 5. orderby in descending order
	// Find the aggregate count for CA by filtering
	val caCountMnMDf: Dataset[Row] = (mnmDf
		.select("State", "Color", "Count")
		.where(col("State") === "CA")
		.groupBy("State", "Color")
		.agg(count("Count").alias("TotalCount"))
		.orderBy(desc("TotalCount"))

		)
	caCountMnMDf.show(10, truncate = false)
	println(s"Total rows for CA = ${caCountMnMDf.count()}")


	sparkSession.stop()
}
