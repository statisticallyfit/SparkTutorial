package com.SparkDocumentationByTesting.specs.AboutDataFrames

/**
 *
 */
class AboutSQLFunctions {

	// sqrt(), monontonic(), input_partitions() = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L638-L872

	// lit(), typedLit(): https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L978-L1002


	// NOTE: different ways of calling SQL functions to do operations on dataframes - using groupby, using select, using windowing...

	/*val avgBySelectExpr: Double = flightDf.selectExpr("avg(count)").select($"avg(count)").collectCol[Double].head

	val avgBySelect: Double = flightDf.select(avg("count")).collectCol[Double].head

	val avgByGrouping: Double = flightDf.groupBy().avg("count").collectCol[Double].head // source = https://stackoverflow.com/a/44384396

	val avgByWindowingDummyCol: Double = flightDf
		.withColumn("dummyCol", lit(null))
		.withColumn("mean", avg("count").over(Window.partitionBy("dummyCol")))
		.select($"mean")
		.collectCol[Double]
		.head

	val avgByWindowingEmpty: Double = flightDf
		.withColumn("mean", avg("count").over(Window.partitionBy()))
		.select($"mean")
		.collectCol[Double]
		.head
	// source = https://stackoverflow.com/q/44382822*/
}
