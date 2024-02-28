package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutGrouping

import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook.flightDf

/**
 *
 */
class GroupBySpecs {


	// NOTE showing how groupby() alone without arg works
	// NOTE: showing how groupby() works compared to the other methods here of using the sql functions

	/*val avgExpected: Double = 1770.765625
	val avgBySelectExpr: Double = flightDf.selectExpr("avg(count)").collectCol[Double].head
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
		.head*/


	// TODO show how groupby can change what appears in the column while changing to json:

	/*val res1 = (dfNested.groupBy("name", "dob", "gender", "salary") // NOTE: these let you use them later on at get_json_object call
		.agg(collect_list(struct(col("name"), col("dob"), col("gender"), col("salary"))).alias("jsonoutput"))
		.toJSON
		.select(
			get_json_object(col("value"), "$.name").as("name"),
			get_json_object(col("value"), "$.dob").as("dateofbirth"),
			get_json_object(col("value"), "$.jsonoutput").as("jsonoutput")
		))*/

}
