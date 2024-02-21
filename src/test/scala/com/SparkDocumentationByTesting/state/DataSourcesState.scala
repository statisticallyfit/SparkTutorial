package com.SparkDocumentationByTesting.state

import org.apache.spark.sql.types._

/**
 *
 */

object DataSourcesState {

	val manualAirplaneSchema = StructType(Seq(
		StructField("TYPE", StringType),
		StructField("COUNTRY", StringType),
		StructField("CITY", StringType),
		StructField("ENGINES", IntegerType),
		StructField("FIRST_FLIGHT", StringType),
		StructField("NUMBER_BUILT", IntegerType)
	))

	val manualFlightSchema: StructType = (new StructType()
		.add("DEST_COUNTRY_NAME", StringType)
		.add("ORIGIN_COUNTRY_NAME", StringType)
		.add("count", LongType))
}
