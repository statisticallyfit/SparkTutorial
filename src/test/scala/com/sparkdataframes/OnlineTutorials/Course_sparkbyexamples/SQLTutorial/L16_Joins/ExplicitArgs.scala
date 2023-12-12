package com.sparkdataframes.OnlineTutorials.Course_sparkbyexamples.SQLTutorial.L16_Joins

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row}

/**
 *
 */
class ExplicitArgs(
				   leftDF: DataFrame,
				   rightDF: DataFrame,
				   leftColname: String, givenLeftDataType: DataType,
				   rightColname: String, givenRightDataType: DataType
			   )
object ExplicitArgs {
	def applying(
				  leftDF: DataFrame,
				  rightDF: DataFrame,
				  leftColname: String, givenLeftDataType: DataType,
				  rightColname: String, givenRightDataType: DataType
			  ) = new ExplicitArgs(leftDF, rightDF, leftColname, givenLeftDataType, rightColname, givenRightDataType)
}
