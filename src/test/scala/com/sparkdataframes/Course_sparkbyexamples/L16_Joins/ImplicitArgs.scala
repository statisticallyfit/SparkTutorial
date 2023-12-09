package com.sparkdataframes.Course_sparkbyexamples.L16_Joins


import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame}

/**
 *
 */
class ImplicitArgs(
				   leftDF: DataFrame,
				   rightDF: DataFrame,
				   leftColname: String, givenLeftDataType: DataType,
				   rightColname: String, givenRightDataType: DataType
			   )

object ImplicitArgs {
	def applying(
				  leftDF: DataFrame,
				  rightDF: DataFrame,
				  leftColname: String, givenLeftDataType: DataType,
				  rightColname: String, givenRightDataType: DataType
			  ) = new ImplicitArgs(leftDF, rightDF, leftColname, givenLeftDataType, rightColname, givenRightDataType)
}
