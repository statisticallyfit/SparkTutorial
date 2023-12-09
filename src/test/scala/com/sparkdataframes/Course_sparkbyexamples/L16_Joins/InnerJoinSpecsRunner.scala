//package com.sparkdataframes.Course_sparkbyexamples.L16_Joins
//
//
//
//
//import com.MySharedSparkContext
//
//
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}
//
//import util.DataFrameCheckUtils._
//
//import scala.reflect.runtime.universe._
//import org.scalatest.funspec.AnyFunSpec
//import org.scalatest.matchers.should._
//
//
///**
// *
// */
//class InnerJoinSpecsRunner extends AnyFunSpec with Matchers with MySharedSparkContext {
//
//
//	import com.sparkdataframes.Course_sparkbyexamples.L16_Joins.JoinsData._
//
//	/*val spark: SparkSession = SparkSession.builder()
//		.master("local[1]")
//		.appName("SparkByExamples.com")
//		.getOrCreate()
//	// for console
//	//val spark: SparkSession = SparkSession.builder().master("local[1]").appName("SparkByExamples.com").getOrCreate()
//
//	import spark.implicits._*/
//
//
//
//
//
//	// TESTING
//	//  Inner join Tests- use to match dataframes on KEY columns, and where KEYS don't match, the rows get dropped
//	//  from both datasets
//
//	// Testing if even if empdf has a string col, can conversion to int col and thus comparison to deptDF, still
//	// take place?
//	val ij_convertStrColToInt = InnerJoinTests[String, Int, Int](empDF_strCol, deptDF, leftColname,
//		StringType, rightColname, IntegerType)
//	ij_convertStrColToInt.testColumnAggregationForInnerJoin
//	ij_convertStrColToInt.testColumnTypesForInnerJoin
//	ij_convertStrColToInt.testIntersectedColumnsForInnerJoin
//
//	val ij_keepColAsInt = InnerJoinTests[Int, Int, Int](empDF_intCol, deptDF, leftColname,
//		IntegerType, rightColname, IntegerType)
//	ij_keepColAsInt.testColumnAggregationForInnerJoin
//	ij_keepColAsInt.testColumnTypesForInnerJoin
//	ij_keepColAsInt.testIntersectedColumnsForInnerJoin
//
//
//	val ij_convertColToStr = InnerJoinTests[String, Int, String](empDF_strCol, deptDF, leftColname,
//		StringType, rightColname, IntegerType)
//	ij_convertColToStr.testColumnAggregationForInnerJoin
//	ij_convertColToStr.testColumnTypesForInnerJoin
//	ij_convertColToStr.testIntersectedColumnsForInnerJoin
//
//}
