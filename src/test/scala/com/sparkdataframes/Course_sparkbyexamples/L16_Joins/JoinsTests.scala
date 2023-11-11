package com.sparkdataframes.Course_sparkbyexamples.L16_Joins




import com.sparkdataframes.OnlineTutorials.Course_sparkbyexamples.SQLTutorial.L16_JoinsStudy



import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._
import util.DataFrameCheckUtils._



/**
 *
 */
class JoinsTests {


	import com.sparkdataframes.Course_sparkbyexamples.L16_Joins.JoinsData._




	// TESTING
	//  Inner join Tests- use to match dataframes on KEY columns, and where KEYS don't match, the rows get dropped
	//  from both datasets

	// Testing if even if empdf has a string col, can conversion to int col and thsus comparison to deptdf, still
	// take place?
	val ij_convertStrColToInt = L16_JoinsStudy.InnerJoinSpecs[String, Int, Int](empDF_strCol, deptDF, "emp_dept_id",
		StringType, "dept_id", IntegerType)
	ij_convertStrColToInt.testColumnAggregationForInnerJoin
	ij_convertStrColToInt.testColumnTypesForInnerJoin
	ij_convertStrColToInt.testIntersectedColumnsForInnerJoin

	val ij_keepColAsInt = L16_JoinsStudy.InnerJoinSpecs[Int, Int, Int](empDF_intCol, deptDF, "emp_dept_id",
		IntegerType, "dept_id", IntegerType)
	ij_keepColAsInt.testColumnAggregationForInnerJoin
	ij_keepColAsInt.testColumnTypesForInnerJoin
	ij_keepColAsInt.testIntersectedColumnsForInnerJoin


	val ij_convertColToStr = L16_JoinsStudy.InnerJoinSpecs[String, Int, String](empDF_strCol, deptDF, "emp_dept_id",
		StringType, "dept_id", IntegerType)
	ij_convertColToStr.testColumnAggregationForInnerJoin
	ij_convertColToStr.testColumnTypesForInnerJoin
	ij_convertColToStr.testIntersectedColumnsForInnerJoin


	// --------------------

	// TESTING: outer join tests --- Outer join returns all rows from both dataframes, and where join expression
	//  doesn’t match it returns null on the respective record columns.

	val oj = L16_JoinsStudy.OuterJoinSpecs[String, Int, Int](empDFExtra_strCol, deptDF, "emp_dept_id", StringType,
		"dept_id", IntegerType)
	oj.testSamnessOfAllKindsOfOuterJoins

	oj.testColumnAggregationForOuterJoin

	oj.testIntersectedColumnsForOuterJoin
	oj.testColumnTypesForOuterJoin
	oj.testMismatchedRowsForOuterJoin

	oj.testDifferingRecordsHaveNullsInOuterJoin
	oj.testMatchingRecordsDontHaveNullsInOuterJoin
	oj.testOuterJoinKeepsAllLeftAndRightRecordsRegardlessOfMatch

	// ---------------------------------


	// TESTING  Left outer join returns all rows from the left dataframe / dataset regardless of
	//  the match found on the right data set; shows the null row componenets only where the left df doesn't match
	//  the right df (and drops records from right df where match wasn't found)
	// SIMPLE: just keeps the intersect + left differences, no right differences.
	val loj = L16_JoinsStudy.LeftOuterJoinSpecs[String, Int, Int](empDFExtra_strCol, deptDF, "emp_dept_id", StringType,
		"dept_id", IntegerType)

	loj.testSamnessOfAllKindsOfLeftOuterJoins

	loj.testColumnAggregationForLeftOuterJoin

	loj.testIntersectedColumnsForLeftOuterJoin
	loj.testColumnTypesForLeftOuterJoin
	loj.testMismatchedRowsForLeftOuterJoin

	loj.testDifferingRecordsHaveNullsInLeftOuterJoin
	loj.testMatchingRecordsDontHaveNullsInLeftOuterJoin

	loj.TestLeftOuterJoinKeepsAllLeftRecordsAndDropsDifferingRightRecords.byChecking_LeftOuterJoinColIntersectsLeftDFColExactly
	loj.TestLeftOuterJoinKeepsAllLeftRecordsAndDropsDifferingRightRecords.byChecking_LeftOuterJoinColEqualsLeftDFCol
	loj.TestLeftOuterJoinKeepsAllLeftRecordsAndDropsDifferingRightRecords.byChecking_DiffsOfLeftToRightAreKeptInLeftOuterJoin
	loj.TestLeftOuterJoinKeepsAllLeftRecordsAndDropsDifferingRightRecords.byChecking_DiffsOfRightToLeftAreDroppedInLeftOuterJoin


	// ---------------------------------

	// TESTING: right outer joins --- Right Outer join is opposite of left join, here it returns all rows
	//  from the right DataFrame/Dataset regardless of match found on the left dataset.
	//  When join expression doesn’t  match, it assigns null for that record and drops records from left where match not found.
	val roj = L16_JoinsStudy.RightOuterJoinSpecs[String, Int, Int](empDFExtra_strCol, deptDF, "emp_dept_id", StringType,
		"dept_id", IntegerType)
	roj.testSamnessOfAllKindsOfRightOuterJoins

	roj.testColumnAggregationForRightOuterJoin

	roj.testIntersectedColumnsForRightOuterJoin
	roj.testColumnTypesForRightOuterJoin
	roj.testMismatchedRowsForRightOuterJoin

	roj.testDifferingRecordsHaveNullsInRightOuterJoin
	roj.testMatchingRecordsDontHaveNullsInRightOuterJoin

	roj.TestRightOuterJoinKeepsAllRightRecordsAndDropsDifferingLeftRecords.byChecking_RightOuterJoinColIntersectsRightDFColExactly
	roj.TestRightOuterJoinKeepsAllRightRecordsAndDropsDifferingLeftRecords.byChecking_RightOuterJoinColEqualsRightDFCol
	roj.TestRightOuterJoinKeepsAllRightRecordsAndDropsDifferingLeftRecords.byChecking_DiffsOfRightToLeftAreKeptInRightOuterJoin
	roj.TestRightOuterJoinKeepsAllRightRecordsAndDropsDifferingLeftRecords.byChecking_DiffsOfLeftToRightAreDroppedInRightOuterJoin



	// TESTING: left semi joins
	// Left semi join is just like inner join, but drops the columns from the right dataframe, while keeping all the
	// columns from the left dataframe. Also, it only returns the left df's columns for which the records match.
	// NOTE: "leftsemi" == "semi"
	val lsj = L16_JoinsStudy.LeftSemiJoinSpecs[String, Int, Int](empDFExtra_strCol, deptDF, "emp_dept_id", StringType,
		"dept_id", IntegerType)

	lsj.testIntersectedColumnsForLeftSemiJoin
	lsj.testColumnTypesForLeftSemiJoin

	lsj.testLeftSemiJoinLacksRightDFColumns

	lsj.TestLeftSemiJoinHasNoMismatchedRows.byChecking_LeftSemiColDoesNotEqualTheLeftToRightColDiffs
	lsj.TestLeftSemiJoinHasNoMismatchedRows.byChecking_LeftSemiColDiffsAreEmpty
	lsj.TestLeftSemiJoinHasNoMismatchedRows.byChecking_LeftSemiRowsLackNullsSinceOnlyLeftColsAreKept
	lsj.TestLeftSemiJoinHasNoMismatchedRows.byChecking_LeftSemiColIntersectingRightColMatchesLeftCol
	lsj.TestLeftSemiJoinHasNoMismatchedRows.byChecking_LeftSemiColIsDisjointFromTheLeftToRightColDiffs


	// TODO left off here - make left semi tests symmetrical to left anti tests + also do the self-join tests.


	//TESTING: Left anti join -  Left-anti join is exact opposite of left semi join - it returns only the columns from the left dataframe for
	// non-matched records. Also, like leftSemiJoin, leftAntiJoin does not keep columns from the right df.
	val laj = L16_JoinsStudy.LeftAntiJoinSpecs[String, Int, Int](empDFExtra_strCol, deptDF, "empt_dept_id", StringType,
		"dept_id", IntegerType)

	laj.testColumnTypesForLeftAntiJoin
	laj.testIntersectedColumnsForLeftAntiJoin

	laj.TestLeftAntiDropsRightDFColumns.byChecking_LeftAntiJoinLacksRightDFCols
	laj.TestLeftAntiDropsRightDFColumns.byChecking_LeftAntiJoinHasSameColnamesAsLeftDF

	laj.TestLeftAntiJoinKeepsOnlyMismatchedRows.byChecking_LeftAntiJoinKeepsOnlyMismatchesFromLeftNotRight
	laj.TestLeftAntiJoinKeepsOnlyMismatchedRows.byChecking_LeftAntiColHasNonEmptyLeftToRightDiffs
	laj.TestLeftAntiJoinKeepsOnlyMismatchedRows.byChecking_LeftAntiPlusLeftSemiIsLeftOuter
	laj.TestLeftAntiJoinKeepsOnlyMismatchedRows.byChecking_LeftAntiRowsLackNullsSinceOnlyLeftColsAreKept
	laj.TestLeftAntiJoinKeepsOnlyMismatchedRows.byChecking_LeftAntiColsEqualTheLeftToRightDiffs
	laj.TestLeftAntiJoinKeepsOnlyMismatchedRows.byChecking_LeftAntiHasNoRightDFRecords


}
