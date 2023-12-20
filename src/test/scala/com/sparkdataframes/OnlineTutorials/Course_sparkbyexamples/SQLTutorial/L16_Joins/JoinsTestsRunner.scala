package com.sparkdataframes.OnlineTutorials.Course_sparkbyexamples.SQLTutorial.L16_Joins

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}



/**
 *
 */
class JoinsTestsRunner  {


	import JoinsData._

	val spark: SparkSession = SparkSession.builder()
		.master("local[1]")
		.appName("SparkByExamples.com")
		.getOrCreate()
	// for console
	//val spark: SparkSession = SparkSession.builder().master("local[1]").appName("SparkByExamples.com").getOrCreate()

	import spark.implicits._

	// ---------------------------------------------------------------------------


	final val LEFT_EMPSTRCOL_DF: DataFrame = empDFExtra_strCol
	final val LEFT_EMP_COLNAME: String = "emp_dept_id"
	final val RIGHT_DEPT_DF: DataFrame = deptDF
	final val RIGHT_DEPT_COLNAME: String = "dept_id"


	/*// TESTING
	//  Inner join Tests- use to match dataframes on KEY columns, and where KEYS don't match, the rows get dropped
	//  from both datasets

	// Testing if even if empdf has a string col, can conversion to int col and thus comparison to RIGHT_DEPT_DF, still
	// take place?
	val ij_convertStrColToInt = InnerJoinSpecs[String, Int, Int](empDF_strCol, RIGHT_DEPT_DF, LEFT_EMP_COLNAME,
		StringType, RIGHT_DEPT_COLNAME, IntegerType)
	ij_convertStrColToInt.testColumnAggregationForInnerJoin
	ij_convertStrColToInt.testColumnTypesForInnerJoin
	ij_convertStrColToInt.testIntersectedColumnsForInnerJoin

	val ij_keepColAsInt = InnerJoinSpecs[Int, Int, Int](empDF_intCol, RIGHT_DEPT_DF, LEFT_EMP_COLNAME,
		IntegerType, RIGHT_DEPT_COLNAME, IntegerType)
	ij_keepColAsInt.testColumnAggregationForInnerJoin
	ij_keepColAsInt.testColumnTypesForInnerJoin
	ij_keepColAsInt.testIntersectedColumnsForInnerJoin


	val ij_convertColToStr = InnerJoinSpecs[String, Int, String](LEFT_EMPSTRCOL_DF, RIGHT_DEPT_DF, LEFT_EMP_COLNAME,
		StringType, RIGHT_DEPT_COLNAME, IntegerType)
	ij_convertColToStr.testColumnAggregationForInnerJoin
	ij_convertColToStr.testColumnTypesForInnerJoin
	ij_convertColToStr.testIntersectedColumnsForInnerJoin
*/

	// --------------------

	// TESTING: outer join tests --- Outer join returns all rows from both dataframes, and where join expression
	//  doesn’t match it returns null on the respective record columns.

	val oj = L16_Joins.OuterJoinSpecs[String, Int, Int](LEFT_EMPSTRCOL_DF, RIGHT_DEPT_DF, LEFT_EMP_COLNAME, StringType,
		RIGHT_DEPT_COLNAME, IntegerType)
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
	val loj = L16_Joins.LeftOuterJoinSpecs[String, Int, Int](LEFT_EMPSTRCOL_DF, RIGHT_DEPT_DF, LEFT_EMP_COLNAME, StringType,
		RIGHT_DEPT_COLNAME, IntegerType)

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
	val roj = L16_Joins.RightOuterJoinSpecs[String, Int, Int](LEFT_EMPSTRCOL_DF, RIGHT_DEPT_DF, LEFT_EMP_COLNAME, StringType,
		RIGHT_DEPT_COLNAME, IntegerType)
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
	val lsj = L16_Joins.LeftSemiJoinSpecs[String, Int, Int](LEFT_EMPSTRCOL_DF, RIGHT_DEPT_DF, LEFT_EMP_COLNAME, StringType,
		RIGHT_DEPT_COLNAME, IntegerType)

	lsj.testIntersectedColumnsForLeftSemiJoin
	lsj.testColumnTypesForLeftSemiJoin

	lsj.TestLeftSemiDropsRightDFColumns.byChecking_LeftSemiJoinLacksRightDFCols
	lsj.TestLeftSemiDropsRightDFColumns.byChecking_LeftSemiJoinHasSameColnamesAsLeftDF

	lsj.TestLeftSemiJoinKeepsOnlyMatchingRows.byChecking_LeftSemiColDoesNotEqualTheLeftToRightColDiffs
	lsj.TestLeftSemiJoinKeepsOnlyMatchingRows.byChecking_LeftSemiColDiffsAreEmpty
	lsj.TestLeftSemiJoinKeepsOnlyMatchingRows.byChecking_LeftSemiRowsLackNullsSinceOnlyLeftColsAreKept
	lsj.TestLeftSemiJoinKeepsOnlyMatchingRows.byChecking_LeftSemiColIntersectingRightColMatchesLeftCol
	lsj.TestLeftSemiJoinKeepsOnlyMatchingRows.byChecking_LeftSemiColIsDisjointFromTheLeftToRightColDiffs





	//TESTING: Left anti join -  Left-anti join is exact opposite of left semi join - it returns only the columns from the left dataframe for
	// non-matched records. Also, like leftSemiJoin, leftAntiJoin does not keep columns from the right df.

	val laj = L16_Joins.LeftAntiJoinSpecs[String, Int, Int](LEFT_EMPSTRCOL_DF, RIGHT_DEPT_DF, LEFT_EMP_COLNAME, StringType,
		RIGHT_DEPT_COLNAME, IntegerType)

	laj.testColumnTypesForLeftAntiJoin
	laj.testIntersectedColumnsForLeftAntiJoin

	laj.TestLeftAntiDropsRightDFColumns.byChecking_LeftAntiJoinLacksRightDFCols
	laj.TestLeftAntiDropsRightDFColumns.byChecking_LeftAntiJoinHasSameColnamesAsLeftDF

	laj.TestLeftAntiJoinKeepsOnlyMismatchedRows.byChecking_LeftAntiPlusLeftSemiIsLeftOuter
	laj.TestLeftAntiJoinKeepsOnlyMismatchedRows.byChecking_LeftAntiJoinKeepsOnlyMismatchesFromLeftNotRight
	laj.TestLeftAntiJoinKeepsOnlyMismatchedRows.byChecking_LeftAntiColHasNonEmptyLeftToRightDiffs
	laj.TestLeftAntiJoinKeepsOnlyMismatchedRows.byChecking_LeftAntiRowsLackNullsSinceOnlyLeftColsAreKept
	laj.TestLeftAntiJoinKeepsOnlyMismatchedRows.byChecking_LeftAntiHasNoRightDFRecords
	laj.TestLeftAntiJoinKeepsOnlyMismatchedRows.byChecking_LeftAntiColEqualsTheLeftToRightDiffs


}
