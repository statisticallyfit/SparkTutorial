//package com.sparkdataframes.Course_sparkbyexamples.L16_Joins
//
//
//
//
//import com.MySharedSparkContext
//
//import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.types.DataType
//import org.apache.spark.sql.{DataFrame, Row}
//import util.DataFrameCheckUtils._
//
//import scala.reflect.runtime.universe._
//
//
//import org.scalatest.funspec.AnyFunSpec
//import org.scalatest.matchers.should._
//
//
//// L = scala type relating to the column of the left df
//// R = scala type relating to the column of the right df (e.g. coltype of rightDF can be 'IntegerType' so user
//// would have to pass in 'Int' or 'Integer')
//// T = target type (for instance may want to convert the leftDF with LEFT  col type into RIGHT type from rightDf
//// col)
//
///**
// *
// * @param leftDF
// * @param rightDF
// * @param leftColname
// * @param givenLeftDataType
// * @param rightColname
// * @param givenRightDataType
// * @param typeTag$LEFT$0
// * @param typeTag$RIGHT$0
// * @param typeTag$TARGET$0
// * @tparam LEFT
// * @tparam RIGHT
// * @tparam TARGET
// *
// * Inner join is used to join two DataFrames/Datasets on key columns, and where keys don’t match the rows get
// * dropped from both datasets
// */
//
///*trait InnerJoinTrait[LEFT, RIGHT, TARGET] {
//
//	val leftDF: DataFrame
//	val rightDF: DataFrame
//	val leftColname: String
//	val rightColname: String
//	val givenLeftDataType: DataType
//	val givenRightDataType: DataType
//
//}*/
//
//
//trait InnerJoinTrait {this: AnyFunSpec with Matchers with MySharedSparkContext =>
//
//}
//
//case class InnerJoinTests[LEFT: TypeTag, RIGHT: TypeTag, TARGET: TypeTag](leftDF: DataFrame,
//														    rightDF: DataFrame,
//														    leftColname: String, givenLeftDataType: DataType,
//														    rightColname: String, givenRightDataType: DataType)  {
//
//	this: AnyFunSpec with Matchers with MySharedSparkContext =>
//
//
//	info("INNER JOIN DESCRIPTION: inner join joins two dataframes on selected columns, and where keys don't match, the rows containing those keys get dropped from both left and right dfs. ")
//
//
//	info(s"typeOfColumn(leftDF, leftColname).toString = ${typeOfColumn(leftDF, leftColname).toString}")
//	info(s"typeOf[LEFT].toString = ${typeOf[LEFT].toString}")
//	info(s"typeOfColumn(rightDF, rightColname).toString = ${typeOfColumn(rightDF, rightColname).toString}")
//	info(s"typeOf[RIGHT].toString = ${typeOf[RIGHT].toString}")
//	info(s"typeOf[TARGET].toString = ${typeOf[TARGET].toString}")
//
//
//
//	// Make sure passed types match the df column types
//	assert(typeOfColumn(leftDF, leftColname).toString.contains(typeOf[LEFT].toString) && // check 'Int' contained in 'IntegerType' for instance
//
//		typeOfColumn(rightDF, rightColname).toString.contains(typeOf[RIGHT].toString) &&
//
//		typeOfColumn(leftDF, leftColname) == givenLeftDataType &&
//		typeOfColumn(rightDF, rightColname) == givenRightDataType &&
//
//		//make sure the target type is either left or right type
//		((typeOf[TARGET].toString == typeOf[LEFT].toString)
//			|| (typeOf[TARGET].toString == typeOf[RIGHT].toString))
//	)
//
//
//	val innerJoin = leftDF.join(
//		right = rightDF,
//		joinExprs = leftDF(leftColname) === rightDF(rightColname),
//		joinType = "inner"
//	)
//
//	def showInnerJoin = innerJoin.show(truncate = false)
//
//	def testColumnAggregationForInnerJoin = {
//
//		describe("Test 1: testColumnAggregationForInnerJoin") {
//
//			it("colnames of inner join must be an aggregation of each of the colnames of the joined dataframes") {
//				innerJoin.columns.toList shouldEqual (leftDF.columns.toList ++ rightDF.columns.toList)
//			}
//		}
//		/*assert(innerJoin.columns.toList == (leftDF.columns.toList ++ rightDF.columns.toList),
//			"Test 1: colnames of inner join must be an aggregation of each of the colnames of the joined dataframes"
//		)*/
//	}
//
//	def testIntersectedColumnsForInnerJoin = {
//		// NOTE: converting the left df col to be of type RIGHT (Int) since rightdf (deptdf) col is of type Integer
//		//  while leftdf (empdf) col is of type String
//		val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname) // want to convert empDF string
//		// col
//		// emp-dept-id from string into int to be able to compare this leftCol with the rightCol from dept-df
//		val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname) // get col as int (already int)
//
//		val leftCol_IJ: List[Option[TARGET]] = getColAs[TARGET](innerJoin, leftColname)
//
//		assert(getColAs[TARGET](innerJoin, leftColname).sameElements(getColAs[TARGET](innerJoin, rightColname)),
//			"Test 3: left df col and right df col contain the same elements because those were the matching records. ")
//
//		assert(leftCol_IJ.toSet == lc.toSet.intersect(rc.toSet),
//			"Test 4: Inner join df column elements are a result of intersecting the column elements of left " +
//				"and right df (this indicates matching records)")
//
//		assert(leftCol_IJ.toSet.subsetOf(lc.toSet) &&
//			leftCol_IJ.toSet.subsetOf(rc.toSet),
//			"Test 5: inner join df column elements are subset of left df and right df column elements")
//
//	}
//
//	def testColumnTypesForInnerJoin = {
//		// get types as DataType
//		val leftDataType: DataType = typeOfColumn(leftDF, leftColname)
//		val rightDataType: DataType = typeOfColumn(rightDF, rightColname)
//
//		// confirm the datatypes are (same) as given scala tpes (above) for each col corresponding to colname per each df
//		assert(leftDataType.toString.contains(typeOf[LEFT].toString)) // test e.g. "IntegerType" corresponds to
//		// passed type "Int" or "Integer"
//		assert(rightDataType.toString.contains(typeOf[RIGHT].toString))
//
//		// Do the desired test: check that type of each column per df is indeed the datatype that corresponds to the
//		// passed type
//		assert(typeOfColumn(leftDF, leftColname) == givenLeftDataType &&
//			typeOfColumn(innerJoin, leftColname) == givenLeftDataType,
//			"Test: the left column of inner join df has same data type as that of column in the left df"
//		)
//		assert(givenLeftDataType == leftDataType) // followup for consistency
//
//		assert(typeOfColumn(rightDF, rightColname) == givenRightDataType &&
//			typeOfColumn(innerJoin, rightColname) == givenRightDataType,
//			"Test: the right column of inner join df has same data type as that of column in the right df"
//		)
//		assert(givenRightDataType == rightDataType) // followup for consistency
//	}
//
//	def testInnerJoin: Unit = {
//		testColumnAggregationForInnerJoin
//		testColumnTypesForInnerJoin
//		testIntersectedColumnsForInnerJoin
//	}
//}