package com.sparkscalaexamples.SQLTutorial


import org.apache.spark.sql.{DataFrame, Row, Column, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.catalyst.plans._

import org.apache.spark.sql.types.{DataType, StringType, IntegerType, BooleanType, DoubleType, StructField, StructType}
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._


import util.DataFrameCheckUtils._


/**
 *
 */

object SparkJoins {



	// L = scala type relating to the column of the left df
	// R = scala type relating to the column of the right df (e.g. coltype of rightDF can be 'IntegerType' so user
	// would have to pass in 'Int' or 'Integer')
	// T = target type (for instance may want to convert the leftDF with LEFT  col type into RIGHT type from rightDf
	// col)
	case class TestInnerJoin[LEFT: TypeTag, RIGHT: TypeTag, TARGET: TypeTag](leftDF: DataFrame,
															   rightDF: DataFrame,
															   leftColname: String, givenLeftDataType: DataType,
															   rightColname: String, givenRightDataType: DataType) {


		// Make sure passed types match the df column types
		assert(typeOfColumn(leftDF, leftColname).toString.contains(typeOf[LEFT].toString) && // check 'Int' contained in 'IntegerType' for instance

			typeOfColumn(rightDF, rightColname).toString.contains(typeOf[RIGHT].toString) &&

			typeOfColumn(leftDF, leftColname) == givenLeftDataType &&
			typeOfColumn(rightDF, rightColname) == givenRightDataType &&

			//make sure the target type is either left or right type
			((typeOf[TARGET].toString == typeOf[LEFT].toString)
				|| (typeOf[TARGET].toString == typeOf[RIGHT].toString))
		)


		val innerJoin = leftDF.join(
			right = rightDF,
			joinExprs = leftDF(leftColname) === rightDF(rightColname),
			joinType = "inner"
		)

		def showInnerJoin = innerJoin.show(truncate = false)

		def testColumnAggregationForInnerJoin: Unit = {

			assert(innerJoin.columns.toList == (leftDF.columns.toList ++ rightDF.columns.toList),
				"Test 1: colnames of inner join must be an aggregation of each of the colnames of the joined dataframes"
			)
		}

		def testIntersectedColumnsForInnerJoin = {
			// NOTE: converting the left df col to be of type RIGHT (Int) since rightdf (deptdf) col is of type Integer
			//  while leftdf (empdf) col is of type String
			val leftCol: List[TARGET] = getColAs[TARGET](leftDF, leftColname) // want to convert empDF string col
			// emp-dept-id from string into int to be able to compare this leftCol with the rightCol from dept-df
			val rightCol: List[TARGET] = getColAs[TARGET](rightDF, rightColname) // get col as int (already int)
			val commonColElems: Set[TARGET] = leftCol.toSet.intersect(rightCol.toSet)
			val innerCol = getColAs[TARGET](innerJoin, leftColname)

			assert(getColAs[TARGET](innerJoin, leftColname).sameElements(getColAs[TARGET](innerJoin, rightColname)),
				"Test 3: left df col and right df col contain the same elements because those were the matching records. ")

			assert(innerCol.toSet == commonColElems, "Test 4: Inner join df column elements are a result of " +
				"intersecting the column elements of left and right df (this indicates matching records)")

			assert(innerCol.toSet.subsetOf(leftCol.toSet) &&
				innerCol.toSet.subsetOf(rightCol.toSet),
				"Test 5: inner join df column elements are subset of left df and right df column elements")

		}

		def testColumnTypesForInnerJoin = {
			// get types as DataType
			val leftDataType: DataType = typeOfColumn(leftDF, leftColname)
			val rightDataType:DataType = typeOfColumn(rightDF, rightColname)

			// confirm the datatypes are (same) as given scala tpes (above) for each col corresponding to colname per each df
			assert(leftDataType.toString.contains(typeOf[LEFT].toString)) // test e.g. "IntegerType" corresponds to
			// passed type "Int" or "Integer"
			assert(rightDataType.toString.contains(typeOf[RIGHT].toString))

			// Do the desired test: check that type of each column per df is indeed the datatype that corresponds to the
			// passed type
			assert(typeOfColumn(leftDF, leftColname) == givenLeftDataType &&
				typeOfColumn(innerJoin, leftColname) == givenLeftDataType,
				"Test: the left column of inner join df has same data type as that of column in the left df"
			)
			assert(givenLeftDataType == leftDataType) // followup for consistency

			assert(typeOfColumn(rightDF, rightColname) == givenRightDataType &&
				typeOfColumn(innerJoin, rightColname) == givenRightDataType,
				"Test: the right column of inner join df has same data type as that of column in the right df"
			)
			assert(givenRightDataType == rightDataType) // followup for consistency
		}

		def testInnerJoin: DataFrame = {
			testColumnAggregationForInnerJoin
			testColumnTypesForInnerJoin
			testIntersectedColumnsForInnerJoin

			innerJoin
		}
	}



	// --------------------------------------------------------------------------------------------------------

	// L = scala type relating to the column of the left df
	// R = scala type relating to the column of the right df (e.g. coltype of rightDF can be 'IntegerType' so user
	// would have to pass in 'Int' or 'Integer')
	// T = target type (for instance may want to convert the leftDF with LEFT  col type into RIGHT type from rightDf
	// col)
	case class TestOuterJoin[LEFT: TypeTag, RIGHT: TypeTag, TARGET: TypeTag](leftDF: DataFrame,
															   rightDF: DataFrame,
															   leftColname: String, givenLeftDataType: DataType,
															   rightColname: String, givenRightDataType: DataType) {


		// Make sure passed types match the df column types
		assert(typeOfColumn(leftDF, leftColname).toString.contains(typeOf[LEFT].toString) && // check 'Int' contained in 'IntegerType' for instance

			typeOfColumn(rightDF, rightColname).toString.contains(typeOf[RIGHT].toString) &&

			typeOfColumn(leftDF, leftColname) == givenLeftDataType &&
			typeOfColumn(rightDF, rightColname) == givenRightDataType &&

			//make sure the target type is either left or right type
			((typeOf[TARGET].toString == typeOf[LEFT].toString)
				|| (typeOf[TARGET].toString == typeOf[RIGHT].toString))
		)


		val outerJoin: DataFrame = leftDF.join(right = rightDF,
			joinExprs = leftDF(leftColname) === rightDF(rightColname),
			joinType = "outer"
		)

		val fullJoin = leftDF.join(rightDF, leftDF(leftColname) === rightDF(rightColname), joinType = "full")

		val fullOuterJoin = leftDF.join(rightDF, leftDF(leftColname) === rightDF(rightColname), joinType =
			"fullouter")


		private val (oc, fc, foc) = (outerJoin.collect, fullJoin.collect, fullOuterJoin.collect)


		def testSamnessOfAllKindsOfOuterJoins = {
			assert(oc.sameElements(fc) && fc.sameElements(foc),
			"Test all outer join result in same df")
		}

		def testColumnAggregationForOuterJoin: Unit = {

			assert(outerJoin.columns.toList == (leftDF.columns.toList ++ rightDF.columns.toList),
				"Test 1: colnames of outerJoin must be an aggregation of each of the colnames of the joined dataframes"
			)
		}

		def testColumnTypesForOuterJoin = {
			// get types as DataType
			val leftDataType: DataType = typeOfColumn(leftDF, leftColname)
			val rightDataType:DataType = typeOfColumn(rightDF, rightColname)

			// confirm the datatypes are (same) as given scala tpes (above) for each col corresponding to colname per each df
			assert(leftDataType.toString.contains(typeOf[LEFT].toString)) // test e.g. "IntegerType" corresponds to
			// passed type "Int" or "Integer"
			assert(rightDataType.toString.contains(typeOf[RIGHT].toString))

			// Do the desired test: check that type of each column per df is indeed the datatype that corresponds to the
			// passed type
			assert(typeOfColumn(leftDF, leftColname) == givenLeftDataType &&
				typeOfColumn(outerJoin, leftColname) == givenLeftDataType,
				"Test: the left column of inner join df has same data type as that of column in the left df"
			)
			assert(givenLeftDataType == leftDataType) // followup for consistency

			assert(typeOfColumn(rightDF, rightColname) == givenRightDataType &&
				typeOfColumn(outerJoin, rightColname) == givenRightDataType,
				"Test: the right column of inner join df has same data type as that of column in the right df"
			)
			assert(givenRightDataType == rightDataType) // followup for consistency
		}

		def testMismatchedRowsForOuterJoin = {

			// TESTING 1 = have `getMismatchRows` function to do it automatically
			// ldr = mismatch rows of left df relative to right df
			// rdl = mistmatch rows of right df relative to left df
			val (leftMismatchRows, rightMismatchRows) = getMismatchRows[TARGET](leftDF, rightDF, leftColname, rightColname)

			// TESTING 2 = have another way to do it, shorter way, using spark's `where` function
			// lc = left col
			val lc: List[TARGET] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[TARGET] = getColAs[TARGET](rightDF, rightColname)

			// left to right mistmatch rows
			val ldr: List[Row] = lc.toSet.diff(rc.toSet)
				.toList
				.flatMap(diffElem => outerJoin.where(outerJoin.col(leftColname) === diffElem).collect.toList)

			// right to left mismatch rows
			val rdl: List[Row] = rc.toSet.diff(lc.toSet)
				.toList
				.flatMap(diffElem => outerJoin.where(outerJoin.col(rightColname) === diffElem).collect.toList)


			assert(leftMismatchRows == ldr, "Test: non-matching rows of first df with respect to second " +
				"df (the two methods must yield same results)")
			assert(rightMismatchRows == rdl, "Test: non-matching rows of second df with respect to first" +
				" df (the two methods must yield same results)")

			assert(ldr.map(row => row.toSeq.takeRight(rightDF.columns.length).forall(_ == null)).forall(_ == true),
			"Test: for leftdf relative to right df, the last elements in the row (size as large as rightdf width) " +
				"that don't match, are always null")

			assert(rdl.map(row => row.toSeq.take(leftDF.columns.length).forall(_ == null)).forall(_ == true),
			"Test: for rightdf relative to leftdf, the first few elements in the row (size as large as leftdf " +
				"width) that don't match, are always null")
		}


		// * for each element in leftcol that is not in rightcol, there corresponds a null at that spot in the left
		// col (check index + that there is null at that index in rightcol)
		// e.g. for elem "50" in emp_dept_id leftcol at index i = 5, there is a l at i = 5 for dept_id in rightcol
		// of outer join
		def testNullLocationsColumnwise = {
			val leftColOuter = getColAs[TARGET](outerJoin, leftColname)
			val rightColOuter = getColAs[TARGET](outerJoin, rightColname)


			def recordNullSpotsColumnwise[T](leftColOuter: List[T],
									   rightColOuter: List[T],
									   rightDF: DataFrame,
									   outerJoinDF: DataFrame
									  ): List[Array[Boolean]] = {

				// indices corresponding to different elems from left df vs. right df
				val diffsLeftVsRight: Set[Int] = leftColOuter.toSet.diff(rightColOuter.toSet).map(diffElem =>
					leftColOuter.indexOf(diffElem))

				// Get cols corresponding to right df from the outer join (to have the nulls from oute rjoin)
				val rightColsToCheckLeftDiffs: Array[List[Any]] = rightDF.columns.map(colNameStr => outerJoinDF
					.select
				(colNameStr)
					.collect.map(row => row(0)).toList)

				// For each different elem from leftdf vs right df (now recorded as index corresponding to that elem, check
				// that corresponding position in the other df contains a null
				// (e.g. elem 50 is diff in leftvsright --> occurs at index i = 5 columnwise ---> check that in right df
				//  there is null at i = 5)
				diffsLeftVsRight.toList.map(diffIndex => rightColsToCheckLeftDiffs.map(colList => colList
				(diffIndex) ==	null))

			}

			assert(recordNullSpotsColumnwise[TARGET](leftColOuter, rightColOuter, rightDF, outerJoin).forall(arr
			=> arr.forall(_ == true)),
			"Test: all elements that don't match (left vs. right df) in outer join, should correspond to a null in" +
				" the right part of outer join")

			assert(recordNullSpotsColumnwise[TARGET](rightColOuter, leftColOuter, leftDF, outerJoin).forall(arr
			=> arr.forall(_ == true)),
				"Test: all elements that don't match (right vs. left df) in outer join, should correspond to a " +
					"null in" +
					" the left part of outer join")

			// -------------------------------------------------------
			// Getting the different elems from left df col and checking the index spot in the rightdf (diff elem = 50,
			// i = 5)
			val diffsLeftVsRight = leftColOuter.toSet.diff(rightColOuter.toSet).map(diffElem =>	leftColOuter.indexOf(diffElem))
			/*val i = leftDF.columns.length
			val colsOnRightToCheckLeftDiffs: Array[Column] = outerJoin.columns.slice(i, i + rightDF.columns.length).map(colnameStr =>	col(colnameStr))
			outerJoin.select(colsOnRightToCheckLeftDiffs:_*).show
			*/
			val rightColsToCheckLeftDiffs = rightDF.columns.map(colNameStr => outerJoin.select(colNameStr).collect.map(row => row(0)).toList)

			// For each different elem from leftdf vs right df (now recorded as index corresponding to that elem, check
			// that corresponding position in the other df contains a null
			// (e.g. elem 50 is diff in leftvsright --> occurs at index i = 5 columnwise ---> check that in right df
			//  there is null at i = 5)
			diffsLeftVsRight.map(diffIndex => rightColsToCheckLeftDiffs.map(colList => colList(diffIndex) == null)).forall(arr =>
				arr.forall(_ == true))






			// Getting the diff elems from rightdf col and checking the index spot in the left df (diff elem = 30,
			// index = 6)

			val diffsRightVsLeft = rightColOuter.toSet.diff(leftColOuter.toSet).map(diffElem => rightColOuter.indexOf(diffElem))
			/*val colsOnLeftToCheckRightDiffs: Array[Column] = outerJoin.columns.slice(0, leftDF.columns.length).map(colnameStr =>	col(colnameStr))
			outerJoin.select(leftColsToCheckRightDiffs:_*).show
			*/
			val leftColsToCheckRightDiffs = leftDF.columns.map(colNameStr => outerJoin.select(colNameStr).collect.map(row => row(0)).toList)
			diffsRightVsLeft.map(diffIndex => leftColsToCheckRightDiffs.map(colList => colList(diffIndex) == null)).forall(arr => arr.forall(_ == true))

		}




		/*

		def testIntersectedColumnsForInnerJoin = {
			// NOTE: converting the left df col to be of type RIGHT (Int) since rightdf (deptdf) col is of type Integer
			//  while leftdf (empdf) col is of type String
			val leftCol: List[TARGET] = getColAs[TARGET](leftDF, leftColname) // want to convert empDF string col
			// emp-dept-id from string into int to be able to compare this leftCol with the rightCol from dept-df
			val rightCol: List[TARGET] = getColAs[TARGET](rightDF, rightColname) // get col as int (already int)
			val commonColElems: Set[TARGET] = leftCol.toSet.intersect(rightCol.toSet)
			val innerCol = getColAs[TARGET](innerJoin, leftColname)

			assert(getColAs[TARGET](innerJoin, leftColname).sameElements(getColAs[TARGET](innerJoin, rightColname)),
				"Test 3: left df col and right df col contain the same elements because those were the matching records. ")

			assert(innerCol.toSet == commonColElems, "Test 4: Inner join df column elements are a result of " +
				"intersecting the column elements of left and right df (this indicates matching records)")

			assert(innerCol.toSet.subsetOf(leftCol.toSet) &&
				innerCol.toSet.subsetOf(rightCol.toSet),
				"Test 5: inner join df column elements are subset of left df and right df column elements")

		}



		def testInnerJoin: DataFrame = {
			testColumnAggregationForInnerJoin
			testColumnTypesForInnerJoin
			testIntersectedColumnsForInnerJoin

			innerJoin
		}*/
	}

}


