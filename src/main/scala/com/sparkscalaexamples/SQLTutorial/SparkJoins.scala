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
			val leftCol: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname) // want to convert empDF string
			// col
			// emp-dept-id from string into int to be able to compare this leftCol with the rightCol from dept-df
			val rightCol: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname) // get col as int (already int)
			val commonColElems: Set[Option[TARGET]] = leftCol.toSet.intersect(rightCol.toSet)
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

		def testInnerJoin: Unit = {
			testColumnAggregationForInnerJoin
			testColumnTypesForInnerJoin
			testIntersectedColumnsForInnerJoin
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
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			//Prerequisite: Asserting that there are no None's (nulls) - that only happens after join operations,
			// here we are
			// just taking the columns from the original dfs (left and right)
			assert(lc.toSet.diff(rc.toSet).forall(_.isDefined))

			// left to right mistmatch rows
			val ldr: List[Row] = lc.toSet.diff(rc.toSet)
				.toList
				.flatMap(diffElem => outerJoin.where(outerJoin.col(leftColname) === diffElem.get).collect.toList)
			// assertion tests no None's so can just .get out of the Some()

			// right to left mismatch rows
			val rdl: List[Row] = rc.toSet.diff(lc.toSet)
				.toList
				.flatMap(diffElem => outerJoin.where(outerJoin.col(rightColname) === diffElem.get).collect.toList)


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



		/**
		 * For each element in the leftcolumn that is not in rightcol, there corresponds a null at that spot in the
		 * rightcol. This function tests that at that spot, the value is null.
		 *
		 * (e.g. for elem "50" in emp_dept_id empDF at index i = 5, there is
		 * null at i = 5 for dept_id in deptDF  part of the outer join )
		 */
		def testDifferingRecordsHaveNullsInOuterJoin = {
			val leftColOuter: List[Option[TARGET]] = getColAs[TARGET](outerJoin, leftColname)
			val rightColOuter: List[Option[TARGET]] = getColAs[TARGET](outerJoin, rightColname)


			val resLR: Array[List[List[Boolean]]] = recordNullSpotsColumnwise[TARGET](leftColOuter, rightColOuter,rightDF, outerJoin)

			assert(resLR.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
			"Test: all elements that don't match (left vs. right df) in outer join, should correspond to a null in" +
				" the right part of outer join")


			val resRL: Array[List[List[Boolean]]] = recordNullSpotsColumnwise[TARGET](rightColOuter,	leftColOuter, leftDF, outerJoin)

			assert(resRL.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that don't match (right vs. left df) in outer join, should correspond to a " +
					"null in" +
					" the left part of outer join")


		}


		def testMatchingRecordsDontHaveNullsInOuterJoin = {
			// NOTE: converting the left df col to be of type RIGHT (Int) since rightdf (deptdf) col is of type Integer
			//  while leftdf (empdf) col is of type String
			val leftColOuter: List[Option[TARGET]] = getColAs[TARGET](outerJoin, leftColname)
			val rightColOuter: List[Option[TARGET]] = getColAs[TARGET](outerJoin, rightColname)


			val resLR: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](leftColOuter,
				rightColOuter, rightDF, outerJoin)

			assert(resLR.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that DO match (left vs. right df) in outer join, should NOT correspond to a " +
					"null in" +
					" the right part of outer join")

			val resRL: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](rightColOuter,
				leftColOuter, leftDF, outerJoin)

			assert(resRL.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that DO match (right vs. left df) in outer join, should NOT correspond to a " +
					"null in the left part of outer join")
		}


		def testIntersectedColumnsForOuterJoin = {
			val leftCol: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rightCol: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftColOuter: List[Option[TARGET]] = getColAs[TARGET](outerJoin, leftColname)
			val rightColOuter: List[Option[TARGET]] = getColAs[TARGET](outerJoin, rightColname)

			assert(leftCol.length <= leftColOuter.length &&
				leftCol.toSet.subsetOf(leftColOuter.toSet),
				"Test: left df column is a subset of the corresponding left df column in the outer join result"
			)

			assert(rightCol.length <= rightColOuter.length &&
				rightCol.toSet.subsetOf(rightColOuter.toSet),
				"Test: right df column is a subset of the corresponding right df column in the outer join result"
			)
		}

		def testOuterJoin: Unit = {
			// NOTE: schema of tests for all other kinds of outer joins:
			testSamnessOfAllKindsOfOuterJoins

			testIntersectedColumnsForOuterJoin
			testColumnTypesForOuterJoin
			testMismatchedRowsForOuterJoin

			testDifferingRecordsHaveNullsInOuterJoin
			testMatchingRecordsDontHaveNullsInOuterJoin
		}
	}




	// --------------------------------------------------------------------------------------------------------

	// L = scala type relating to the column of the left df
	// R = scala type relating to the column of the right df (e.g. coltype of rightDF can be 'IntegerType' so user
	// would have to pass in 'Int' or 'Integer')
	// T = target type (for instance may want to convert the leftDF with LEFT  col type into RIGHT type from rightDf
	// col)
	case class TestLeftOuterJoin[LEFT: TypeTag, RIGHT: TypeTag, TARGET: TypeTag](leftDF: DataFrame,
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


		val leftOuterJoin: DataFrame = leftDF.join(right = rightDF,
			joinExprs = leftDF(leftColname) === rightDF(rightColname),
			joinType = "left" // "leftouter"
		)
		val leftOuterJoin2 = leftDF.join(rightDF, leftDF(leftColname) === rightDF(rightColname), joinType =
			"leftouter")


		private val (lo, lo2) = (leftOuterJoin.collect, leftOuterJoin2.collect)



		def testSamnessOfAllKindsOfLeftOuterJoins = {
			assert(lo.sameElements(lo2) ,
				"Test all left outer join results in same df")
		}



		def testColumnAggregationForLeftOuterJoin: Unit = {

			assert(leftOuterJoin.columns.toList == (leftDF.columns.toList ++ rightDF.columns.toList),
				"Test 1: colnames of leftOuterJoin must be an aggregation of each of the colnames of the joined " +
					"dataframes"
			)
		}



		def testColumnTypesForLeftOuterJoin = {
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
				typeOfColumn(leftOuterJoin, leftColname) == givenLeftDataType,
				"Test: the left column of inner join df has same data type as that of column in the left df"
			)
			assert(givenLeftDataType == leftDataType) // followup for consistency

			assert(typeOfColumn(rightDF, rightColname) == givenRightDataType &&
				typeOfColumn(leftOuterJoin, rightColname) == givenRightDataType,
				"Test: the right column of inner join df has same data type as that of column in the right df"
			)
			assert(givenRightDataType == rightDataType) // followup for consistency
		}



		def testMismatchedRowsForLeftOuterJoin = {

			// TESTING 1 = have `getMismatchRows` function to do it automatically
			// ldr = mismatch rows of left df relative to right df
			// rdl = mistmatch rows of right df relative to left df
			val (leftMismatchRows, rightMismatchRows) = getMismatchRows[TARGET](leftDF, rightDF, leftColname, rightColname)

			// TESTING 2 = have another way to do it, shorter way, using spark's `where` function
			// lc = left col
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			//Prerequisite: Asserting that there are no None's (nulls) - that only happens after join operations,
			// here we are
			// just taking the columns from the original dfs (left and right)
			assert(lc.toSet.diff(rc.toSet).forall(_.isDefined))

			// left to right mistmatch rows
			val ldr: List[Row] = lc.toSet.diff(rc.toSet)
				.toList
				.flatMap(diffElem => leftOuterJoin.where(leftOuterJoin.col(leftColname) === diffElem.get).collect.toList)
			// assertion tests no None's so can just .get out of the Some()

			// right to left mismatch rows
			val rdl: List[Row] = rc.toSet.diff(lc.toSet)
				.toList
				.flatMap(diffElem => leftOuterJoin.where(leftOuterJoin.col(rightColname) === diffElem.get).collect.toList)


			assert(leftMismatchRows == ldr, "Test: non-matching rows of first df with respect to second " +
				"df (the two methods must yield same results)")
			assert(rightMismatchRows == rdl, "Test: non-matching rows of second df with respect to first" +
				" df (the two methods must yield same results)")

			assert(ldr.map(row => row.toSeq.takeRight(rightDF.columns.length).forall(_ == null)).forall(_ == true),
				"Test: for leftdf relative to right df, the last elements in the row (size as large as rightdf width) " +
					"that don't match, are always null")


			assert(rdl.isEmpty && (ldr.isEmpty || ldr.nonEmpty), "Test: left outer join's keeps non-matching " +
				"records from left df but not from" +
				" " +
				"right df")
		}




		/**
		 * For each element in the leftcolumn that is not in rightcol, there corresponds a null at that spot in the
		 * rightcol. This function tests that at that spot, the value is null.
		 *
		 * (e.g. for elem "50" in emp_dept_id empDF at index i = 5, there is
		 * null at i = 5 for dept_id in deptDF  part of the outer join )
		 */
		def testDifferingRecordsHaveNullsInLeftOuterJoin = {
			val leftColLeftOuter: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, leftColname)
			val rightColLeftOuter: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, rightColname)


			val resLR: Array[List[List[Boolean]]] = recordNullSpotsColumnwise[TARGET](leftColLeftOuter, rightColLeftOuter,rightDF, leftOuterJoin)

			assert(resLR.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that don't match (left vs. right df) in outer join, should correspond to a null in" +
					" the right part of left outer join")


			val resRL: Array[List[List[Boolean]]] = recordNullSpotsColumnwise[TARGET](rightColLeftOuter,	leftColLeftOuter, leftDF, leftOuterJoin)

			assert(resRL.forall(colLst => colLst.isEmpty),
				"Test: in the case of non-matching records, left outer join only keeps the left df non-matches, " +
					"not the right df ones")

		}



		def testMatchingRecordsDontHaveNullsInLeftOuterJoin = {
			// NOTE: converting the left df col to be of type RIGHT (Int) since rightdf (deptdf) col is of type Integer
			//  while leftdf (empdf) col is of type String
			val leftColLeftOuter: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, leftColname)
			val rightColLeftOuter: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, rightColname)

			val resLR: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](leftColLeftOuter,
				rightColLeftOuter, rightDF, leftOuterJoin)

			assert(resLR.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that DO match (left vs. right df) in the left outer join, should NOT " +
					"correspond to a " +
					"null in" +
					" the right part of left outer join")

			val resRL: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](rightColLeftOuter,
				leftColLeftOuter, leftDF, leftOuterJoin)

			assert(resRL.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that DO match (right vs. left df) in the left outer join, should NOT " +
					"correspond to a " +
					"null in the left part of left outer join")
		}



		def testIntersectedColumnsForLeftOuterJoin = {
			val leftCol: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rightCol: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftColLeftOuter: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, leftColname)
			val rightColLeftOuter: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, rightColname)

			assert(leftCol.length <= leftColLeftOuter.length &&
				leftCol.toSet.subsetOf(leftColLeftOuter.toSet),
				"Test: left df column is a subset of the corresponding left df column in the left outer join result"
			)

			assert(rightCol.length <= rightColLeftOuter.length &&
				rightCol.toSet.subsetOf(rightColLeftOuter.toSet),
				"Test: right df column is a subset of the corresponding right df column in the outer join result"
			)
		}


		def testLeftOuterJoin: Unit = {
			// NOTE: schema of tests for all other kinds of outer joins:
			testSamnessOfAllKindsOfLeftOuterJoins

			testIntersectedColumnsForLeftOuterJoin
			testColumnTypesForLeftOuterJoin
			testMismatchedRowsForLeftOuterJoin

			testDifferingRecordsHaveNullsInLeftOuterJoin
			testMatchingRecordsDontHaveNullsInLeftOuterJoin
		}
	}




	// --------------------------------------------------------------------------------------------------------

	// L = scala type relating to the column of the left df
	// R = scala type relating to the column of the right df (e.g. coltype of rightDF can be 'IntegerType' so user
	// would have to pass in 'Int' or 'Integer')
	// T = target type (for instance may want to convert the leftDF with LEFT  col type into RIGHT type from rightDf
	// col)
	case class TestRightOuterJoin[LEFT: TypeTag, RIGHT: TypeTag, TARGET: TypeTag](leftDF: DataFrame,
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


		val rightOuterJoin: DataFrame = leftDF.join(right = rightDF,
			joinExprs = leftDF(leftColname) === rightDF(rightColname),
			joinType = "right"
		)
		val rightOuterJoin2 = leftDF.join(rightDF, leftDF(leftColname) === rightDF(rightColname), joinType =
			"rightouter")


		private val (ro, ro2) = (rightOuterJoin.collect, rightOuterJoin2.collect)



		def testSamnessOfAllKindsOfRightOuterJoins = {
			assert(ro.sameElements(ro2) ,
				"Test all right outer join results in same df")
		}



		def testColumnAggregationForRightOuterJoin: Unit = {

			assert(rightOuterJoin.columns.toList == (leftDF.columns.toList ++ rightDF.columns.toList),
				"Test 1: colnames of right outer join must be an aggregation of each of the colnames of the " +
					"joined " +
					"dataframes"
			)
		}



		def testColumnTypesForRightOuterJoin = {
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
				typeOfColumn(rightOuterJoin, leftColname) == givenLeftDataType,
				"Test: the left column of inner join df has same data type as that of column in the left df"
			)
			assert(givenLeftDataType == leftDataType) // followup for consistency

			assert(typeOfColumn(rightDF, rightColname) == givenRightDataType &&
				typeOfColumn(rightOuterJoin, rightColname) == givenRightDataType,
				"Test: the right column of right outer join df has same data type as the column in the right df"
			)
			assert(givenRightDataType == rightDataType) // followup for consistency
		}



		def testMismatchedRowsForRightOuterJoin = {

			// TESTING 1 = have `getMismatchRows` function to do it automatically
			// ldr = mismatch rows of left df relative to right df
			// rdl = mistmatch rows of right df relative to left df
			val (leftMismatchRows, rightMismatchRows) = getMismatchRows[TARGET](leftDF, rightDF, leftColname, rightColname)

			// TESTING 2 = have another way to do it, shorter way, using spark's `where` function
			// lc = left col
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			//Prerequisite: Asserting that there are no None's (nulls) - that only happens after join operations,
			// here we are
			// just taking the columns from the original dfs (left and right)
			assert(lc.toSet.diff(rc.toSet).forall(_.isDefined))

			// left to right mistmatch rows
			val ldr: List[Row] = lc.toSet.diff(rc.toSet)
				.toList
				.flatMap(diffElem => rightOuterJoin.where(rightOuterJoin.col(leftColname) === diffElem.get).collect.toList)
			// assertion tests no None's so can just .get out of the Some()

			// right to left mismatch rows
			val rdl: List[Row] = rc.toSet.diff(lc.toSet)
				.toList
				.flatMap(diffElem => rightOuterJoin.where(rightOuterJoin.col(rightColname) === diffElem.get).collect.toList)


			assert(leftMismatchRows == ldr, "Test: non-matching rows of first df with respect to second " +
				"df (the two methods must yield same results)")
			assert(rightMismatchRows == rdl, "Test: non-matching rows of second df with respect to first" +
				" df (the two methods must yield same results)")

			assert(rdl.map(row => row.toSeq.take(leftDF.columns.length).forall(_ == null)).forall(_ == true),
				"Test: for rightdf relative to left df, the last elements in the row (size as large as left df " +
					"width) that don't match, are always null")


			assert(ldr.isEmpty && (rdl.isEmpty || rdl.nonEmpty), "Test: right outer join's keeps non-matching " +
				"records from rit df but not from the left df")
		}




		/**
		 * For each element in the leftcolumn that is not in rightcol, there corresponds a null at that spot in the
		 * rightcol. This function tests that at that spot, the value is null.
		 *
		 * (e.g. for elem "50" in emp_dept_id empDF at index i = 5, there is
		 * null at i = 5 for dept_id in deptDF  part of the outer join )
		 */
			// TODO left off here
		def testDifferingRecordsHaveNullsInLeftOuterJoin = {
			val leftColLeftOuter: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, leftColname)
			val rightColLeftOuter: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, rightColname)


			val resLR: Array[List[List[Boolean]]] = recordNullSpotsColumnwise[TARGET](leftColLeftOuter, rightColLeftOuter,rightDF, rightOuterJoin)

			assert(resLR.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that don't match (left vs. right df) in outer join, should correspond to a null in" +
					" the right part of left outer join")


			val resRL: Array[List[List[Boolean]]] = recordNullSpotsColumnwise[TARGET](rightColLeftOuter,	leftColLeftOuter, leftDF, rightOuterJoin)

			assert(resRL.forall(colLst => colLst.isEmpty),
				"Test: in the case of non-matching records, left outer join only keeps the left df non-matches, " +
					"not the right df ones")

		}



		def testMatchingRecordsDontHaveNullsInLeftOuterJoin = {
			// NOTE: converting the left df col to be of type RIGHT (Int) since rightdf (deptdf) col is of type Integer
			//  while leftdf (empdf) col is of type String
			val leftColLeftOuter: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, leftColname)
			val rightColLeftOuter: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, rightColname)

			val resLR: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](leftColLeftOuter,
				rightColLeftOuter, rightDF, rightOuterJoin)

			assert(resLR.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that DO match (left vs. right df) in the left outer join, should NOT " +
					"correspond to a " +
					"null in" +
					" the right part of left outer join")

			val resRL: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](rightColLeftOuter,
				leftColLeftOuter, leftDF, rightOuterJoin)

			assert(resRL.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that DO match (right vs. left df) in the left outer join, should NOT " +
					"correspond to a " +
					"null in the left part of left outer join")
		}



		def testIntersectedColumnsForLeftOuterJoin = {
			val leftCol: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rightCol: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftColLeftOuter: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, leftColname)
			val rightColLeftOuter: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, rightColname)

			assert(leftCol.length <= leftColLeftOuter.length &&
				leftCol.toSet.subsetOf(leftColLeftOuter.toSet),
				"Test: left df column is a subset of the corresponding left df column in the left outer join result"
			)

			assert(rightCol.length <= rightColLeftOuter.length &&
				rightCol.toSet.subsetOf(rightColLeftOuter.toSet),
				"Test: right df column is a subset of the corresponding right df column in the outer join result"
			)
		}


		def testLeftOuterJoin: Unit = {
			// NOTE: schema of tests for all other kinds of outer joins:
			testSamnessOfAllKindsOfRightOuterJoins

			testIntersectedColumnsForLeftOuterJoin
			testColumnTypesForRightOuterJoin
			testMismatchedRowsForRightOuterJoin

			testDifferingRecordsHaveNullsInLeftOuterJoin
			testMatchingRecordsDontHaveNullsInLeftOuterJoin
		}
	}
}


