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
			"Test all outerJoin result in same df")
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
		 * null at i = 5 for dept_id in deptDF  part of the outerJoin )
		 */
		def testDifferingRecordsHaveNullsInOuterJoin = {
			val leftCol_OJ: List[Option[TARGET]] = getColAs[TARGET](outerJoin, leftColname)
			val rightCol_OJ: List[Option[TARGET]] = getColAs[TARGET](outerJoin, rightColname)


			val resLR: Array[List[List[Boolean]]] = recordNullSpotsColumnwise[TARGET](leftCol_OJ, rightCol_OJ,rightDF, outerJoin)

			assert(resLR.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
			"Test: all elements that don't match (left vs. right df) in outerJoin, should correspond to a null in" +
				" the right part of outerJoin")


			val resRL: Array[List[List[Boolean]]] = recordNullSpotsColumnwise[TARGET](rightCol_OJ,	leftCol_OJ, leftDF, outerJoin)

			assert(resRL.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that don't match (right vs. left df) in outerJoin, should correspond to a " +
					"null in" +
					" the left part of outerJoin")


		}


		def testMatchingRecordsDontHaveNullsInOuterJoin = {
			// NOTE: converting the left df col to be of type RIGHT (Int) since rightdf (deptdf) col is of type Integer
			//  while leftdf (empdf) col is of type String
			val leftCol_OJ: List[Option[TARGET]] = getColAs[TARGET](outerJoin, leftColname)
			val rightCol_OJ: List[Option[TARGET]] = getColAs[TARGET](outerJoin, rightColname)


			val resLR: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](leftCol_OJ,
				rightCol_OJ, rightDF, outerJoin)

			assert(resLR.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that DO match (left vs. right df) in outerJoin, should NOT correspond to a " +
					"null in" +
					" the right part of outerJoin")

			val resRL: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](rightCol_OJ,
				leftCol_OJ, leftDF, outerJoin)

			assert(resRL.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that DO match (right vs. left df) in outerJoin, should NOT correspond to a " +
					"null in the left part of outerJoin")
		}



		def testOuterJoinKeepsAllLeftAndRightRecordsRegardlessOfMatch = {
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftCol_OJ: List[Option[TARGET]] = getColAs[TARGET](outerJoin, leftColname)
			val rightCol_OJ: List[Option[TARGET]] = getColAs[TARGET](outerJoin, rightColname)

			// Check that leftOuterJoin keeps all the left records, regardless of match
			assert(leftCol_OJ.toSet.intersect(lc.toSet) == lc.toSet /*&&
				leftCol_OJ.toSet.intersect(lc.toSet) == ojLeft.toSet*/,
				"Test: outerJoin keeps all the records from the left df, regardless of match"
			)

			assert(leftCol_OJ.toSet.filterNot(_.isEmpty) == lc.toSet,
				"Test (simpler): outerJoin keeps all the records from the left df, regardless of match"
			)

			assert(lc.toSet.diff(rc.toSet).subsetOf(leftCol_OJ.toSet),
				"Test: outerJoin does not erase non-matching left df records"
			)

			// Check outerJoin does not erase non-matching right df records
			assert(rc.toSet.diff(lc.toSet).subsetOf(rightCol_OJ.toSet),
				"Test: outerJoin does not erase non-matching right df records"
			)
		}



		def testIntersectedColumnsForOuterJoin = {
			val leftCol: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rightCol: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftCol_OJ: List[Option[TARGET]] = getColAs[TARGET](outerJoin, leftColname)
			val rightCol_OJ: List[Option[TARGET]] = getColAs[TARGET](outerJoin, rightColname)

			assert(leftCol.length <= leftCol_OJ.length &&
				leftCol.toSet.subsetOf(leftCol_OJ.toSet),
				"Test: left df column is a subset of the corresponding left df column in the outerJoin result"
			)

			assert(rightCol.length <= rightCol_OJ.length &&
				rightCol.toSet.subsetOf(rightCol_OJ.toSet),
				"Test: right df column is a subset of the corresponding right df column in the outerJoin result"
			)
		}

		def testOuterJoin: Unit = {
			// NOTE: schema of tests for all other kinds of outerJoins:
			testSamnessOfAllKindsOfOuterJoins

			testIntersectedColumnsForOuterJoin
			testColumnTypesForOuterJoin
			testMismatchedRowsForOuterJoin

			testOuterJoinKeepsAllLeftAndRightRecordsRegardlessOfMatch
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
				"Test all leftOuterJoin results in same df")
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


			assert(rdl.isEmpty && (ldr.isEmpty || ldr.nonEmpty), "Test: leftOuterJoin's keeps non-matching " +
				"records from left df but not from" +
				" " +
				"right df")
		}




		/**
		 * For each element in the leftcolumn that is not in rightcol, there corresponds a null at that spot in the
		 * rightcol. This function tests that at that spot, the value is null.
		 *
		 * (e.g. for elem "50" in emp_dept_id empDF at index i = 5, there is
		 * null at i = 5 for dept_id in deptDF  part of the outerJoin )
		 */
		def testDifferingRecordsHaveNullsInLeftOuterJoin = {
			val leftCol_LOJ: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, leftColname)
			val rightCol_LOJ: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, rightColname)


			val resLR: Array[List[List[Boolean]]] = recordNullSpotsColumnwise[TARGET](leftCol_LOJ, rightCol_LOJ,rightDF, leftOuterJoin)

			assert(resLR.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that don't match (left vs. right df) in outerJoin, should correspond to a null in" +
					" the right part of leftOuterJoin")


			val resRL: Array[List[List[Boolean]]] = recordNullSpotsColumnwise[TARGET](rightCol_LOJ,	leftCol_LOJ, leftDF, leftOuterJoin)

			assert(resRL.forall(colLst => colLst.isEmpty),
				"Test: in the case of non-matching records, leftOuterJoin only keeps the left df non-matches, " +
					"not the right df ones")

		}



		def testMatchingRecordsDontHaveNullsInLeftOuterJoin = {
			// NOTE: converting the left df col to be of type RIGHT (Int) since rightdf (deptdf) col is of type Integer
			//  while leftdf (empdf) col is of type String
			val leftCol_LOJ: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, leftColname)
			val rightCol_LOJ: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, rightColname)

			val resLR: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](leftCol_LOJ,
				rightCol_LOJ, rightDF, leftOuterJoin)

			assert(resLR.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that DO match (left vs. right df) in the leftOuterJoin, should NOT " +
					"correspond to a " +
					"null in" +
					" the right part of leftOuterJoin")

			val resRL: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](rightCol_LOJ,
				leftCol_LOJ, leftDF, leftOuterJoin)

			assert(resRL.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that DO match (right vs. left df) in the leftOuterJoin, should NOT " +
					"correspond to a " +
					"null in the left part of leftOuterJoin")
		}


		def testLeftOuterJoinKeepsAllLeftRecordsAndDropsDifferingRightRecords = {
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftCol_LOJ: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, leftColname)
			val rightCol_LOJ: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, rightColname)

			// Check that leftOuterJoin keeps all the left records, regardless of match
			assert(leftCol_LOJ.toSet.intersect(lc.toSet) == lc.toSet &&
				leftCol_LOJ.toSet.intersect(lc.toSet) == leftCol_LOJ.toSet,
				"Test: leftOuterJoin keeps all the records from the left df, regardless of match"
			)

			assert(leftCol_LOJ.toSet == lc.toSet,
				"Test (simpler): leftOuterJoin keeps all the records from the left df, regardless of match"
			)

			assert(lc.toSet.diff(rc.toSet).subsetOf(leftCol_LOJ.toSet),
				"Test: leftOuterJoin does not erase non-matching left df records"
			)

			// Check that leftOuterJoin drops records from the right df where non matching
			assert(rc.toSet.diff(lc.toSet).intersect(rightCol_LOJ.toSet).isEmpty,
				"Test: leftOuterJoin drops records from right df where records are non-matching"
			)
		}



		def testIntersectedColumnsForLeftOuterJoin = {
			val leftCol: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rightCol: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftCol_LOJ: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, leftColname)
			val rightCol_LOJ: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, rightColname)

			assert(leftCol.length <= leftCol_LOJ.length &&
				leftCol.toSet.subsetOf(leftCol_LOJ.toSet),
				"Test: left df column is a subset of the corresponding left df column in the leftOuterJoin result"
			)

			/*assert(rightCol.length <= rightCol_LOJ.length &&
				rightCol.toSet.subsetOf(rightCol_LOJ.toSet),
				"Test: right df column is a subset of the corresponding right df column in the outerJoin result"
			)*/
		}


		def testLeftOuterJoin: Unit = {
			// NOTE: schema of tests for all other kinds of outerJoins:
			testSamnessOfAllKindsOfLeftOuterJoins

			testIntersectedColumnsForLeftOuterJoin
			testColumnTypesForLeftOuterJoin
			testMismatchedRowsForLeftOuterJoin

			testDifferingRecordsHaveNullsInLeftOuterJoin
			testMatchingRecordsDontHaveNullsInLeftOuterJoin
			testLeftOuterJoinKeepsAllLeftRecordsAndDropsDifferingRightRecords
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
				"Test all rightOuterJoin results in same df")
		}



		def testColumnAggregationForRightOuterJoin: Unit = {

			assert(rightOuterJoin.columns.toList == (leftDF.columns.toList ++ rightDF.columns.toList),
				"Test 1: colnames of rightOuterJoin must be an aggregation of each of the colnames of the " +
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
				"Test: the right column of rightOuterJoin df has same data type as the column in the right df"
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


			assert(ldr.isEmpty && (rdl.isEmpty || rdl.nonEmpty), "Test: rightOuterJoin's keeps non-matching " +
				"records from the right df but not from the left df")
		}




		/**
		 * For each element in the leftcolumn that is not in rightcol, there corresponds a null at that spot in the
		 * rightcol. This function tests that at that spot, the value is null.
		 *
		 * (e.g. for elem "50" in emp_dept_id empDF at index i = 5, there is
		 * null at i = 5 for dept_id in deptDF  part of the outerJoin )
		 */
		def testDifferingRecordsHaveNullsInRightOuterJoin = {
			val leftColRightOuter: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, leftColname)
			val rightColRightOuter: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, rightColname)


			val resLR: Array[List[List[Boolean]]] = recordNullSpotsColumnwise[TARGET](leftColRightOuter, rightColRightOuter,rightDF, rightOuterJoin)

			assert(resLR.forall(colLst => colLst.isEmpty),
				"Test: in the case of non-matching records, the rightOuterJoin only keeps the non-matching " +
					"records from the right df which show up as nulls on the left df side (... not from the left" +
					" df)")


			val resRL: Array[List[List[Boolean]]] = recordNullSpotsColumnwise[TARGET](rightColRightOuter,	leftColRightOuter, leftDF, rightOuterJoin)

			assert(resRL.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: rightOuterJoin doesn't keep non-matching records from the left df")

		}



		def testMatchingRecordsDontHaveNullsInRightOuterJoin = {
			// NOTE: converting the left df col to be of type RIGHT (Int) since rightdf (deptdf) col is of type Integer
			//  while leftdf (empdf) col is of type String
			val leftCol_ROJ: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, leftColname)
			val rightCol_ROJ: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, rightColname)

			val resLR: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](leftCol_ROJ,
				rightCol_ROJ, rightDF, rightOuterJoin)

			assert(resLR.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that DO match (left vs. right df) in the rightOuterJoin, should NOT " +
					"correspond to a null in the right part of leftOuterJoin")

			val resRL: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](rightCol_ROJ,
				leftCol_ROJ, leftDF, rightOuterJoin)

			assert(resRL.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that DO match (right vs. left df) in the leftOuterJoin, should NOT " +
					"correspond to a " +
					"null in the left part of leftOuterJoin")
		}


		def testRightOuterJoinKeepsAllRightRecordsAndDropsDifferingLeftRecords = {
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftCol_ROJ: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, leftColname)
			val rightCol_ROJ: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, rightColname)

			// Check that rightOuterJoin keeps all the right records, regardless of match
			assert(rightCol_ROJ.toSet.intersect(rc.toSet) == rc.toSet &&
				rightCol_ROJ.toSet.intersect(rc.toSet) == rightCol_ROJ.toSet,
				"Test: rightOuterJoin keeps all the records from the right df, regardless of match"
			)

			assert(rightCol_ROJ.toSet == rc.toSet,
				"Test (simpler): rightOuterJoin keeps all the records from the right df, regardless of match"
			)

			assert(rc.toSet.diff(lc.toSet).subsetOf(rightCol_ROJ.toSet),
				"Test: rightOuterJoin does not erase non-matching right df records"
			)

			// Check that rightOuterJoin drops records from the left df where non matching
			assert(lc.toSet.diff(rc.toSet).intersect(leftCol_ROJ.toSet).isEmpty,
				"Test: rightOuterJoin drops records from left df where records are non-matching"
			)
		}



		def testIntersectedColumnsForRightOuterJoin = {
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftCol_ROJ: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, leftColname)
			val rightCol_ROJ: List[Option[TARGET]] = getColAs[TARGET](rightOuterJoin, rightColname)

			/*assert(lc.length <= leftCol_ROJ.length &&
				lc.toSet.subsetOf(leftCol_ROJ.toSet),
				"Test: left df column is a subset of the corresponding left df column in the leftOuterJoin result"
			)*/

			assert(rc.length <= rightCol_ROJ.length &&
				rc.toSet.subsetOf(rightCol_ROJ.toSet),
				"Test: right df column is a subset of the corresponding right df column in the outerJoin result"
			)
		}


		def testRightOuterJoin: Unit = {
			// NOTE: schema of tests for all other kinds of outerJoins:
			testSamnessOfAllKindsOfRightOuterJoins

			testIntersectedColumnsForRightOuterJoin
			testColumnTypesForRightOuterJoin
			testMismatchedRowsForRightOuterJoin

			testDifferingRecordsHaveNullsInRightOuterJoin
			testMatchingRecordsDontHaveNullsInRightOuterJoin
			testRightOuterJoinKeepsAllRightRecordsAndDropsDifferingLeftRecords
		}
	}
}


