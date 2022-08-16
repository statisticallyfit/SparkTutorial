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
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname) // want to convert empDF string
			// col
			// emp-dept-id from string into int to be able to compare this leftCol with the rightCol from dept-df
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname) // get col as int (already int)

			val leftCol_IJ: List[Option[TARGET]] = getColAs[TARGET](innerJoin, leftColname)

			assert(getColAs[TARGET](innerJoin, leftColname).sameElements(getColAs[TARGET](innerJoin, rightColname)),
				"Test 3: left df col and right df col contain the same elements because those were the matching records. ")

			assert(leftCol_IJ.toSet == lc.toSet.intersect(rc.toSet),
				"Test 4: Inner join df column elements are a result of intersecting the column elements of left " +
					"and right df (this indicates matching records)")

			assert(leftCol_IJ.toSet.subsetOf(lc.toSet) &&
				leftCol_IJ.toSet.subsetOf(rc.toSet),
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
			val (canonicalLeftDiffs, canonicalRightDiffs) = getMismatchRows[TARGET](leftDF, rightDF, leftColname, rightColname)

			// TESTING 2 = have another way to do it, shorter way, using spark's `where` function
			// lc = left col
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			//Prerequisite: Asserting that there are no None's (nulls) - that only happens after join operations,
			// here we are
			// just taking the columns from the original dfs (left and right)
			assert(lc.toSet.diff(rc.toSet).forall(_.isDefined))

			// left to right mismatch rows for outerJoin
			val ojLeftDiffs: List[Row] = lc.toSet.diff(rc.toSet)
				.toList
				.flatMap(diffElem => outerJoin.where(outerJoin.col(leftColname) === diffElem.get).collect.toList)
			// assertion tests no None's so can just .get out of the Some()

			// right to left mismatch rows for outerJoin
			val ojRightDiffs: List[Row] = rc.toSet.diff(lc.toSet)
				.toList
				.flatMap(diffElem => outerJoin.where(outerJoin.col(rightColname) === diffElem.get).collect.toList)


			assert(canonicalLeftDiffs.map(row => row.toSeq.takeRight(rightDF.columns.length).forall(_ == null))
				.forall(_	== true),
				"Test: the canonical list of mismatched rows (left vs. right df) should have nulls filling the \" +\n\t\t\t\"shape of the right df"
			)

			assert(canonicalRightDiffs.map(row => row.toSeq.take(leftDF.columns.length).forall(_ == null)).forall(_
				== true),
				"Test: canonical list of mismatched rows (right vs. left df) should have nulls filling the shape \" +\n\t\t\t\"of the left df"
			)


			assert(canonicalLeftDiffs == ojLeftDiffs, "Test: outerJoin keeps non-matching rows from left vs. right" +
				" df")
			assert(canonicalRightDiffs == ojRightDiffs, "Test: outerJoin keeps non-matching rows from right df vs." +
				" left df")
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
			assert(leftCol_OJ.toSet.intersect(lc.toSet) == lc.toSet &&
				lc.toSet.subsetOf(leftCol_OJ.toSet)
				/*leftCol_OJ.toSet.intersect(lc.toSet) == ojLeft.toSet*/,
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
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftCol_OJ: List[Option[TARGET]] = getColAs[TARGET](outerJoin, leftColname)
			val rightCol_OJ: List[Option[TARGET]] = getColAs[TARGET](outerJoin, rightColname)


			assert(lc.length <= leftCol_OJ.length &&
				lc.toSet.subsetOf(leftCol_OJ.toSet),
				"Test: outerJoin keeps only the matching records from left df"
			)
			assert(rc.length <= rightCol_OJ.length &&
				rc.toSet.subsetOf(rightCol_OJ.toSet),
				"Test: outerJoin keeps only the matching records from right df"
			)

			/*val commonElems = lc.toSet.intersect(rc.toSet)
			assert(lc.toSet.intersect(rc.toSet) == leftCol_OJ.toSet,
				"Test: leftSemiJoin keeps only the matching records between left and right dfs"
			)*/
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
			// These contain the canonical list of mismatched rows, regardless of what the outerjoin type does.
			val (canonicalLeftDiffs, canonicalRightDiffs) = getMismatchRows[TARGET](leftDF, rightDF, leftColname, rightColname)

			// TESTING 2 = have another way to do it, shorter way, using spark's `where` function
			// lc = left col
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			//Prerequisite: Asserting that there are no None's (nulls) - that only happens after join operations,
			// here we are
			// just taking the columns from the original dfs (left and right)
			assert(lc.toSet.diff(rc.toSet).forall(_.isDefined))

			// left to right mismatch rows for the left outer join
			val lojLeftDiffs: List[Row] = lc.toSet.diff(rc.toSet)
				.toList
				.flatMap(diffElem => leftOuterJoin.where(leftOuterJoin.col(leftColname) === diffElem.get).collect.toList)
			// assertion tests no None's so can just .get out of the Some()

			// right to left mismatch rows for the left outer join
			val lojRightDiffs: List[Row] = rc.toSet.diff(lc.toSet)
				.toList
				.flatMap(diffElem => leftOuterJoin.where(leftOuterJoin.col(rightColname) === diffElem.get).collect.toList)

			assert(canonicalLeftDiffs.map(row => row.toSeq.takeRight(rightDF.columns.length)
				.forall(_ == null)).forall(_	== true),
				"Test: the canonical list of mismatched rows (left vs. right df) should have nulls filling the " +
					"shape of the right df"
			)
			assert(canonicalRightDiffs.map(row => row.toSeq.take(leftDF.columns.length)
				.forall(_ == null)).forall(_ == true),
				"Test: canonical list of mismatched rows (right vs. left df) should have nulls filling the shape " +
					"of the left df"
			)

			assert(canonicalLeftDiffs == lojLeftDiffs &&
				lojRightDiffs.isEmpty &&
				(lojLeftDiffs.isEmpty || lojLeftDiffs.nonEmpty), "Test: leftOuterJoin " +
				"keeps non-matching rows from the left df " +
				"vs right df but not from right df vs left df"
			)
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
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftCol_LOJ: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, leftColname)
			val rightCol_LOJ: List[Option[TARGET]] = getColAs[TARGET](leftOuterJoin, rightColname)

			assert(lc.length <= leftCol_LOJ.length &&
				lc.toSet.subsetOf(leftCol_LOJ.toSet),
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


		// TODO left off here, match against the loj version in sublime
		def testMismatchedRowsForRightOuterJoin = {

			// TESTING 1 = have `getMismatchRows` function to do it automatically
			// ldr = mismatch rows of left df relative to right df
			// rdl = mistmatch rows of right df relative to left df
			val (canonicalLeftDiffs, canonicalRightDiffs) = getMismatchRows[TARGET](leftDF, rightDF, leftColname, rightColname)

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


			assert(canonicalLeftDiffs == ldr, "Test: non-matching rows of first df with respect to second " +
				"df (the two methods must yield same results)")
			assert(canonicalRightDiffs == rdl, "Test: non-matching rows of second df with respect to first" +
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


	// --------------------------------------------------------------------------------------------------------

	// L = scala type relating to the column of the left df
	// R = scala type relating to the column of the right df (e.g. coltype of rightDF can be 'IntegerType' so user
	// would have to pass in 'Int' or 'Integer')
	// T = target type (for instance may want to convert the leftDF with LEFT  col type into RIGHT type from rightDf
	// col)
	case class TestLeftSemiJoin[LEFT: TypeTag, RIGHT: TypeTag, TARGET: TypeTag](leftDF: DataFrame,
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


		val leftSemiJoin: DataFrame = leftDF.join(right = rightDF,
			joinExprs = leftDF(leftColname) === rightDF(rightColname),
			joinType = "leftsemi"
		)


		def testLeftSemiDropsRightDFColumns: Unit = {

			assert(leftSemiJoin.columns.toSet.subsetOf( (leftDF.columns ++ rightDF.columns).toSet ),
				"Test: leftSemiJoin drops the columns from the right df and keeps on the columns from the left df"
			)
			assert(leftDF.columns.sameElements(leftSemiJoin.columns),
				"Test: leftSemiJoin's columns are equal to leftDF columns"
			)
			assert(! leftSemiJoin.columns.contains(rightDF.columns),
				"Test: leftSemiJoin does not keep the right df columns"
			)
		}


		def testIntersectedColumnsForLeftSemiJoin = {
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftCol_LSJ: List[Option[TARGET]] = getColAs[TARGET](leftSemiJoin, leftColname)
			//val rightCol_LSJ: List[Option[TARGET]] = getColAs[TARGET](leftSemiJoin, rightColname)

			assert((leftCol_LSJ.length <= lc.length) && (leftCol_LSJ.toSet.subsetOf(lc.toSet)),
				"Test: leftSemiJoin keeps only the matching records between the left and right df"
			)
			assert(lc.toSet.intersect(rc.toSet) == leftCol_LSJ.toSet,
				"Test: leftSemiJoin keeps only the matching records between left and right dfs"
			)
		}


		def testColumnTypesForLeftSemiJoin = {
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
				typeOfColumn(leftSemiJoin, leftColname) == givenLeftDataType,
				"Test: the left column of leftSemiJoin has same data type as that of column in the left df"
			)
			assert(givenLeftDataType == leftDataType) // followup for consistency

			/*assert(typeOfColumn(rightDF, rightColname) == givenRightDataType &&
				typeOfColumn(leftSemiJoin, rightColname) == givenRightDataType, // TODO leftsemijoin has no
				// rightdf side
				"Test: the right column of leftSemiJoin df has same data type as the column in the right df"
			)*/
			assert(givenRightDataType == rightDataType) // followup for consistency
		}



		def testLeftSemiJoinLacksRightDFColumns = {

			assert(leftSemiJoin.columns.sameElements(leftDF.columns) &&
				! leftSemiJoin.columns.contains(rightColname) &&
				leftSemiJoin.collect.forall(row => row.toSeq.length == leftDF.columns.length),
				"Test: left semi join lacks the right df, and contains the columns of the left df only")
		}

		def testNoMismatchedRowsInLeftSemiJoin = {

			// TESTING 1 = have `getMismatchRows` function to do it automatically
			// ldr = mismatch rows of left df relative to right df
			// rdl = mistmatch rows of right df relative to left df
			val (canonicalLeftDiffs, canonicalRightDiffs) = getMismatchRows[TARGET](leftDF, rightDF, leftColname, rightColname)

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
				.flatMap(diffElem => leftSemiJoin.where(leftSemiJoin.col(leftColname) === diffElem.get).collect.toList)
			// assertion tests no None's so can just .get out of the Some()

			// right to left mismatch rows
			/*val rdl: List[Row] = rc.toSet.diff(lc.toSet)
				.toList
				.flatMap(diffElem => leftSemiJoin.where(leftSemiJoin.col(rightColname) === diffElem.get).collect.toList)*/
			// NOTE: no right df side in leftsemijoin so cannot call rightColname in the leftSemiJoin df.

			assert(canonicalLeftDiffs != ldr , "Test 1: leftSemiJoin does not keep mismatches")

			assert(ldr.isEmpty , "Test 2: leftSemiJoin does not keep mismatched rows")

			// Check that all rows have no Nulls because there are no mismatched rows (records not matched join
			// expression are ignored from both left and right dfs)
			assert(getCols(leftSemiJoin).map(colLst => ! colLst.contains(null)).forall(_ == true) &&
				leftSemiJoin.collect.forall(row => ! row.toSeq.contains(null)),
				"Test 3: leftSemiJoin keeps no columns from right df so there are no nulls from potential " +
					"mismatched rows from the right df side"
			)
		}



		def testLeftSemiJoinDropsNonMatchingRecords = {
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftCol_LSJ: List[Option[TARGET]] = getColAs[TARGET](leftSemiJoin, leftColname)
			//val rightCol_ROJ: List[Option[TARGET]] = getColAs[TARGET](leftSemiJoin, rightColname)

			// TODO update this - doesn't make sense? compare to above
			// TODO combine with above test
			// Check that rightOuterJoin keeps all the right records, regardless of match
			assert(leftCol_LSJ.toSet.intersect(rc.toSet).subsetOf(lc.toSet), // TODO fix to be subset not ==
				// for other tests too
				"Test: leftSemiJoin keeps matching records between left and right df"
			)

			assert(lc.toSet.diff(rc.toSet).intersect(leftCol_LSJ.toSet).isEmpty,
				"Test: leftSemiJoin drops records from right df where non-matching. Check the differences between" +
					" the left " +
					"and right cols are disjoint (nothing in common)"
			)
		}





		def testLeftSemiJoin: Unit = {
			// NOTE: schema of tests for all other kinds of outerJoins:

			testIntersectedColumnsForLeftSemiJoin
			testColumnTypesForLeftSemiJoin

			testNoMismatchedRowsInLeftSemiJoin
			testLeftSemiJoinDropsNonMatchingRecords
		}
	}


	// --------------------------------------------------------------------------------------------------------

	// L = scala type relating to the column of the left df
	// R = scala type relating to the column of the right df (e.g. coltype of rightDF can be 'IntegerType' so user
	// would have to pass in 'Int' or 'Integer')
	// T = target type (for instance may want to convert the leftDF with LEFT  col type into RIGHT type from rightDf
	// col)
	case class TestLeftAntiJoin[LEFT: TypeTag, RIGHT: TypeTag, TARGET: TypeTag](leftDF: DataFrame,
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


		// Left Anti join does the exact opposite of the Spark leftsemi join, leftanti join
		// returns only columns from the left DataFrame/Dataset for non-matched records.
		val leftAntiJoin: DataFrame = leftDF.join(right = rightDF,
			joinExprs = leftDF(leftColname) === rightDF(rightColname),
			joinType = "leftanti"
		)
		// Include to compare with left Anti join
		val leftSemiJoin: DataFrame = leftDF.join(right = rightDF,
			joinExprs = leftDF(leftColname) === rightDF(rightColname),
			joinType = "leftsemi"
		)


		def testLeftAntiDropsRightDFColumns: Unit = {

			assert(leftAntiJoin.columns.toSet.subsetOf( (leftDF.columns ++ rightDF.columns).toSet ),
				"Test: leftAntiJoin drops the columns from the right df and keeps on the columns from the left df"
			)
			assert(leftDF.columns.sameElements(leftAntiJoin.columns),
				"Test: leftAntiJoin's columns are equal to leftDF columns"
			)
			assert(! leftAntiJoin.columns.contains(rightDF.columns),
				"Test: leftSemiJoin does not keep the right df columns"
			)
		}


		def testIntersectedColumnsForLeftAntiJoin = {
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftCol_LAJ: List[Option[TARGET]] = getColAs[TARGET](leftAntiJoin, leftColname)
			val leftCol_LSJ: List[Option[TARGET]] = getColAs[TARGET](leftSemiJoin, leftColname)
			//val rightCol_LSJ: List[Option[TARGET]] = getColAs[TARGET](leftSemiJoin, rightColname)

			assert((leftCol_LAJ.length + leftCol_LSJ.length == lc.length) &&
				(leftCol_LSJ ++ leftCol_LAJ).sameElements(lc) ,
				"Test: leftAntiJoin keeps only the non-matching records between the left and right df, while " +
					"leftSemiJoin keeps the matching records between left and right df."
			)


			assert(lc.toSet.diff(rc.toSet).sameElements(leftCol_LAJ.toSet) &&
				lc.toSet.intersect(rc.toSet) == leftCol_LSJ.toSet,
				"Test: leftAntiJoin keeps only the non-matching records between left and right dfs; it is equal " +
					"to the leftdf diff relative to the right df. "
			)
		}


		def testColumnTypesForLeftAntiJoin = {
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
				typeOfColumn(leftAntiJoin, leftColname) == givenLeftDataType,
				"Test: the left column of leftSemiJoin has same data type as that of column in the left df"
			)
			assert(givenLeftDataType == leftDataType) // followup for consistency

			/*assert(typeOfColumn(rightDF, rightColname) == givenRightDataType &&
				typeOfColumn(leftSemiJoin, rightColname) == givenRightDataType, // TODO leftsemijoin has no
				// rightdf side
				"Test: the right column of leftSemiJoin df has same data type as the column in the right df"
			)*/
			assert(givenRightDataType == rightDataType) // followup for consistency
		}



		def testLeftAntiJoinLacksRightDFColumns = {

			assert(leftAntiJoin.columns.sameElements(leftDF.columns) &&
				! leftAntiJoin.columns.contains(rightColname) &&
				leftAntiJoin.collect.forall(row => row.toSeq.length == leftDF.columns.length),
				"Test: left semi join lacks the right df, and contains the columns of the left df only")
		}

		def testLeftAntiJoinHasOnlyMismatchedRows = {

			// TESTING 1 = have `getMismatchRows` function to do it automatically
			// ldr = mismatch rows of left df relative to right df
			// rdl = mistmatch rows of right df relative to left df
			val (canonicalLeftDiffs, canonicalRightDiffs): (List[Row], List[Row]) = getMismatchRows[TARGET](leftDF, rightDF,leftColname,rightColname)

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
				.flatMap(diffElem => leftAntiJoin.where(leftAntiJoin.col(leftColname) === diffElem.get).collect.toList)
			// assertion tests no None's so can just .get out of the Some()

			// right to left mismatch rows
			/*val rdl: List[Row] = rc.toSet.diff(lc.toSet)
				.toList
				.flatMap(diffElem => leftSemiJoin.where(leftSemiJoin.col(rightColname) === diffElem.get).collect.toList)*/
			// NOTE: no right df side in leftsemijoin so cannot call rightColname in the leftSemiJoin df.

			/** canonicalLeftDiffs has nulls from the getColAs null-padding function but leftAntiJoin has no nulls
			 * because it only keeps left df cols not right df cols --> SO need to just compare the non-null values.
			 *
			 * scala> canonicalLeftDiffs.foreach(println(_))
				[11,Llewelyn,4,2030,60,F,5555,null,null]
				[10,Lisbeth,4,2030,100,F,5005,null,null]
				[7,Layla,3,2030,70,F,5000,null,null]
				[8,Lobelia,3,2030,80,F,5500,null,null]
				[6,Brown,2,2010,50,,-1,null,null]
				[9,Linda,4,2030,90,F,5050,null,null]

				scala> ldrx.foreach(println(_))
				[11,Llewelyn,4,2030,60,F,5555]
				[10,Lisbeth,4,2030,100,F,5005]
				[7,Layla,3,2030,70,F,5000]
				[8,Lobelia,3,2030,80,F,5500]
				[6,Brown,2,2010,50,,-1]
				[9,Linda,4,2030,90,F,5050]
			 */
			val canonicalLeftDiffs_NoNull: List[Seq[Any]] = canonicalLeftDiffs.map(row => row.toSeq.filter(_ != null))
			val ldr_NoNull: List[Seq[Any]] = ldr.map(row => row.toSeq.filter(_ != null))

			assert(canonicalLeftDiffs_NoNull == ldr_NoNull , "Test 1: leftAntiJoin keeps only the mismatches between" +
				" left and right dfs")

			// Check that all rows have no Nulls because there are no mismatched rows (records not matched join
			// expression are ignored from both left and right dfs)
			assert(getCols(leftAntiJoin).map(colLst => ! colLst.contains(null)).forall(_ == true) &&
				leftAntiJoin.collect.forall(row => ! row.toSeq.contains(null)),
				"Test 3: leftAntiJoin keeps only left df cols so there are no nulls from unmatched records from " +
					"the rightdf side. "
			)
		}



		// TODO maybe combine this with above one
		// TODO do the same for the left semi version
		def testLeftAntiJoinKeepsOnlyNonMatchingRecords = {
			val lc: List[Option[TARGET]] = getColAs[TARGET](leftDF, leftColname)
			val rc: List[Option[TARGET]] = getColAs[TARGET](rightDF, rightColname)

			val leftCol_LSJ: List[Option[TARGET]] = getColAs[TARGET](leftAntiJoin, leftColname)
			//val rightCol_ROJ: List[Option[TARGET]] = getColAs[TARGET](leftSemiJoin, rightColname)

			// TODO update this test to match what is done by leftAiJoin
			// Check that rightOuterJoin keeps all the right records, regardless of match
			assert(leftCol_LSJ.toSet.intersect(rc.toSet).subsetOf(lc.toSet), // TODO fix to be subset not ==
				// for other tests too
				"Test: leftSemiJoin keeps matching records between left and right df"
			)

			assert(lc.toSet.diff(rc.toSet).intersect(leftCol_LSJ.toSet).isEmpty,
				"Test: leftSemiJoin drops records from right df where non-matching. Check the differences between" +
					" the left " +
					"and right cols are disjoint (nothing in common)"
			)
		}





		def testLeftSemiJoin: Unit = {
			// NOTE: schema of tests for all other kinds of outerJoins:

			testIntersectedColumnsForLeftAntiJoin
			testColumnTypesForLeftAntiJoin

			testLeftAntiJoinHasOnlyMismatchedRows
			testLeftAntiJoinKeepsOnlyNonMatchingRecords
		}
	}
}


