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



		/**
		 * For each element in the leftcolumn that is not in rightcol, there corresponds a null at that spot in the
		 * rightcol. This function tests that at that spot, the value is null.
		 *
		 * (e.g. for elem "50" in emp_dept_id empDF at index i = 5, there is
		 * null at i = 5 for dept_id in deptDF  part of the outer join )
		 */
		def testNullLocationsColumnwise = {
			val leftColOuter = getColAs[TARGET](outerJoin, leftColname)
			val rightColOuter = getColAs[TARGET](outerJoin, rightColname)


			/**
			 *
			 * @param leftColOuter a single column from the leftDF
			 * @param rightColOuter a single column from the rightDF
			 * @param rightDF DF that is joined on the right side when the outer join is created
			 * @param outerJoinDF
			 * @tparam T the type that the column should be, when extracted already from the outerJoinDF
			 * @return
			 */
			def recordNullSpotsColumnwise[T](leftColOuter: List[T],
									   rightColOuter: List[T],
									   rightDF: DataFrame,
									   outerJoinDF: DataFrame
									  ): List[Array[Boolean]] = {

				// indices corresponding to different elems from left df vs. right df
				val iDiffsLeftToRight: List[Int] = leftColOuter.toSet.diff(rightColOuter.toSet).toList // convert
					// to list else things won't be in order...?
					.map(diffElem => leftColOuter.indexOf(diffElem))

				// Get cols corresponding to right df from the outer join (to have the nulls from oute rjoin)
				val outerJoinRightDFCols: Array[List[Any]] = rightDF.columns.map(colNameStr => outerJoinDF.select(colNameStr).collect.map(row => row(0)).toList)

				// For each different elem from leftdf vs right df (now recorded as index corresponding to that elem, check
				// that corresponding position in the other df contains a null
				// (e.g. elem 50 is diff in leftvsright --> occurs at index i = 5 columnwise ---> check that in right df
				//  there is null at i = 5)
				iDiffsLeftToRight.map(iDiff => outerJoinRightDFCols.map(colList => colList(iDiff) == null))
			// TODO figure out if need to remove -1s from the iDiffs list like in the iCommons list (below)
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

		}


		def testMatchingRecordsDontHaveNullsInOuterJoin = {
			// NOTE: converting the left df col to be of type RIGHT (Int) since rightdf (deptdf) col is of type Integer
			//  while leftdf (empdf) col is of type String
			val leftColOuter: List[Option[TARGET]] = getColAs[TARGET](outerJoin, leftColname)
			val rightColOuter: List[Option[TARGET]] = getColAs[TARGET](outerJoin, rightColname)

			/**
			 *
			 * @param leftColOuter a single column from the leftDF
			 * @param rightColOuter a single column from the rightDF
			 * @param rightDF DF that is joined on the right side when the outer join is created
			 * @param outerJoinDF
			 * @tparam T the type that the column should be, when extracted already from the outerJoinDF
			 * @return
			 */
			def recordNonNullSpotsColumnwise[T](leftColOuter: List[Option[T]],
									   rightColOuter: List[Option[T]],
									   rightDF: DataFrame,
									   outerJoinDF: DataFrame
									  ): Array[List[List[Boolean]]] = {

				// remove from null result// only checking for non-null records, so remove any possible null from
				// the intersection operation
				val iCommonsLeftToRight: List[List[Int]] = leftColOuter.toSet.intersect(rightColOuter.toSet).toList
					.filter(_
					!= None).map(commElem
				=>leftColOuter.zipWithIndex.filter{ case (elem, i) => elem == commElem}.unzip._2)

				// Get cols corresponding to right df from the outer join (to have the nulls from oute rjoin)
				val outerJoinRightDFCols: Array[List[Any]] = rightDF.columns.map(colNameStr => outerJoinDF.select(colNameStr).collect.map(row => row(0)).toList)

				// For each different elem from leftdf vs right df (now recorded as index corresponding to that elem, check
				// that corresponding position in the other df contains a null
				// (e.g. elem 50 is diff in leftvsright --> occurs at index i = 5 columnwise ---> check that in right df
				//  there is null at i = 5)
				outerJoinRightDFCols.map(colList => iCommonsLeftToRight.map(commonIndexList => commonIndexList.map(i => colList(i) !=
					null)))

			}

			val resLR: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](leftColOuter,
				rightColOuter, rightDF, outerJoin)

			assert(resLR.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that don't match (left vs. right df) in outer join, should correspond to a null in" +
					" the right part of outer join")

			val resRL: Array[List[List[Boolean]]] = recordNonNullSpotsColumnwise[TARGET](rightColOuter,
				leftColOuter, leftDF, outerJoin)

			assert(resRL.forall(colLst => colLst.forall(indexLst => indexLst.forall(_ == true))),
				"Test: all elements that don't match (right vs. left df) in outer join, should correspond to a " +
					"null in" +
					" the left part of outer join")
		}



		def testInnerJoin: DataFrame = {
			testColumnAggregationForInnerJoin
			testColumnTypesForInnerJoin
			testIntersectedColumnsForInnerJoin

			innerJoin
		}*/
	}

}


