package util

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.catalyst.plans._

import org.apache.spark.sql.types.{DataType, StringType, IntegerType, BooleanType, DoubleType, StructField, StructType}
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._



/**
 *
 */
object DataFrameCheckUtils {



	/*def convertWithoutCheck[A: TypeTag](df: DataFrame, name: String): List[A] = {
		// assert that we are converting same types (IntegerType -> Int, not to Double for instance)
		val dfDataType: String = df.schema.filter(struct => struct.name == name).head.dataType.toString
		val targetDataType: String = typeOf[A].toString
		assert(dfDataType.contains(targetDataType), "WILL NOT convert different types")

		df.select(name)
			.collectAsList().toList
			.map(r => r(0))
			.asInstanceOf[List[A]]
	}*/

	// Another way to convert the column to the given type; collecting first - actually faster!
	// NOTE; also handling case where conversion is not suitable (like int on "name")
	def getTypedCol[A: TypeTag](df: DataFrame, colname: String): List[A] = {

		// STEP 1 = try to check the col type can be converted, verifying with current schema col type
		val typesStr: List[String] = List(IntegerType, StringType, BooleanType, DoubleType).map(_.toString)
		val givenType: String = typeOf[A].toString
		val castType: String = typesStr.filter(t => t.contains(givenType)).head
		val curColType: String = df.schema.fields.filter(_.name == colname).head.dataType.toString

		// CHECK 1 = if can convert the coltype
		val check1: Boolean = curColType.contains(givenType) // if not, then the schema doesn't have same type as
		// desired  type SO may not be able to convert

		// NOTE: if you pass the datatype-string as cast type in string format, gives weird error ...
		val dfConverted: DataFrame = df.withColumn(colname, col(colname).cast(givenType))

		// CHECK 2 = if can convert the coltype - if all are null then it means the conversion was not suitable
		val check2: Boolean = dfConverted.select(colname).collect.forall(row => row(0) == null)

		val canConvert: Boolean = ! check2 && check1

		canConvert match {
			case false => {
				dfConverted.select(colname)
					.collect
					.map(row => row(0))
					.toList.asInstanceOf[List[A]]
			}// return empty list as sign of graceful error
			case true => {
				dfConverted.select(colname)
					.collect
					.map(row => row.getAs[A](0))
					.toList
			}
		}
	}

	def getTypedCol[A: TypeTag](df: DataFrame, colname: String): List[A] = {

		// STEP 1 = try to check the col type can be converted, verifying with current schema col type
		val typesStr: List[String] = List(IntegerType, StringType, BooleanType, DoubleType).map(_.toString)
		val givenType: String = typeOf[A].toString
		val castType: String = typesStr.filter(t => t.contains(givenType)).head
		val curColType: String = df.schema.fields.filter(_.name == colname).head.dataType.toString

		// CHECK 1 = if can convert the coltype
		val check1: Boolean = curColType.contains(givenType) // if not, then the schema doesn't have same type as
		// desired  type SO may not be able to convert

		// NOTE: if you pass the datatype-string as cast type in string format, gives weird error ...
		val dfConverted: DataFrame = df.withColumn(colname, col(colname).cast(givenType))

		// CHECK 2 = if can convert the coltype - if all are null then it means the conversion was not suitable
		val check2: Boolean = dfConverted.select(colname).collect.forall(row => row(0) == null)

		val canConvert: Boolean = ! check2 && check1

		canConvert match {
			case false => {
				dfConverted.select(colname)
					.collect
					.map(row => row(0))
					.toList.asInstanceOf[List[A]]
			}// return empty list as sign of graceful error
			case true => {
				dfConverted.select(colname)
					.collect
					.map(row => row.getAs[A](0))
					.toList
			}
		}
	}

	def has[T](df: DataFrame, colname: String, valueToCheck: T): Boolean = {
		val res: Array[Row] = df.filter(df.col(colname).contains(valueToCheck)).collect()
		! res.isEmpty
	}


	def typeOfColumn(df: DataFrame, colname: String): DataType = {
		df.schema.fields.filter(_.name == colname).head.dataType
	}

	// Gets the row that corresponds to the given value under the given column name
	def rowsAt[A: TypeTag](df: DataFrame, colname: String, targetValue: A): List[Row] = {

		// NOTE - Instead use: df.where(df.col(name) == value)).collect.toList

		val colWithTheValue: List[A] = getTypedCol[A](df, colname)

		val rows: Array[Row] = df.collect()

		val targetRows: List[Row] = rows.zip(colWithTheValue).toList
			.filter{ case(row, value) => value == targetValue}
			.map(_._1)

		targetRows
	}


	def numMismatchCases[T: TypeTag](dfLeft: DataFrame, dfRight: DataFrame,
							   colnameLeft: String, colnameRight: String): (Int, Int) = {

		val colLeft: List[T] = getTypedCol[T](dfLeft, colnameLeft)
		val colRight: List[T] = getTypedCol[T](dfRight, colnameRight)
		val diffLeftVersusRight: Set[T] = colLeft.toSet.diff(colRight.toSet)
		val diffRightVersusLeft: Set[T] = colRight.toSet.diff(colLeft.toSet)

		// Count number of times each different element appears in the respective column of each df
		/*val numDiffsA: List[(T, Int)] = setDiffAToB.toList.map(setElem =>
			(setElem, colA.count(colElem => colElem	== setElem)))
		val numDiffsB: List[(T, Int)] = setDiffBToA.toList.map(setElem =>
			(setElem, colB.count(colElem => colElem	== setElem)))*/

		// Count num cols and num rows of the join data sets
		val numColOfJoinedDFs: Int = dfLeft.columns.length + dfRight.columns.length

		val numRowOfInnerJoinDF: Int = colLeft.toSet.intersect(colRight.toSet).toList.map(setElem =>
			(setElem, colLeft.count(colElem => colElem == setElem))
		).unzip._2.sum

		val numMismatchOuterJoin: Int = (diffLeftVersusRight ++ diffRightVersusLeft).toList.map(diffElem =>
			(diffElem, (colLeft ++ colRight).count(colElem => colElem == diffElem))
		).unzip._2.sum

		val numRowOfOuterJoinDF = numRowOfInnerJoinDF + numMismatchOuterJoin

		// Return pair of num matching rows and number of mismatched rows
		(numRowOfInnerJoinDF, numRowOfOuterJoinDF)
	}

	// Type A = the type that the columns are converted to when joining dataframes (IntegerType -> Int)
	// GOAL: get the rows that don't match in the outer join with respect to dfA (_._1) and dfB (_._2)
	def getMismatchRows[T: TypeTag](dfLeft: DataFrame, dfRight: DataFrame,
							  colnameLeft: String, colnameRight: String): (List[Row], List[Row]) = {

		val colLeft: List[T] = getTypedCol[T](dfLeft, colnameLeft)
		val colRight: List[T] = getTypedCol[T](dfRight, colnameRight)
		val diffLeftVersusRight: Set[T] = colLeft.toSet.diff(colRight.toSet)
		val diffRightVersusLeft: Set[T] = colRight.toSet.diff(colLeft.toSet)

		// Get all the rows (per df) that contain the different element (the non-matching element, respective to
		// each df)
		val rowDiffLeft: List[Row] = diffLeftVersusRight.toList.flatMap(diffedElem => rowsAt[T](dfLeft, colnameLeft, diffedElem))
		val rowDiffRight: List[Row] = diffRightVersusLeft.toList.flatMap(diffedElem => rowsAt[T](dfRight, colnameRight, diffedElem))

		// Now fill each with null (first df has nulls at the end, since dfB is added after, while dfB's row has
		// nulls at beginning, to accomodate values from dfA) since when join, it is dfA + dfB not dfB + dfA
		val nulledRowsLeft: List[Seq[Any]] = rowDiffLeft.map(row => row.toSeq ++ Seq.fill(dfRight.columns.length)(null) )
		val nulledRowsRight: List[Seq[Any]] = rowDiffRight.map(row => Seq.fill(dfLeft.columns.length)(null) ++ row.toSeq)

		// First _._1 = the rows that don't match in the outer join, for dfA relative to dfB
		// Second _._2 = the rows that don't match in the outer join, for dfB relative to dfA
		val rowsMismatchLeft: List[Row] = nulledRowsLeft.map(Row(_:_*))
		val rowsMismatchRight: List[Row] = nulledRowsRight.map(Row(_:_*))

		assert(rowsMismatchLeft.map(row => row.toSeq.takeRight(dfRight.columns.length).forall(_ == null)).forall(_ == true),
			"Test: Last elements in the row that don't match are always null (first df relative to second)"
		)
		assert(rowsMismatchRight.map(row => row.toSeq.take(dfLeft.columns.length).forall(_ == null)).forall(_ == true),
			"Test: first elements in the row that don't match are always null (second df relative to first"
		)

		(rowsMismatchLeft, rowsMismatchRight)
	}
}
