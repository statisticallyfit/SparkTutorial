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
	def convert[A: TypeTag](df: DataFrame, colname: String): List[A] = {

		// STEP 1 = try to check the col type can be converted, verifying with current schema col type
		val typesStr: List[String] = List(IntegerType, StringType, BooleanType, DoubleType).map(_.toString)
		val givenType: String = typeOf[A].toString
		val castType: String = typesStr.filter(t => t.contains(givenType)).head
		val curColType: String = df.schema.fields.filter(_.name == colname).head.dataType.toString

		// CHECK 1 = if can convert the coltype
		val check1: Boolean = curColType == castType // if not, then the schema doesn't have same type as desired
		// type SO may not be able to convert

		// NOTE: if you pass the datatype-string as cast type in string format, gives weird error ...
		val dfConverted: DataFrame = df.withColumn(colname, col(colname).cast(givenType))

		// CHECK 2 = if can convert the coltype - if all are null then it means the conversion was not suitable
		val check2: Boolean = dfConverted.select(colname).collect.forall(row => row(0) == null)

		val canConvert: Boolean = ! check2 // && check1

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


	def colType(df: DataFrame, colname: String): DataType = {
		df.schema.fields.filter(_.name == colname).head.dataType
	}

	// Gets the row that corresponds to the given value under the given column name
	def rowsAt[A: TypeTag](df: DataFrame, colname: String, targetValue: A): List[Row] = {

		// NOTE - Instead use: df.where(df.col(name) == value)).collect.toList

		val colWithTheValue: List[A] = convert[A](df, colname)

		val rows: Array[Row] = df.collect()

		val targetRows: List[Row] = rows.zip(colWithTheValue).toList
			.filter{ case(row, value) => value == targetValue}
			.map(_._1)

		targetRows
	}


	def numMismatchCases[T: TypeTag](dfA: DataFrame, dfB: DataFrame,
							   colnameA: String, colnameB: String): (Int, Int) = {

		val colA: List[T] = convert[T](dfA, colnameA)
		val colB: List[T] = convert[T](dfB, colnameB)
		val setDiffAToB: Set[T] = colA.toSet.diff(colB.toSet)
		val setDiffBToA: Set[T] = colB.toSet.diff(colA.toSet)

		// Count number of times each different element appears in the respective column of each df
		/*val numDiffsA: List[(T, Int)] = setDiffAToB.toList.map(setElem =>
			(setElem, colA.count(colElem => colElem	== setElem)))
		val numDiffsB: List[(T, Int)] = setDiffBToA.toList.map(setElem =>
			(setElem, colB.count(colElem => colElem	== setElem)))*/

		// Count num cols and num rows of the join data sets
		val numColOfJoinedDFs: Int = dfA.columns.length + dfB.columns.length

		val numRowOfInnerJoinDF: Int = colA.toSet.intersect(colB.toSet).toList.map(setElem =>
			(setElem, colA.count(colElem => colElem == setElem))
		).unzip._2.sum

		val numMismatchOuterJoin: Int = (setDiffAToB ++ setDiffBToA).toList.map(diffElem =>
			(diffElem, (colA ++ colB).count(colElem => colElem == diffElem))
		).unzip._2.sum

		val numRowOfOuterJoinDF = numRowOfInnerJoinDF + numMismatchOuterJoin

		// Return pair of num matching rows and number of mismatched rows
		(numRowOfInnerJoinDF, numRowOfOuterJoinDF)
	}

	// Type A = the type that the columns are converted to when joining dataframes (IntegerType -> Int)
	// GOAL: get the rows that don't match in the outer join with respect to dfA (_._1) and dfB (_._2)
	def getMismatchRows[T: TypeTag](dfA: DataFrame, dfB: DataFrame,
							  colnameA: String, colnameB: String): (List[Row], List[Row]) = {

		val colA: List[T] = convert[T](dfA, colnameA)
		val colB: List[T] = convert[T](dfB, colnameB)
		val setDiffAToB: Set[T] = colA.toSet.diff(colB.toSet)
		val setDiffBToA: Set[T] = colB.toSet.diff(colA.toSet)

		// Get all the rows (per df) that contain the different element (the non-matching element, respective to
		// each df)
		val diffRowsA: List[Row] = setDiffAToB.toList.flatMap(diffedElem => rowsAt[T](dfA, colnameA, diffedElem))
		val diffRowsB: List[Row] = setDiffBToA.toList.flatMap(diffedElem => rowsAt[T](dfB, colnameB, diffedElem))

		// Now fill each with null (first df has nulls at the end, since dfB is added after, while dfB's row has
		// nulls at beginning, to accomodate values from dfA) since when join, it is dfA + dfB not dfB + dfA
		val bufferedRowsA: List[Seq[Any]] = diffRowsA.map(row => row.toSeq ++ Seq.fill(dfB.columns.length)(null) )
		val bufferedRowsB: List[Seq[Any]] = diffRowsB.map(row => Seq.fill(dfA.columns.length)(null) ++ row.toSeq)

		// First _._1 = the rows that don't match in the outer join, for dfA relative to dfB
		// Second _._2 = the rows that don't match in the outer join, for dfB relative to dfA
		val abRows: List[Row] = bufferedRowsA.map(Row(_:_*))
		val baRows: List[Row] = bufferedRowsB.map(Row(_:_*))

		assert(abRows.map(row => row.toSeq.takeRight(dfB.columns.length).forall(_ == null)).forall(_ == true),
			"Test: Last elements in the row that don't match are always null (first df relative to second)"
		)
		assert(baRows.map(row => row.toSeq.take(dfA.columns.length).forall(_ == null)).forall(_ == true),
			"Test: first elements in the row that don't match are always null (second df relative to first"
		)

		(abRows, baRows)
	}
}
