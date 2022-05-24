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

	val dts: List[DataType] = List(IntegerType, StringType, BooleanType, DoubleType)
	val sts: List[String] = List("Int", "String", "Boolean", "Double")
	val dataTypeToScalaType: Map[DataType, String] = dts.zip(sts).toMap

	// Gets the column as type relating to the type it has already (e.g. if col type is "IntegerType" then return
	// List[Int] -- C is Int, or if coltype is "StringType" then return "String" -- C is String)
	// NOTE: idea of this function (relative to the other one below with typetag) is not NOT HAVE TO pass a scala
	//  primitve type in the brackets, to just have it infer automatically with below code
	/*def getCol[C](df: DataFrame, colname: String): List[C] = {

		// getting datatype at the column (e.g. "IntegerType")
		val colDataType: DataType = typeOfColumn(df, colname)

		// Use the above map that converts from DataType to ordinary scala type
		val convType: String = dataTypeToScalaType.get(colDataType).get // out of option
		// HELP cannot cast because it is in String format + also not known until runtime

		// Converting here manually (cannot automate this process to put arbitrary type in the brackets because it
		// won't work to pass it even with typetag
		convType match {
			case "String" => df.select(colname).collect.map(row => row.getAs[String](0)).toList.asInstanceOf[List[C]]
			case "Int" => 	df.select(colname).collect.map(row => row.getAs[Int](0)).toList.asInstanceOf[List[C]]
			case "Double" => 	df.select(colname).collect.map(row => row.getAs[Double](0)).toList.asInstanceOf[List[C]]
			case "Boolean" => 	df.select(colname).collect.map(row => row.getAs[Boolean](0)).toList.asInstanceOf[List[C]]
		}
	}*/

	// Another way to convert the column to the given type; collecting first - actually faster!
	// NOTE; also handling case where conversion is not suitable (like int on "name")
	def getColAs[A: TypeTag](df: DataFrame, colname: String): List[Option[A]] = {

		// STEP 1 = try to check the col type can be converted, verifying with current schema col type
		val typesStr: List[String] = List(IntegerType, StringType, BooleanType, DoubleType).map(_.toString)
		val givenType: String = typeOf[A].toString
		//val castType: String = typesStr.filter(t => t.contains(givenType)).head
		val curColType: String = df.schema.fields.filter(_.name == colname).head.dataType.toString

		// CHECK 1 = if can convert the coltype
		val check1: Boolean = curColType.contains(givenType) // if not, then the schema doesn't have same type as
		// desired  type SO may not be able to convert

		// Getting the column from the df (now thesulting df will contain the column converted to the right
		// DataType when passing the scala type here, e.g. if givenType is Int, resulting "emp_dept_id" col will be
		// IntegerType when it was StringType before)
		val dfWithConvertedCol: DataFrame = df.withColumn(colname, col(colname).cast(givenType))


		// CHECK 2 = if can convert the coltype - if all are null then it means the conversion was not suitable
		val check2: Boolean = dfWithConvertedCol.select(colname).collect.forall(row => row(0) == null)

		val canConvert: Boolean = check1 && (! check2)

		// Choice 1 = leave the list unconverted List[Any] or
		// Choice 2 = make List[Option[A]]
		// Either way, can still access elements and compare xs(i) with an element of type A so no need for
		//conversion anyway
		// Doing choice 1
		/*dfWithConvertedCol.select(colname)
			.collect
			.map(row => row(0))
			.toList */// leaves as List[Any]

		// Doing choice 2
		dfWithConvertedCol.select(colname)
			.collect
			.map(row => row(0))
			.toList // leaves as List[Any]
			.map(Option(_))
			.asInstanceOf[List[Option[A]]]

		/*canConvert match {
			case false => {
				dfWithConvertedCol.select(colname)
					.collect
					.map(row => row(0))
					.toList.asInstanceOf[List[A]]
			}// return empty list as sign of graceful error // TODO assert here result is empty! results in error
			// otherwise
			case true => {
				dfWithConvertedCol.select(colname)
					.collect
					.map(row => row.getAs[A](0))
					.toList
				// TODO problem - for type A = Int, this returns 0 when element is 'null' and don't want that. However, the 'null' remains when using 'case false'
			}
		}*/
	}


	def has[T](df: DataFrame, colname: String, valueToCheck: T): Boolean = {
		val res: Array[Row] = df.filter(df.col(colname).contains(valueToCheck)).collect()
		! res.isEmpty
	}


	def typeOfColumn(df: DataFrame, colname: String): DataType = {
		df.schema.fields.filter(_.name == colname).head.dataType
	}

	// Gets the row that corresponds to the given value under the given column name
	// -- targetValue: if None represents null, else Some(v) represents the value v in the df.
	def rowsAt[A: TypeTag](df: DataFrame, colname: String, targetValue: Option[A]): List[Row] = {

		// NOTE - Instead use: df.where(df.col(name) == value)).collect.toList

		val colWithTheValue: List[Option[A]] = getColAs[A](df, colname)

		val rows: Array[Row] = df.collect()

		val targetRows: List[Row] = rows.zip(colWithTheValue).toList
			.filter{ case(row, optValue) => optValue == targetValue}
			.map(_._1)

		targetRows
	}


	def numMismatchCases[T: TypeTag](dfLeft: DataFrame, dfRight: DataFrame,
							   colnameLeft: String, colnameRight: String): (Int, Int) = {

		val colLeft: List[Option[T]] = getColAs[T](dfLeft, colnameLeft)
		val colRight: List[Option[T]] = getColAs[T](dfRight, colnameRight)
		val diffLeftVersusRight: Set[Option[T]] = colLeft.toSet.diff(colRight.toSet)
		val diffRightVersusLeft: Set[Option[T]] = colRight.toSet.diff(colLeft.toSet)

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

		val colLeft: List[Option[T]] = getColAs[T](dfLeft, colnameLeft)
		val colRight: List[Option[T]] = getColAs[T](dfRight, colnameRight)
		val diffLeftVersusRight: Set[Option[T]] = colLeft.toSet.diff(colRight.toSet)
		val diffRightVersusLeft: Set[Option[T]] = colRight.toSet.diff(colLeft.toSet)

		// Get all the rows (per df) that contain the different element (the non-matching element, respective to
		// each df)
		// TODO fix rowsAt to handle option
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
