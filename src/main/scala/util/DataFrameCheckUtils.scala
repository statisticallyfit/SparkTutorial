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



	def convertWithoutCheck[A: TypeTag](df: DataFrame, name: String): List[A] = {
		// assert that we are converting same types (IntegerType -> Int, not to Double for instance)
		val dfDataType: String = df.schema.filter(struct => struct.name == name).head.dataType.toString
		val targetDataType: String = typeOf[A].toString
		assert(dfDataType.contains(targetDataType), "WILL NOT convert different types")

		df.select(name)
			.collectAsList().toList
			.map(r => r(0))
			.asInstanceOf[List[A]]
	}

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
	def rowAt[A: TypeTag](df: DataFrame, colname: String, targetValue: A): Row = {

		val colWithTheValue: List[A] = convert[A](df, colname)

		val rows: Array[Row] = df.collect()
		val targetRow = rows.zip(colWithTheValue).filter{ case(row, value) => value == targetValue}.head._1

		targetRow
	}
}
