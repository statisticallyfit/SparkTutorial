package com.SparkDocumentationByTesting.state

import scala.reflect.runtime.universe._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import utilities.DFUtils
import utilities.DFUtils.TypeAbstractions._


import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums.{TradeDf, AnimalDf}
import TradeDf._
import AnimalDf._
import com.data.util.EnumHub._


/**
 *
 */

trait DFSelectState {

	val rows: Seq[Row] //rows of the dataframe

	// cannot put arbitrary number of ints here
	// C0, C1...

	// The map of colname-to-index, of the df
	val nameIndexMap: Map[NameOfCol, Int]
	// The map of colname-to-strtype, of the df, where strtype = datatype converted to string format
	val nameTypeMap: Map[NameOfCol, DataType]
}

object StateAboutSelect {


	object F extends DFSelectState  { // state object for flightData
		val rows: Seq[Row] = flightDf.collect().toSeq

		val C1: Int = rows.head.fieldIndex(flightDf.columns(0))
		val C2: Int = rows.head.fieldIndex(flightDf.columns(1))
		val C3: Int = rows.head.fieldIndex(flightDf.columns(2))

		/*val C0 = rows.head.fieldIndex("ORIGIN_COUNTRY_NAME")
		val C1 = rows.head.fieldIndex("DEST_COUNTRY_NAME")
		val C2 = rows.head.fieldIndex("count")*/

		val nameIndexMap: Map[NameOfCol, Int] = DFUtils.colnamesToIndices(flightDf)
		val nameTypeMap: Map[NameOfCol, DataType] = DFUtils.colnamesToDataTypes(flightDf)

	}

	object A extends DFSelectState { // state object for animal data

		val rows: Seq[Row] = animalDf.collect().toSeq

		val C1: Int = rows.head.fieldIndex(colnamesAnimal(0))
		val C2: Int = rows.head.fieldIndex(colnamesAnimal(1))
		val C3: Int = rows.head.fieldIndex(colnamesAnimal(2))
		val C4: Int = rows.head.fieldIndex(colnamesAnimal(3))

		val nameIndexMap: Map[NameOfCol, Int] = DFUtils.colnamesToIndices(animalDf)
		val nameTypeMap: Map[NameOfCol, DataType] = DFUtils.colnamesToDataTypes(animalDf)
	}


	case class TypeHolder[T: TypeTag]()

	def toRuntimeType[T: TypeTag](d: DataType): TypeHolder[T] = {
		d match {
			case NullType => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case StringType => TypeHolder[String]().asInstanceOf[TypeHolder[T]]
			//case CharType => Container[Double]().asInstanceOf[Container[T]]
			case IntegerType => TypeHolder[Integer]().asInstanceOf[TypeHolder[T]]
			case DoubleType => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case FloatType => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			//case DecimalType => Container[Double]().asInstanceOf[Container[T]]
			case LongType => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case ShortType => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case BooleanType => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case TimestampType => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case DateType => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case BinaryType => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case ByteType => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			//case MapType => Container[Double]().asInstanceOf[Container[T]]
			//case ArrayType => Container[Double]().asInstanceOf[Container[T]]

		}
	}
	def toRuntimeType[T: TypeTag](st: TypenameOfCol): TypeHolder[T] = {
		st match {
			case "Null" => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case "String" => TypeHolder[String]().asInstanceOf[TypeHolder[T]]
			//case CharType => Container[Double]().asInstanceOf[Container[T]]
			case "Integer" => TypeHolder[Integer]().asInstanceOf[TypeHolder[T]]
			case "Double" => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case "Float" => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			//case DecimalType => Container[Double]().asInstanceOf[Container[T]]
			case "Long" => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case "Short" => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case "Boolean" => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case "Timestamp" => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case "Date" => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case "Binary" => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			case "Byte" => TypeHolder[Double]().asInstanceOf[TypeHolder[T]]
			//case MapType => Container[Double]().asInstanceOf[Container[T]]
			//case ArrayType => Container[Double]().asInstanceOf[Container[T]]

		}
	}


	case class SelectLogicArgs[C: TypeTag](df: DataFrame, colName: NameOfCol, colnameToIndexMap: Map[NameOfCol, Int], tph: TypeHolder[C])


}
