package com.SparkDocumentationByTesting.state

import scala.reflect.runtime.universe._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import utilities.EnumUtils.implicits._
import utilities.DFUtils
import utilities.DFUtils.TypeAbstractions._


import utilities.DataHub.ImportedDataFrames.fromBillChambersBook._
import utilities.DataHub.ManualDataFrames.fromEnums.{TradeDf, AnimalDf}
import TradeDf._
import AnimalDf._
import utilities.EnumHub._

import World.Africa._
import World.Europe._
import World.NorthAmerica._
import World.SouthAmerica._
import World._
import World.Asia._
import World.Oceania._
import World.CentralAmerica._

/**
 *
 */

trait SpecState {

	val rows: Seq[Row] //rows of the dataframe

	// cannot put arbitrary number of ints here
	// C0, C1...

	// The map of colname-to-index, of the df
	val mapOfNameToIndex: Map[NameOfCol, Int]
	// The map of colname-to-strtype, of the df, where strtype = datatype converted to string format
	val mapOfNameToType: Map[NameOfCol, DataType]
}

object SpecState {


	object FlightState extends SpecState  { // state object for flightData
		val rows: Seq[Row] = flightDf.collect().toSeq

		val C0: Int = rows.head.fieldIndex(flightDf.columns(0))
		val C1: Int = rows.head.fieldIndex(flightDf.columns(1))
		val C2: Int = rows.head.fieldIndex(flightDf.columns(2))

		/*val C0 = rows.head.fieldIndex("ORIGIN_COUNTRY_NAME")
		val C1 = rows.head.fieldIndex("DEST_COUNTRY_NAME")
		val C2 = rows.head.fieldIndex("count")*/

		val mapOfNameToIndex: Map[NameOfCol, Int] = DFUtils.colnamesToIndices(flightDf)
		val mapOfNameToType: Map[NameOfCol, DataType] = DFUtils.colnamesToDataTypes(flightDf)

	}

	object TradeState extends SpecState { // state object for animal data

		val rows: Seq[Row] = tradeDf.collect().toSeq

		val C0: Int = rows.head.fieldIndex(colnamesTrade(0))
		val C1: Int = rows.head.fieldIndex(colnamesTrade(1))
		val C2: Int = rows.head.fieldIndex(colnamesTrade(2))
		val C3: Int = rows.head.fieldIndex(colnamesTrade(3))
		val C4: Int = rows.head.fieldIndex(colnamesTrade(4))

		val mapOfNameToIndex: Map[NameOfCol, Int] = DFUtils.colnamesToIndices(tradeDf)
		val mapOfNameToType: Map[NameOfCol, DataType] = DFUtils.colnamesToDataTypes(tradeDf)


		val coupleOfCompanies: Seq[Company] = Seq(
			Company.Ford, Company.Apple, Company.IBM, Company.Samsung, Company.JPMorgan, Company.Google
		)//.map(_.toString)
		val coupleOfFinancialInstrs: Seq[FinancialInstrument] = Seq(
			Instrument.FinancialInstrument.Stock,
			Instrument.FinancialInstrument.Swap,
			Instrument.FinancialInstrument.Bond,
			Instrument.FinancialInstrument.Commodity.PreciousMetal.Gold,
			Instrument.FinancialInstrument.Derivative,
			Instrument.FinancialInstrument.Commodity.Gemstone.Ruby
		)//.map(_.toString)

		val allTransactions: Seq[Transaction] = Seq(
			Transaction.Buy, Transaction.Sell
		)//.map(_.toString)

		val coupleOfCountries: Seq[World] = Seq(
			China,
			Ireland,
			Argentina,
			Canada,
			Spain
		)//.map(_.toString)
	}

	object AnimalState extends SpecState { // state object for animal data

		val rows: Seq[Row] = animalDf.collect().toSeq

		val C0: Int = rows.head.fieldIndex(colnamesAnimal(0))
		val C1: Int = rows.head.fieldIndex(colnamesAnimal(1))
		val C2: Int = rows.head.fieldIndex(colnamesAnimal(2))
		val C3: Int = rows.head.fieldIndex(colnamesAnimal(3))

		val mapOfNameToIndex: Map[NameOfCol, Int] = DFUtils.colnamesToIndices(animalDf)
		val mapOfNameToType: Map[NameOfCol, DataType] = DFUtils.colnamesToDataTypes(animalDf)


		val coupleOfCountries: Seq[World] = Seq(Africa, Brazil, Arabia, Russia) //.names //.map(enum => enum.toString)
		val coupleOfAnimals: Seq[Animal] = Seq(
			Animal.Cat.WildCat.Lion,
			Animal.SeaCreature.Dolphin,
			Animal.Elephant,
			Animal.Bird.Eagle.GoldenEagle
		)//.map(_.toString)

		val coupleOfClimates: Seq[ClimateZone] = Seq(
			ClimateZone.Tundra,
			ClimateZone.Temperate,
			ClimateZone.Tropical,
			ClimateZone.Arid,
			ClimateZone.Dry,
			ClimateZone.Desert,
			ClimateZone.Mediterranean,
			ClimateZone.Continental,
			ClimateZone.Arctic
		)//.map(_.toString)
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
