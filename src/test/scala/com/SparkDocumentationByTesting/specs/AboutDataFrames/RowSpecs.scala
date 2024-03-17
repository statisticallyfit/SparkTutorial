package com.SparkDocumentationByTesting.specs.AboutDataFrames


import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._ //genericrowwithschema...
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, Dataset, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize }
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

import utilities.DFUtils; import DFUtils._ ; import DFUtils.TypeAbstractions._; import DFUtils.implicits._
import utilities.GeneralMainUtils._
import utilities.GeneralMainUtils.implicits._
import utilities.DataHub.ManualDataFrames.fromEnums._
import ArtistDf._
import TradeDf._
import AnimalDf._

import utilities.EnumUtils.implicits._
import utilities.EnumHub._
import Human._
import ArtPeriod._
import Artist._
import Scientist._ ; import NaturalScientist._ ; import Mathematician._;  import Engineer._
import Craft._;
import Art._; import Literature._; import PublicationMedium._;  import Genre._
import Science._; import NaturalScience._ ; import Mathematics._ ; import Engineering._ ;
import Animal._; import SeaCreature._

//import com.SparkSessionForTests
import com.SparkDocumentationByTesting.CustomMatchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import org.scalatest.Assertions._
import utilities.SparkSessionWrapper

import World.Africa._
import World.Europe._
import World.NorthAmerica._
import World.SouthAmerica._
import World._
import World.Asia._
import World.Oceania._
import World.CentralAmerica._



import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._

import utilities.SparkSessionWrapper


/**
 * // TODO refactor to be organized by the function being described not by whether it is Row with/out schema.
 *
 */
class RowSpecs extends AnyFunSpec with Matchers with SparkSessionWrapper {


	import com.SparkDocumentationByTesting.state.SpecState._

	/*import AnimalState._
	import FlightState._
	import TradeState._*/

	import sparkSessionWrapper.implicits._


	/**
	 * SOURCE:
	 * 	- spark RowTests = https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RowTest.scala#L59
	 */

	describe("Creating rows") {


		import Instrument.MusicalInstrument._
		import Instrument.FinancialInstrument._

		val natureSeq: Seq[EnumString] = Seq(Animal.Bird.Sparrow, ClimateZone.Tundra, Spain, Commodity.Gemstone.Sapphire).enumNames //.map(_.toString)
		val commoditySeq: Seq[EnumString] = Seq(Commodity.Gemstone.Opal, Commodity.Gemstone.Diamond, Commodity.Gemstone.Sapphire, Commodity.PreciousMetal.Silver, Commodity.PreciousMetal.Platinum).enumNames //.map(_.toString)


		it("creating regular (external) Row (no schema)") {

			val externalRowFromArgs: Row = Row(BassInstrument.Tuba, Voice, StringInstrument.Harp, WoodwindInstrument.Oboe, WoodwindInstrument.Flute, BassInstrument.FrenchHorn, Piano())

			val externalRowFromArgsAsList: Row = Row(natureSeq: _*)

			val externalRowFromSeq: Row = Row.fromSeq(commoditySeq)

			externalRowFromArgs shouldBe a[Row]
			externalRowFromArgsAsList shouldBe a[Row]
			externalRowFromSeq shouldBe a[Row]

			externalRowFromArgs.schema shouldBe null
			externalRowFromArgsAsList.schema shouldBe null
			externalRowFromSeq.schema shouldBe null

		}
		it("creating InternalRow (no schema)") {

			val internalRowFromArgs: InternalRow = InternalRow(BassInstrument.Tuba, Voice, StringInstrument.Harp, WoodwindInstrument.Oboe, WoodwindInstrument.Flute, BassInstrument.FrenchHorn, Piano())

			val internalRowFromArgsAsList: InternalRow = InternalRow(natureSeq: _*)

			val internalRowFromSeq: InternalRow = InternalRow.fromSeq(commoditySeq)

			internalRowFromArgs shouldBe an[InternalRow]
			internalRowFromArgsAsList shouldBe an[InternalRow]
			internalRowFromSeq shouldBe an[InternalRow]

			// WARNING: has no schema attribute
			/*internalRowFromArgs.schema shouldBe null
			internalRowFromArgsAsList.schema shouldBe null
			internalRowFromSeq.schema shouldBe null*/
		}
		it("creating GenericRowWithSchema (with schema)") {

			val natureSchema: StructType = StructType(Seq(
				StructField(Animal.enumName, StringType),
				StructField(ClimateZone.enumName, StringType),
				StructField(World.enumName, StringType),
				StructField(Instrument.FinancialInstrument.Commodity.enumName, StringType),
			))

			val genericSchemaRow = new GenericRowWithSchema(natureSeq.toArray, natureSchema)


			genericSchemaRow shouldBe a[GenericRowWithSchema]
			genericSchemaRow.schema shouldEqual natureSchema
		}
		it("creating GenericRow (no schema)") {

			// weird: requires explicit Array[Any]
			val genericRow = new GenericRow(natureSeq.toArray.map(_.asInstanceOf[Any]))

			genericRow shouldBe a[GenericRow]
			genericRow.schema shouldEqual null
		}
	}

	describe("Accessing rows from a DataFrame") {

		describe("accessing first row") {

			import AnimalDf._

			it("using first()") {
				animalDf.first() shouldBe Row(Animal.Cat.WildCat.Lion.enumName, 12, World.Africa.enumName, ClimateZone.Tundra.enumName)
			}
			it("using head") {
				animalDf.head shouldBe Row(Animal.Cat.WildCat.Lion.enumName, 12, World.Africa.enumName, ClimateZone.Tundra.enumName)
			}
		}

		describe("accessing an arbitrary row") {

			import AnimalDf._
			// df.rdd.take(n)

			it("Get nth row") {
				val n = 11 // scala.util.Random.between(0, animalDf.count()).toInt

				animalDf.take(n + 1).drop(n).head shouldBe Row(Animal.Gorilla.enumName, 43, World.Africa.enumName, ClimateZone.Rainforest.enumName)
			}
		}
	}
	// TODO accessing rows from DataSet, RDD...




	describe("Accessing items within a row") {


		import com.SparkDocumentationByTesting.state.RowSpecState._


		describe("fieldIndex(colname) - returns the column index corresponding to the colname") {

			it("for external Row from dataframe (with schema)") {

				// "from dataframe  - comparing ways of calling fieldIndex()"
				val indexAnswer: Int = TradeState.C1
				val indexByMap = TradeState.mapOfNameToIndex(Instrument.FinancialInstrument.enumName)
				val indexByFieldIndexFunc = TradeState.rows.head.fieldIndex(Instrument.FinancialInstrument.enumName)

				indexAnswer shouldEqual 1
				indexByMap shouldEqual 1
				indexByFieldIndexFunc shouldEqual 1


				// from dataframe - comparing calling different columns
				import AnimalDf._

				val ir = scala.util.Random.between(minInclusive = 0, maxExclusive = AnimalState.rows.length)
				val sampleRow: Row = AnimalState.rows(ir)

				val ic = scala.util.Random.between(0, animalDf.columns.length)
				val anyColname: NameOfCol = colnamesAnimal(ic)

				sampleRow.fieldIndex(anyColname) shouldBe a[Int]
				sampleRow.fieldIndex(Animal.enumName) shouldBe 0
				sampleRow.fieldIndex(World.enumName) shouldBe 2

			}
			it("on generic row, separate from dataframe (with schema)") {

				pearlGSRow shouldBe a [GenericRowWithSchema]
				pearlGSRow.schema should equal (seaSchema)
				//pearlGSRow.schema should not be null

				pearlGSRow.fieldIndex(Animal.SeaCreature.enumName) shouldBe 0
				pearlGSRow.getAs[String](Animal.SeaCreature.enumName) should equal(Pearl.enumName)

				pearlGSRow.fieldIndex("YearsOld") shouldBe 1
				pearlGSRow.getAs[Int]("YearsOld") should be >= 1000

				seahorseGSRow.fieldIndex(WaterType.enumName) shouldBe 2

				anemoneGSRow.getAs[String](Animal.SeaCreature.enumName) should equal(Anemone.enumName)

			}
			it("throws exception for GenericRow (no schema)") {

				pearlGNRow shouldBe a [GenericRow]
				pearlGNRow.schema shouldBe null

				intercept[UnsupportedOperationException] {
					pearlGNRow.fieldIndex(Animal.SeaCreature.enumName)
				}
			}

			it("throws exception for external Row (no schema)") {
				seahorseRow shouldBe a [Row]
				seahorseRow.schema shouldBe null

				intercept[UnsupportedOperationException] {
					seahorseRow.fieldIndex(Animal.SeaCreature.enumName)
				}
			}
		}

		describe("get - returns item within Row") {


			describe("get(i) should return the value at position i in the Row with Any type") {

				it("on generic row (with schema)"){
					pearlGSRow shouldBe a [GenericRowWithSchema]
					pearlGSRow.schema should equal (seaSchema)

					pearlGSRow.get(0) shouldBe an[Any]
					pearlGSRow.get(0) shouldEqual Pearl.enumName
				}
				it("on generic row (no schema)"){
					pearlGNRow shouldBe a [GenericRow]
					pearlGNRow.schema shouldBe null

					pearlGNRow.get(0) shouldBe an[Any]
					pearlGNRow.get(0) shouldEqual Pearl.enumName
				}
				it("on external Row (with schema), from dataframe"){
					val aDfRow: Row = TradeState.rows(3)

					aDfRow shouldBe a[Row]
					aDfRow.schema should equal(TradeDf.tradeSchema)

					aDfRow.get(1) shouldBe an [Any]
					aDfRow.get(1) shouldEqual Instrument.FinancialInstrument.Bond.enumName
				}
				it("on external Row (no schema)"){

					pearlRow shouldBe a [Row]
					pearlRow.schema shouldBe null

					pearlRow.get(0) shouldBe an[Any]
					pearlRow.get(0) shouldEqual Pearl.enumName
				}
			}

			describe("getAs[T] lets you specify the type of the item you want to get") {

				describe("on generic row (with schema)"){
					pearlGSRow shouldBe a [GenericRowWithSchema]
					pearlGSRow.schema shouldEqual seaSchema

					it("works when passed index") {
						pearlGSRow.getAs[String](0) shouldBe a[String]
						pearlGSRow.getAs[String](0) shouldEqual Pearl.enumName
					}
					it("works when passed colname") {
						pearlGSRow.getAs[String](Animal.SeaCreature.enumName) shouldBe a[String]
						pearlGSRow.getAs[String](Animal.SeaCreature.enumName) shouldEqual Pearl.enumName
					}
				}

				describe("on external Row (with schema), from dataframe") {
					val aDfRow: Row = TradeState.rows(3)

					aDfRow shouldBe a[Row]
					aDfRow.schema should equal (TradeDf.tradeSchema) //not be null

					it("works when passed index") {
						aDfRow.getAs[Int](2) shouldBe an[Int]
						aDfRow.getAs[Int](2) should equal(10)
					}
					it("works when passed colname") {
						aDfRow.getAs[Int]("Amount") shouldBe an[Int]
						aDfRow.getAs[Int]("Amount") should equal(10)
					}
				}

				describe("on generic row (no schema)"){

					pearlGNRow shouldBe a [GenericRow]
					pearlGNRow.schema should equal(null)

					it("works when passed index") {
						pearlGNRow.getAs[String](0) shouldBe a[String]
						pearlGNRow.getAs[String](0) shouldEqual Pearl.enumName
					}
					it("throws exception when passed colname") {
						val catchThat = intercept[UnsupportedOperationException] {
							pearlGNRow.getAs[String](Animal.SeaCreature.enumName)
						}
						catchThat shouldBe an[UnsupportedOperationException]
						catchThat should not equal (Pearl.enumName)
					}
				}

				describe("on external Row (no schema)"){

					pearlRow shouldBe a[Row]
					pearlRow.schema should equal(null)

					it("works when passed index") {
						pearlRow.getAs[String](0) shouldBe a[String]
						pearlRow.getAs[String](0) shouldEqual Pearl.enumName
					}
					it("throws exception when passed colname") {
						val catchThat = intercept[UnsupportedOperationException] {
							pearlRow.getAs[String](Animal.SeaCreature.enumName)
						}
						catchThat shouldBe an[UnsupportedOperationException]
						catchThat should not equal (Pearl.enumName)
					}
					// TODO internal Row for all these tests
				}




				describe("throws exception when item types don't correspond") {

					it("for generic row (with schema)"){
						pearlGSRow shouldBe a [GenericRowWithSchema]
						pearlGSRow.schema shouldEqual seaSchema

						val catchThat = intercept[ClassCastException] {
							pearlGSRow.getAs[String]("YearsOld")
						}
						catchThat shouldBe a[ClassCastException]
						catchThat should not equal (pearlGSRow.getAs[Int]("YearsOld"))
					}

					it("for external row (with schema), from dataframe"){
						val aDfRow: Row = AnimalState.rows(10)

						aDfRow shouldBe a [Row]
						aDfRow.schema shouldEqual AnimalDf.animalDf.schema

						val catchIt = intercept[ClassCastException]{
							aDfRow.getAs[Int](0)
						}
						catchIt shouldBe a [ClassCastException]
					}

					// NOTE: it is UnsupportedOperationException not ClassCastException because first of all it is not even possible to index into the row using a name when row has no schema, so it hits that error first. Only if that would pass and type were still incorrect would the ClassCastException be thrown.

					it("for external row (no schema)"){
						shrimpRow shouldBe a [Row]
						shrimpRow.schema shouldEqual null

						val catchCastEx = intercept[ClassCastException] {
							shrimpRow.getAs[Int](0)
							//fieldIndex on a Row without schema is undefined.
						}
						catchCastEx shouldBe a[ClassCastException]
						//catchCastEx.getMessage should (include("class java.lang.String cannot be cast to class java.lang.Integer"))
						// string should (include("seven") and include("eight") and include("nine"))

						val catchUnsuppEx = intercept[UnsupportedOperationException] {
							shrimpRow.getAs[Int](Animal.SeaCreature.enumName)
						}
						catchUnsuppEx shouldBe a[UnsupportedOperationException]
						//catchUnsuppEx.getMessage should (include("fieldIndex on a Row without schema is undefined."))
					}

					it("for generic row (no schema)"){
						anemoneGNRow shouldBe a[GenericRow]
						anemoneGNRow.schema shouldEqual null

						val catchCastEx = intercept[ClassCastException]{
							anemoneGNRow.getAs[Int](0)
							//fieldIndex on a Row without schema is undefined.
						}
						catchCastEx shouldBe a [ClassCastException]
						//catchCastEx.getMessage should (include ("class java.lang.String cannot be cast to class java.lang.Integer"))
						// string should (include("seven") and include("eight") and include("nine"))

						val catchUnsuppEx = intercept[UnsupportedOperationException] {
							anemoneGNRow.getAs[Int](Animal.SeaCreature.enumName)
						}
						catchUnsuppEx shouldBe a[UnsupportedOperationException]
						//catchUnsuppEx.getMessage should (include ("fieldIndex on a Row without schema is undefined."))
					}

				}


				it("asserting type over get(i) is the same as calling getAs[T]") {

					val aDfRow: Row = AnimalState.rows(11)


					aDfRow.get(3) should equal(ClimateZone.Rainforest.enumName)
					aDfRow.get(3).asInstanceOf[String] shouldEqual aDfRow.getAs[String](3)
				}
			}


			describe("specialized get functions let you return the item with a type also") {

				//import com.SparkDocumentationByTesting.state.RowSpecState._

				it("for generic Row (with schema)") {
					pearlGSRow shouldBe a [GenericRowWithSchema]
					pearlGSRow.schema should not be null

					pearlGSRow.getString(0) shouldBe a[String]
					pearlGSRow.getString(0) shouldEqual Pearl.enumName

				}
				it("for external Row (with schema), from dataframe") {
					val aDfRow: Row = TradeState.rows(3)
					val aDfRow2: Row = FlightState.rows(4)

					aDfRow shouldBe a [Row]
					aDfRow.schema should not be null

					aDfRow.getInt(2) shouldBe an[Int]
					aDfRow.getInt(2) should equal(10)

					aDfRow2.getLong(2) shouldBe a[Long]
					aDfRow2.getLong(2) shouldEqual 62
				}

				it("for external row (no schema)") {
					shrimpRow shouldBe a[Row]
					shrimpRow.schema shouldEqual null

					shrimpRow.getString(2) shouldBe a [String]
					shrimpRow.getString(2) shouldEqual WaterType.Freshwater.enumName
				}
				it("for generic row (no schema)") {
					anemoneGNRow shouldBe a[GenericRow]
					anemoneGNRow.schema shouldEqual null

					anemoneGNRow.getString(0) shouldBe a[String]
					anemoneGNRow.getString(0) shouldEqual Anemone.enumName
				}


				it("throws exception when item types don't correspond") {
					intercept[ClassCastException] {
						pearlGSRow.getInt(0)
					}
					intercept[ClassCastException] {
						pearlGNRow.getInt(0)
					}
					intercept[ClassCastException] {
						pearlRow.getInt(0)
					}
					intercept[ClassCastException] {
						TradeState.rows(3).getInt(0)
					}
				}

			}


			describe("getAsValuesMap[T] returns the values corresponding the keys, like getAs except can pass more values at once") {

				val expected: Map[EnumString, Any] = Map(Animal.SeaCreature.enumName -> Pearl.enumName, "YearsOld" -> 1031)

				// checking meaning of names list
				val keys: Array[String] = seaSchema.names.take(2)
				keys should contain allElementsOf Seq(Animal.SeaCreature.enumName, "YearsOld")


				describe("for generic Row (with schema)"){
					it("not asserting type for values makes them of type Nothing") {
						val result: Map[EnumString, Nothing] = pearlGSRow.getValuesMap(keys)

						expected shouldEqual result
						expected.values should not equal (result.values)

						expected.values shouldBe a[Iterable[Any]]
						result.values shouldBe a[Iterable[Nothing]]


						result.get(Animal.SeaCreature.enumName) shouldBe a[Option[Nothing]]
						result.get(Animal.SeaCreature.enumName) shouldBe a[Option[String]]
						result.get(Animal.SeaCreature.enumName) shouldEqual Some(Pearl.enumName)

						result.get("YearsOld") shouldBe a[Option[Nothing]]
						result.get("YearsOld") shouldBe a[Option[String]]
						result.get("YearsOld") shouldEqual Some(1031)
						result.get("YearsOld") should not equal (Some("1031"))
					}

					it("can assert type T for values") {
						val result: Map[EnumString, String] = pearlGSRow.getValuesMap[String](keys)

						expected shouldEqual result
						expected.values should not equal (result.values)

						expected.values shouldBe a[Iterable[Any]]
						result.values shouldBe a[Iterable[String]]


						result.get(Animal.SeaCreature.enumName) shouldBe a[Option[Nothing]]
						result.get(Animal.SeaCreature.enumName) shouldBe a[Option[String]]
						result.get(Animal.SeaCreature.enumName) shouldEqual Some(Pearl.enumName)

						result.get("YearsOld") shouldBe a[Option[Nothing]]
						result.get("YearsOld") shouldBe a[Option[String]]
						result.get("YearsOld") shouldEqual Some(1031) // HELP why int when T = string?
						result.get("YearsOld") should not equal (Some("1031")) // HELP weird because Map[Str -> Str] !!???
					}
				}



				it("throws exception for GenericRow (no schema)") {

					pearlGNRow shouldBe a [GenericRow]
					pearlGNRow.schema shouldEqual null

					//val result: Map[EnumString, Nothing] = pearlGSRow.getValuesMap(keys)

					val catchIt = intercept[UnsupportedOperationException] {
						pearlGNRow.getValuesMap(keys)
					}
					catchIt shouldBe a[UnsupportedOperationException]
					catchIt.getMessage shouldEqual "fieldIndex on a Row without schema is undefined."
					catchIt should not equal (expected)
				}
			}
		}
	}
}
