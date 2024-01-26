package com.SparkDocumentationByTesting.specs.AboutDataFrames

import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import utilities.DFUtils
import DFUtils.implicits._
import DFUtils.TypeAbstractions._


//import com.SparkSessionForTests
import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._
/*import AnimalDf._
import TradeDf._*/
import com.data.util.EnumHub._

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._

import utilities.SparkSessionWrapper


/**
 *
 */
class AboutRowSpecs extends AnyFunSpec with Matchers with SparkSessionWrapper {


	import com.SparkDocumentationByTesting.state.SpecState._

	/*import AnimalState._
	import FlightState._
	import TradeState._*/

	import sparkSessionWrapper.implicits._


	/**
	 * SOURCE:
	 * 	- spark RowTests = https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RowTest.scala#L59
	 */

	/**
	 * SOURCE = spark tests repo
	 * 	- RowTest: https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RowTest.scala#L126
	 */
	describe("Creating rows"){


		import Instrument.MusicalInstrument._
		import Instrument.FinancialInstrument._

		val natureSeq: Seq[EnumString] = Seq(Animal.Bird.Sparrow, Climate.Tundra, Country.Spain, Commodity.Gemstone.Sapphire).map(_.toString)
		val commoditySeq: Seq[EnumString] = Seq(Commodity.Gemstone.Opal, Commodity.Gemstone.Diamond, Commodity.Gemstone.Sapphire, Commodity.PreciousMetal.Silver, Commodity.PreciousMetal.Platinum).map(_.toString)


		describe("creating regular (external) Row (no schema)"){

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
		describe("creating InternalRow (no schema)"){


			val internalRowFromArgs: InternalRow = InternalRow(BassInstrument.Tuba, Voice, StringInstrument.Harp, WoodwindInstrument.Oboe, WoodwindInstrument.Flute, BassInstrument.FrenchHorn, Piano())

			val internalRowFromArgsAsList: InternalRow = InternalRow(natureSeq: _*)

			val internalRowFromSeq: InternalRow = InternalRow.fromSeq(commoditySeq)

			internalRowFromArgs shouldBe an [InternalRow]
			internalRowFromArgsAsList shouldBe an [InternalRow]
			internalRowFromSeq shouldBe an [InternalRow]

			// WARNING: has no schema attribute
			/*internalRowFromArgs.schema shouldBe null
			internalRowFromArgsAsList.schema shouldBe null
			internalRowFromSeq.schema shouldBe null*/
		}
		describe("creating GenericRowWithSchema (with schema)"){

			val natureSchema: StructType = StructType(Seq(
				StructField(Animal.toString, StringType),
				StructField(Climate.toString, StringType),
				StructField(Country.toString, StringType),
				StructField(Instrument.FinancialInstrument.Commodity.toString, StringType),
			))

			val genericSchemaRow = new GenericRowWithSchema(natureSeq.toArray, natureSchema)


			genericSchemaRow shouldBe a[GenericRowWithSchema]
			genericSchemaRow.schema shouldEqual  natureSchema
		}
		describe("creating GenericRow (no schema)"){

			// weird: requires explicit Array[Any]
			val genericRow = new GenericRow(natureSeq.toArray.map(_.asInstanceOf[Any]))

			genericRow shouldBe  a [ GenericRow ]
			genericRow.schema shouldEqual null
		}
	}

	describe("Accessing rows"){

		describe("accessing first row"){

			import AnimalDf._

			it("using first()"){
				animalDf.first() shouldBe Row(Animal.Cat.Lion.toString, 12, Country.Africa.toString, Climate.Tundra.toString)
			}
			it("using head"){
				animalDf.head shouldBe Row(Animal.Cat.Lion.toString, 12, Country.Africa.toString, Climate.Tundra.toString)
			}
		}

		describe("accessing an arbitrary row"){

			import AnimalDf._

			// TODO put this in the rdd tests
			// df.rdd.take(n)

			describe("Get nth row"){
				val n = 10 // scala.util.Random.between(0, animalDf.count()).toInt

				animalDf.take(n).drop(n-1).head shouldBe Row(Animal.Gorilla.toString, 43, Country.Africa.toString, Climate.Rainforest.toString)
			}
		}
	}





	describe("Row (with schema)" ) {


		describe("Accessing items within the row (with schema)") {

			describe("fieldIndex(colname) returns the column index corresponding to the colname" ) {

				it("from dataframe"){
					// "from dataframe  - comparing ways of calling fieldIndex()"
					val indexAnswer: Int = TradeState.C1
					val indexByMap = TradeState.mapOfNameToIndex(Instrument.FinancialInstrument.toString)
					val indexByFieldIndexFunc = TradeState.rows.head.fieldIndex(Instrument.FinancialInstrument.toString)

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
					sampleRow.fieldIndex("Animal") shouldBe 0
					sampleRow.fieldIndex("Country") shouldBe 3
				}
				it("from generic row, separate from dataframe (with schema)") {

					import com.SparkDocumentationByTesting.state.RowSpecState._

					pearlGSRow.fieldIndex(Animal.SeaCreature.toString) shouldBe 0
					pearlGSRow.getAs[String](Animal.SeaCreature.Pearl.toString) should equal("Pearl")

					pearlGSRow.fieldIndex("YearsOld") shouldBe 1
					pearlGSRow.getAs[Int]("YearsOld") should be >= 1000

					seahorseGSRow.fieldIndex(WaterType.toString) shouldBe 2

					anemoneGSRow.getAs[String](Animal.SeaCreature.toString) should equal("Anemone")

				}
			}

			describe("get"){

				import com.SparkDocumentationByTesting.state.RowSpecState._

				it("get(i) should return the value at position i in the Row with Any type") {

					pearlGSRow.get(0) shouldBe an[Any]
					pearlGSRow.get(0) shouldEqual "Pearl"

					pearlGNRow.get(0) shouldBe an[Any]
					pearlGNRow.get(0) shouldEqual "Pearl"

					pearlRow.get(0) shouldBe an [Any]
					pearlRow.get(0) shouldEqual "Pearl"
				}

				it("getAs[T] lets you specify the type of the item you want to get") {

					pearlGSRow.getAs[String](0) shouldBe a [String]
					pearlGSRow.getAs[String](0) shouldEqual "Pearl"
					pearlGSRow.getAs[String](Animal.SeaCreature.toString) shouldBe a [String]
					pearlGSRow.getAs[String](Animal.SeaCreature.toString) shouldEqual "Pearl"

					// TODO - to put these schema-less rows within the other test below?
					pearlGNRow.getAs[String](0) shouldBe a [String]
					pearlGNRow.getAs[String](0) shouldEqual "Pearl"
					// error
					// WARNING error must check before running so can intercept = need to move to other test below for no-schema rows.
					pearlGNRow.getAs[String](Animal.SeaCreature.toString)


					pearlRow.getAs[String](0) shouldBe a [String]
					pearlRow.getAs[String](0) shouldEqual "Pearl"
					// error
					pearlRow.getAs[String](Animal.SeaCreature.toString)


					TradeState.rows(3).getAs[Int](2) shouldBe an [Int]
					TradeState.rows(3).getAs[Int](2) should equal (10)
					TradeState.rows(3).getAs[Int]("Amount") shouldBe an [Int]
					TradeState.rows(3).getAs[Int]("Amount") should  equal (10)


					AnimalState.rows(10).get(3).asInstanceOf[String] shouldBe a [String]
					AnimalState.rows(10).get(3) should  equal (Climate.Rainforest.toString)
					AnimalState.rows(10).get(3).asInstanceOf[String] shouldEqual AnimalState.rows(10).getAs[String](3)



					// Cannot get a type that doesn't match the one specified in the function
					val catchIt = intercept[ClassCastException] {
						pearlGSRow.getAs[String]("YearsOld")
					}
					catchIt shouldBe a [ClassCastException]
				}

				it("specialized get functions let you return the item with a type also") {
					pearlGSRow.getString(0) shouldBe a[String]
					pearlGSRow.getString(0) shouldEqual "Pearl"
					//pearlGSRow.getAs[String](Animal.SeaCreature.toString) shouldBe a[String]
					//pearlGSRow.getAs[String](Animal.SeaCreature.toString) shouldEqual "Pearl"

					// TODO - to put these schema-less rows within the other test below?
					pearlGNRow.getString(0) shouldBe a[String]
					pearlGNRow.getString(0) shouldEqual "Pearl"

					pearlRow.getString(0) shouldBe a[String]
					pearlRow.getString(0) shouldEqual "Pearl"


					TradeState.rows(3).getInt(2) shouldBe an[Int]
					TradeState.rows(3).getInt(2) should equal(10)

					FlightState.rows(4).getLong(2) shouldBe a [Long]
					FlightState.rows(4).getLong(2) shouldEqual 3

					// Cannot get a type that doesn't match the one specified in the function
					val catchingException = intercept[ClassCastException] {
						pearlRow.getInt(0)
					}
					catchingException shouldBe a [ClassCastException]

				}
			}


			describe("getAsValuesMap[T] returns the values corresponding the keys, like getAs except can pass more values at once") {
				import com.SparkDocumentationByTesting.state.RowSpecState._

				// checking meaning of names list
				val keys: Array[String] = seaSchema.names.take(2)
				keys should contain allElementsOf Seq(Animal.SeaCreature.toString, "YearsOld")


				it("not asserting type for values makes them of type Nothing") {
					val expected: Map[EnumString, Any] = Map(Animal.SeaCreature.toString -> Animal.SeaCreature.Pearl.toString, "YearsOld" -> 1031)
					val result: Map[EnumString, Nothing] = pearlGSRow.getValuesMap(keys)

					expected shouldEqual result
					expected.values should not equal (result.values)

					expected.values shouldBe a[Iterable[Any]]
					result.values shouldBe a[Iterable[Nothing]]


					result.get(Animal.SeaCreature.toString) shouldBe a[Option[Nothing]]
					result.get(Animal.SeaCreature.toString) shouldBe a[Option[String]]
					result.get(Animal.SeaCreature.toString) shouldEqual Some(Animal.SeaCreature.Pearl.toString)

					result.get("YearsOld") shouldBe a[Option[Nothing]]
					result.get("YearsOld") shouldBe a[Option[String]]
					result.get("YearsOld") shouldEqual Some(1031)
					result.get("YearsOld") should not equal (Some("1031"))
				}

				it("can assert type T for values") {
					val expected: Map[EnumString, Any] = Map(Animal.SeaCreature.toString -> Animal.SeaCreature.Pearl.toString, "YearsOld" -> 1031)
					val result: Map[EnumString, String] = pearlGSRow.getValuesMap[String](keys)

					expected shouldEqual result
					expected.values should not equal (result.values)

					expected.values shouldBe a[Iterable[Any]]
					result.values shouldBe a[Iterable[String]]


					result.get(Animal.SeaCreature.toString) shouldBe a[Option[Nothing]]
					result.get(Animal.SeaCreature.toString) shouldBe a[Option[String]]
					result.get(Animal.SeaCreature.toString) shouldEqual Some(Animal.SeaCreature.Pearl.toString)

					result.get("YearsOld") shouldBe a[Option[Nothing]]
					result.get("YearsOld") shouldBe a[Option[String]]
					result.get("YearsOld") shouldEqual Some(1031) // HELP why int when T = string?
					result.get("YearsOld") should not equal (Some("1031")) // HELP weird because Map[Str -> Str] !!???
				}
			}
		}
	}

	describe("Row (without schema)") {

		describe("Accessing items within the row (without schema)"){

			describe("throws exception when") {

				import com.SparkDocumentationByTesting.state.RowSpecState._

				pearlGNRow.schema shouldBe null
				seahorseRow.schema shouldBe null


				it("using fieldIndex(colname)") {
					intercept[UnsupportedOperationException] {
						pearlGNRow.fieldIndex(Animal.SeaCreature.toString)
						seahorseRow.fieldIndex(Animal.SeaCreature.toString)
					}
				}
				it("using getAs(colname)") {
					intercept[UnsupportedOperationException] {
						pearlGNRow.getAs[String](Animal.SeaCreature.toString)
					}
					intercept[UnsupportedOperationException] {
						seahorseRow.getAs[String](Animal.SeaCreature.toString)
					}
				}
				it("NO exception when using getAs(i)") {
					pearlGNRow.getAs[Integer](1) shouldBe an[Integer]
					pearlGNRow.getAs[Integer](1) should equal(1031)

					seahorseRow.getAs[String](2) shouldBe a[String]
					seahorseRow.getAs[String](2) should equal(WaterType.Saltwater.toString)
				}

				it("using getValuesMap()") {

					// checking meaning of names list
					val keys: Array[String] = seaSchema.names.take(2)
					keys should contain allElementsOf Seq(Animal.SeaCreature.toString, "YearsOld")

					intercept[UnsupportedOperationException] {
						shrimpRow.getValuesMap(keys)
					}

					intercept[UnsupportedOperationException] {
						pearlGNRow.getValuesMap(keys)
					}
				}
			}
		}

	}
}
