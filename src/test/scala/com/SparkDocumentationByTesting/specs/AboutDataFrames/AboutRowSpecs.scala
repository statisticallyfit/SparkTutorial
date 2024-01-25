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


		describe("Accessing items within the row") {

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
		}

		describe("getAs[T] returns the value corresponding to the given input") {
			import com.SparkDocumentationByTesting.state.RowSpecState._

			it("input can be colname") {
				pearlGNRow.getAs[String](Animal.SeaCreature.toString) shouldBe "Pearl"
			}
			it("input can be column index") {
				shrimpGNRow.getAs[String](2) shouldBe "Freshwater" //WaterType.Freshwater.toString
			}
		}

		describe("getAsValuesMap[T] returns the values corresponding the keys, like getAs except can pass more values at once"){
			import com.SparkDocumentationByTesting.state.RowSpecState._

			it("input is only a colname, not an index"){

			}
		}
	}

	describe("Row (without schema)") {
		describe("throws exception when accessing by input index"){

			import com.SparkDocumentationByTesting.state.RowSpecState._

			pearlGNRow.schema shouldBe null
			seahorseRow.schema shouldBe null


			it("using fieldIndex(colname)"){
				intercept[UnsupportedOperationException]{
					pearlGNRow.fieldIndex(Animal.SeaCreature.toString)
					seahorseRow.fieldIndex(Animal.SeaCreature.toString)
				}
			}
			it("using getAs"){
				intercept[UnsupportedOperationException] {
					pearlGNRow.getAs[String](Animal.SeaCreature.toString)
				}
				intercept[UnsupportedOperationException]{
					seahorseRow.getAs[String](Animal.SeaCreature.toString)
				}

				// TODO fix - this one works
				intercept[UnsupportedOperationException] {
					pearlGNRow.getAs[Integer](1)
					seahorseRow.getAs[Integer](2)
				}
			}

			// TODO - show getAs, getvaluesmap - don't work

			// HELP - cannot find way to create dataframe with no schema even if the Row has no schema
			/*it("from dataframe (no schema)"){

			}*/
		}

	}
}
