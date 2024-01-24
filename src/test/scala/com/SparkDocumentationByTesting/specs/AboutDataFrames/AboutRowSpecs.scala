package com.SparkDocumentationByTesting.specs.AboutDataFrames


import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import utilities.DFUtils
import DFUtils.implicits._
import DFUtils.TypeAbstractions._


//import com.SparkSessionForTests
import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._
import AnimalDf._
import TradeDf._
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

		describe("regular (external) Row (no schema)"){

		}
		describe("InternalRow (no schema)"){

		}
		describe("GenericRowWithSchema"){

		}
		describe("GenericRow (no schema)"){

		}
	}

	describe("Accessing rows"){

		describe("Getting first row"){
			it("using first()"){
				animalDf.first() shouldBe Row(Animal.Cat.Lion.toString, 12, Country.Africa.toString, Climate.Tundra.toString)
			}
			it("using head"){
				animalDf.head shouldBe Row(Animal.Cat.Lion.toString, 12, Country.Africa.toString, Climate.Tundra.toString)
			}
		}

		describe("Getting rows other than the first one"){


			// TODO put this in the rdd tests
			// df.rdd.take(n)
			describe("Get nth row"){
				val n = 10 // scala.util.Random.between(0, animalDf.count()).toInt

				animalDf.take(n).drop(n-1).head shouldBe Row(Animal.Gorilla.toString, 43, Country.Africa.toString, Climate.Rainforest.toString)
			}
		}
	}

	describe("Row (with schema)" ) {


		describe("fieldIndex(colname) returns the column index corresponding to the colname") {

			it("from dataframe  - comparing ways of calling fieldIndex()") {
				val indexAnswer: Int = TradeState.C1
				val indexByMap = TradeState.mapOfNameToIndex(Instrument.FinancialInstrument.toString)
				val indexByFieldIndexFunc = TradeState.rows.head.fieldIndex(Instrument.FinancialInstrument.toString)

				indexAnswer shouldEqual 1
				indexByMap shouldEqual 1
				indexByFieldIndexFunc shouldEqual 1
			}
			it("from dataframe - comparing calling different columns") {

				val ir = scala.util.Random.between(minInclusive = 0, maxExclusive = AnimalState.rows.length)
				val sampleRow: Row = AnimalState.rows(ir)

				val ic = scala.util.Random.between(0, animalDf.columns.length)
				val anyColname: NameOfCol = colnamesAnimal(ic)

				sampleRow.fieldIndex(anyColname) shouldBe a[Int]
				sampleRow.fieldIndex("Animal") shouldBe 0
				sampleRow.fieldIndex("Country") shouldBe 3
			}

			import com.SparkDocumentationByTesting.state.RowSpecState._

			/*it("from dataframe row (with schema)"){
				dfGenericRowWithSchema.head.fieldIndex("YearsOld") shouldBe 1
				dfGenericRowWithSchema.head.getAs[String](Animal.SeaCreature.toString) shouldBe "Pearl"
			}*/
			it("from generic row, not from dataframe (with schema)") {


				pearlGSRow.fieldIndex(Animal.SeaCreature.toString) shouldBe 0
				pearlGSRow.getAs[String](Animal.SeaCreature.Pearl.toString) should equal("Pearl")

				pearlGSRow.fieldIndex("YearsOld") shouldBe 1
				pearlGSRow.getAs[Int]("YearsOld") should be >= 1000

				seahorseGSRow.fieldIndex(WaterType.toString) shouldBe 2

				anemoneGSRow.getAs[String](Animal.SeaCreature.toString) should equal("Anemone")

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
	}

	describe("Row (without schema)") {
		describe("throws exception when accessing by column name"){

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
					seahorseRow.getAs[String](Animal.SeaCreature.toString)
				}
				intercept[UnsupportedOperationException] {
					pearlGNRow.getAs[Integer](1)
					seahorseRow.getAs[Integer](2)
				}
			}

			// HELP - cannot find way to create dataframe with no schema even if the Row has no schema
			/*it("from dataframe (no schema)"){

			}*/
		}

	}
}
