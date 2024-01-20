package com.SparkDocumentationByTesting.AboutDataFrames

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

import utilities.DFUtils

//import com.SparkSessionForTests
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper // intercept


import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._
import com.data.util.EnumHub._


object StateForSelect {

	object F { // state object for flightData
		val rows: Seq[Row] = flightDf.collect().toSeq

		val C1: Int = rows.head.fieldIndex(flightDf.columns(0))
		val C2: Int = rows.head.fieldIndex(flightDf.columns(1))
		val C3: Int = rows.head.fieldIndex(flightDf.columns(2))

		/*val C0 = rows.head.fieldIndex("ORIGIN_COUNTRY_NAME")
		val C1 = rows.head.fieldIndex("DEST_COUNTRY_NAME")
		val C2 = rows.head.fieldIndex("count")*/

		val nameIndexMap: Map[String, Int] = DFUtils.colnamesToIndices(flightDf)
	}

	object A { // state object for animal data

		val rows: Seq[Row] = animalDf.collect().toSeq

		val C1: Int = rows.head.fieldIndex(colnamesAnimal(0))
		val C2: Int = rows.head.fieldIndex(colnamesAnimal(1))
		val C3: Int = rows.head.fieldIndex(colnamesAnimal(2))
		val C4: Int = rows.head.fieldIndex(colnamesAnimal(3))

		val nameIndexMap: Map[String, Int] = DFUtils.colnamesToIndices(animalDf)
	}
}


// TODO have this class underneath a trait called AboutColumns

/**
 * List testing = https://www.baeldung.com/scala/scalatest-compare-collections
 */
class AboutSelect extends AnyFunSpec with Matchers  with SparkSessionWrapper {


	import sparkSessionWrapper.implicits._

	import StateForSelect._


	// Identifying the types of the columns
	// flightDf.schema.map(_.dataType.typeName) shouldEqual List("string", "string", "long")



	// TODO - test simple select via $, "", col, df.col, column(), ', expr, expr.alias, selectExpr
	// TODO test select multiple cols
	// TODO permutate above two

	describe("Selecting"){

		describe("selecting via ColumnName"){

			// flight data
			it("example-spec test case"){
				flightDf.select($"DEST_COUNTRY_NAME").collect().toSeq should contain atLeastOneOf(
					Row("Iceland"), Row("Romania"), Row("UnitedStates"), Row("Italy"), Row("Pakistan")
				)
			}

			it("property-spec test case"){
				val colViaOverallCollect: Seq[Long] = flightDf.collect().toSeq.map(row => row.getAs[Long](F.C3))
				val colViaNameSelect: Seq[Long] = flightDf.select($"count").collect().toSeq.map(row => row.getAs[Long](0)) // use simple id = 0 because already selecting one column so the row will have length = 1

				val lenRowViaOverallCollect: Int = flightDf.collect().head.size
				val lenRowViaNameSelect: Int = flightDf.select($"count").collect().head.size

				colViaNameSelect shouldBe a[Seq[Long]]
				colViaOverallCollect shouldBe a[Seq[Long]]
				colViaNameSelect should equal(colViaOverallCollect)

				lenRowViaOverallCollect should equal(flightDf.columns.length)
				lenRowViaNameSelect should equal(1)
				lenRowViaOverallCollect should be >= lenRowViaNameSelect
			}
		}

		describe("selecting via string column name"){

			// animal data

			it("example-spec test case"){
				animalDf.select("Animal").collect().toSeq should contain atLeastOneOf(
					Row(Animal.Cat.Lion.toString),
					Row(Animal.SeaCreature.Dolphin.toString),
					Row(Animal.Elephant.toString),
					Row(Animal.Bird.Eagle.GoldenEagle.toString)
				)
			}

			it("property-spec test case"){
				val colViaOverallCollect: Seq[String] = animalDf.collect().toSeq.map(row => row.getAs[String](A.C4))
				val colViaNameSelect: Seq[String] = animalDf.select("Climate").collect().toSeq.map(row => row.getAs[String](0)) // use simple id = 0 because already selecting one column so the row will have length = 1

				val lenRowViaOverallCollect: Int = animalDf.collect().head.size
				val lenRowViaNameSelect: Int = animalDf.select("Climate").collect().head.size

				colViaNameSelect shouldBe a[Seq[String]]
				colViaOverallCollect shouldBe a[Seq[String]]
				colViaNameSelect should equal(colViaOverallCollect)


				lenRowViaOverallCollect should equal(animalDf.columns.length)
				lenRowViaNameSelect should equal(1)
				lenRowViaOverallCollect should be >= lenRowViaNameSelect
			}
			// TODO how to move these into property-based spec?
		}

		it("selecting via col() functions"){
			animalDf.select(animalDf.col("Climate")) shouldBe a [DataFrame]
			val animalClimateDfCol: Seq[String] = animalDf.select(animalDf.col("Climate")).collect().toSeq.map(row => row.getAs[String](0))

			animalClimateDfCol shouldBe a [Seq[String]]
			animalClimateDfCol should contain atLeastOneOf(Climate.Tundra.toString, Climate.Temperate.toString, Climate.Rainforest.toString)

			// ------

			animalDf.select(col("Climate")) shouldBe a[DataFrame]
			val animalClimateCol: Seq[String] = animalDf.select(col("Climate")).collect().toSeq.map(row => row.getAs[String](0))

			animalClimateCol shouldBe a[Seq[String]]
			animalClimateCol should contain atLeastOneElementOf (List(Climate.Tundra, Climate.Temperate, Climate.Rainforest).map(_.toString))
				//(Climate.Tundra.toString, Climate.Temperate.toString, Climate.Rainforest.toString)
		}

		it("selecting multiple columns at the same time"){

		}


		// TODO select by renaming. Example:
		// empDF.select($"*", sumTest as "running_total").show
		// Source = https://hyp.is/LMOsMpwxEe6XKGPBSFlVcw/alvinhenrick.com/2017/05/16/apache-spark-analytical-window-functions/
	}
}

