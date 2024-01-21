package com.SparkDocumentationByTesting.specs.AboutDataFrames

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



// TODO have this class underneath a trait called AboutColumns

/**
 * List testing = https://www.baeldung.com/scala/scalatest-compare-collections
 */
class AboutSelect extends AnyFunSpec with Matchers  with SparkSessionWrapper {


	import sparkSessionWrapper.implicits._

	import com.SparkDocumentationByTesting.state.StateAboutSelect._


	// Identifying the types of the columns
	// flightDf.schema.map(_.dataType.typeName) shouldEqual List("string", "string", "long")



	// TODO - test simple select by $, "", col, df.col, column(), ', expr, expr.alias, selectExpr
	// TODO test select multiple cols
	// TODO permutate above two

	describe("Selecting"){

		describe("selecting by ColumnName"){

			// flight data
			flightDf.select($"DEST_COUNTRY_NAME").collect().toSeq should contain atLeastOneOf(
				Row("Iceland"), Row("Romania"), Row("UnitedStates"), Row("Italy"), Row("Pakistan")
			)

			it("property-spec test case"){

			}
		}


		describe("selecting by string column name"){

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
				val colByOverallCollect: Seq[String] = animalDf.collect().toSeq.map(row => row.getAs[String](A.C4))
				val colByNameSelect: Seq[String] = animalDf.select("Climate").collect().toSeq.map(row => row.getAs[String](0)) // use simple id = 0 because already selecting one column so the row will have length = 1

				val lenRowByOverallCollect: Int = animalDf.collect().head.size
				val lenRowByNameSelect: Int = animalDf.select("Climate").collect().head.size

				colByNameSelect shouldBe a[Seq[String]]
				colByOverallCollect shouldBe a[Seq[String]]
				colByNameSelect should equal(colByOverallCollect)


				lenRowByOverallCollect should equal(animalDf.columns.length)
				lenRowByNameSelect should equal(1)
				lenRowByOverallCollect should be >= lenRowByNameSelect
			}
			// TODO how to move these into property-based spec?
		}

		it("selecting by col() functions"){
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

