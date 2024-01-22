package com.SparkDocumentationByTesting.specs.AboutDataFrames

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

import utilities.DFUtils
import DFUtils.TypeAbstractions._
import DFUtils.implicits._

//import com.SparkSessionForTests
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper // intercept


import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums.{TradeDf, AnimalDf}
import TradeDf._
import AnimalDf._

import com.data.util.EnumHub._



// TODO have this class underneath a trait called AboutColumns

/**
 * List testing = https://www.baeldung.com/scala/scalatest-compare-collections
 */
class AboutSelectSpecs extends AnyFunSpec with Matchers  with SparkSessionWrapper {


	import sparkSessionWrapper.implicits._

	import com.SparkDocumentationByTesting.state.StateAboutSelect._


	// Identifying the types of the columns
	// flightDf.schema.map(_.dataType.typeName) shouldEqual List("string", "string", "long")



	// TODO - test simple select by $, "", col, df.col, column(), ', expr, expr.alias, selectExpr
	// TODO test select multiple cols
	// TODO permutate above two

	describe("Selecting columns"){


		it("selecting by string column name"){

			animalDf.select("Animal").collect().toSeq should contain allOf (
				Row(Animal.Cat.Lion.toString),
				Row(Animal.SeaCreature.Dolphin.toString),
				Row(Animal.Elephant.toString),
				Row(Animal.Bird.Eagle.GoldenEagle.toString)
			)

		}

		it("selecting by symbol ($) column name") {

			println(flightDf.select($"DEST_COUNTRY_NAME").collect().toSeq)
			val t1 = flightDf.select($"DEST_COUNTRY_NAME").collect().toSeq.map(row => row.getAs[String](0)).distinct
			println(s"t1  = $t1")
			//t1 should contain allOf ("Iceland", "Romania", "UnitedStates", "Italy", "Pakistan")
			// TODO fix why not working?

			//Seq(1,2,3) should contain allOf(1, 2)
			//flightDf.select($"count").collect().toSeq.map(row => row.getAs[Long](0)).distinct should contain atLeastOneOf (15, 1, 344, 15, 62, 1, 62, 588, 40)

		}

		/*describe("selecting by col() functions"){


			animalDf.select(animalDf.col("Country")).collect().toSeq should contain allOf(
				Seq(
					Row(Country.Africa),
					Row(Country.Brazil),
					Row(Country.Arabia)
				).map((row: Row) => row.mapRowStr):_*
			)

			animalDf.select(col("Animal")).collect().toSeq should contain allOf(Seq(
				Row(Animal.Cat.Lion),
				Row(Animal.SeaCreature.Dolphin),
				Row(Animal.Elephant),
				Row(Animal.Bird.Eagle.GoldenEagle)
			).map(_.toString):_*)

			animalDf.select(column("Climate")).collect().toSeq should contain allOf(Seq(
				Row(Climate.Tundra),
				Row(Climate.Temperate),
				Row(Climate.Rainforest),
				Row(Climate.Arid),
				Row(Climate.Mediterranean)
			).map(_.toString):_*)

			animalDf.select('Climate).collect().toSeq should contain allOf(Seq(
				Row(Climate.Continental),
				Row(Climate.Dry),
				Row(Climate.Polar)
			).map((r: Row) =>  .toString):_*)

		}

		describe("selecting multiple columns at the same time"){


			// WARNING: cannot mix Column objects and strings
			/*animalDf.select(animalDf.col("Animal"), col("Animal"), column("Animal"), $"Climate", expr("Country")).collect().toSeq.take(5).map(row => row.getAs[String](0)) should equal (Seq(
				(Animal.Cat.Lion, Animal.Cat.Lion, Animal.Cat.Lion, Climate.Tundra, Country.Africa),
				(Animal.Cat.Lion, Animal.Cat.Lion, Animal.Cat.Lion, Climate.Desert, Country.Arabia),
				(Animal.Hyena, Animal.Hyena, Animal.Hyena, Climate.Desert, Country.Africa),

				(Animal.Zebra, Animal.Zebra, Animal.Zebra, Climate.Arid, Country.Africa),
				(Animal.Giraffe, Animal.Giraffe, Animal.Giraffe, Climate.Tundra, Country.Africa),
			))*/
			val colsA: Seq[Seq[String]] = animalDf.select(animalDf.col("Animal"), col("Climate"), column("Animal"), $"Climate", expr("Country"))
				.collect()
				.toSeq
				.map(_.toSeq.asInstanceOf[Seq[String]])
				.transpose

			//colsA(0) should contain atLeastOneElementOf(Animal.values.map(_.toString))
			Animal.values.map(_.toString) should contain allElementsOf(colsA(0))
			Climate.values.map(_.toString) should contain allElementsOf(colsA(1))
			Animal.values.map(_.toString) should contain allElementsOf(colsA(2))
			Climate.values.map(_.toString) should contain allElementsOf(colsA(3))
			Country.values.map(_.toString) should contain allElementsOf(colsA(4))
		}*/


		// TODO select by renaming. Example:
		// empDF.select($"*", sumTest as "running_total").show
		// Source = https://hyp.is/LMOsMpwxEe6XKGPBSFlVcw/alvinhenrick.com/2017/05/16/apache-spark-analytical-window-functions/
	}
}

