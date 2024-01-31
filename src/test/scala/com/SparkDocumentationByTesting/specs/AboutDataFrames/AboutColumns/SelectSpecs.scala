package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._

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
 * List testing = https://www.baeldung.com/scala/scalatest-compare-collections
 */
class SelectSpecs extends AnyFunSpec with Matchers  with SparkSessionWrapper {


	import com.SparkDocumentationByTesting.state.SpecState._
	import AnimalState._
	import sparkSessionWrapper.implicits._


	// Identifying the types of the columns
	// flightDf.schema.map(_.dataType.typeName) shouldEqual List("string", "string", "long")


	/**
	 * SOURCE:
	 */

	// TODO - test simple select by $, "", col, df.col, column(), ', expr, expr.alias, selectExpr
	// TODO test select multiple cols
	// TODO permute above two

	// TODO update alias, as, name = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L126

	// TODO test select produced by binary operator = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L154

	// TODO select after doing operations on a column = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L199-L260

	describe("Selecting columns"){

		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
		 */
		it("selecting by string column name"){

			animalDf.select("Animal").collect().toSeq should contain allOf (
				Row(Animal.Cat.Lion.toString),
				Row(Animal.SeaCreature.Dolphin.toString),
				Row(Animal.Elephant.toString),
				Row(Animal.Bird.Eagle.GoldenEagle.toString)
			)

		}

		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
		 */
		it("selecting by symbol ($) column name") {

			//println(flightDf.select($"DEST_COUNTRY_NAME").collect().toSeq)
			animalDf.select($"Climate").collectCol[String] should contain allElementsOf coupleOfClimates


		}

		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
		 */
		it("selecting by col() functions"){
			animalDf.select(animalDf.col("Country")).collectCol[String].distinct should contain allElementsOf coupleOfCountries

			animalDf.select(col("Animal")).collectCol[String].distinct should contain allElementsOf coupleOfAnimals

			animalDf.select(column("Climate")).collectCol[String] should contain allElementsOf coupleOfClimates

			val apostropheWay: Seq[EnumString] = animalDf.select('Climate).collectCol[String]
			val symbolWay: Seq[EnumString] = animalDf.select(Symbol("Climate")).collectCol[String]
			apostropheWay should equal (symbolWay)
			apostropheWay should contain allElementsOf coupleOfClimates
		}

		it("selecting by df() itself"){
			animalDf.select(animalDf("Country")).collectCol[String].distinct should contain allElementsOf( coupleOfCountries )
		}

		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
		 */
		it("selecting by expr() and selectExpr()"){

			animalDf.select(expr("Country")).collectCol[String] should contain allElementsOf coupleOfCountries

			val resultDf = animalDf.selectExpr("Country as NewCountryName", "Animal as TheZoo")
			resultDf.columns.length shouldEqual 2
			resultDf.columns shouldEqual Seq("NewCountryName", "TheZoo")
			resultDf.select(expr("NewCountryName")).collectCol[String] should contain allElementsOf coupleOfCountries
			resultDf.select(expr("TheZoo")).collectCol[String] should contain allElementsOf coupleOfAnimals
		}

		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
		 */
		// TODO find more examples of selectExpr - categorize the actions that can be done
		it("selectExpr() allows doing more operations besides selecting a column"){
			val resultDf = flightDf.selectExpr(
				"*", //select all original columns
				"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as isWithinCountry" // specifying if destination and origin are the same
			)

			resultDf.columns.length should be (flightDf.columns.length + 1)
			resultDf.select($"isWithinCountry").collectCol[Boolean].take(10) should equal(Seq(false, false, false, false, false, false, false, false, true, false))
		}

		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
		 */
		it("selectExpr() - doing aggregations"){
			val resultDf = flightDf.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))")

			resultDf.count() should be (1) // since each calculation yields a single number
			resultDf.columns.length should be (2)
			resultDf.columns should equal (Seq("avg(count)", "count(distinct(DEST_COUNTRY_NAME))"))

			// ---------
			val avgExpected = 1770.765625
			val avgBySelectExpr = resultDf.select($"avg(count)").collectCol[Double]
			val avgByOtherMethod = flightDf.withColumn("avgCount", avg("count")).select("avgCount").collectCol[Double]

			avgBySelectExpr shouldEqual Seq(avgExpected)
			avgByOtherMethod shouldEqual Seq(avgExpected)

			// ---------
			val countDistinctExpected = 132
			val countDistinctBySelectExpr = resultDf.select($"count(distinct(DEST_COUNTRY_NAME))").collectCol[Int]
			val countDistinctByOtherMethod = flightDf.select($"DEST_COUNTRY_NAME").collectCol[String].distinct.length

			countDistinctBySelectExpr shouldEqual Seq(countDistinctExpected)
			countDistinctByOtherMethod shouldEqual countDistinctExpected
		}

		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
		 */
		it("selecting multiple columns at the same time"){


			// WARNING: cannot mix Column objects and strings
			val expectedMultiSelect: Seq[Seq[String]] = Seq(
				Seq(Animal.Cat.Lion, Animal.Cat.Lion, Animal.Cat.Lion, Climate.Tundra, Country.Africa),
				Seq(Animal.Cat.Lion, Animal.Cat.Lion, Animal.Cat.Lion, Climate.Desert, Country.Arabia),
				Seq(Animal.Hyena, Animal.Hyena, Animal.Hyena, Climate.Desert, Country.Africa),

				Seq(Animal.Zebra, Animal.Zebra, Animal.Zebra, Climate.Arid, Country.Africa),
				Seq(Animal.Giraffe, Animal.Giraffe, Animal.Giraffe, Climate.Tundra, Country.Africa),
			).map(seq => seq.map(enum => enum.toString))

			// NOTE: cannot combine string colname with object colname

			val actualMultiSelect: Seq[Row] = animalDf.select(animalDf.col("Animal"), col("Animal"), column("Animal"), $"Climate", expr("Country")).collect().toSeq

			// 1) Comparing row-wise
			actualMultiSelect shouldBe a [Seq[Row]]
			actualMultiSelect.map(row => row.toSeq.asInstanceOf[Seq[String]]) shouldBe a[Seq[Seq[String]]]
			actualMultiSelect.map(row => row.toSeq.asInstanceOf[Seq[String]]) should contain allElementsOf expectedMultiSelect


			// 2) Comparing col-wise
			val animalSeqUnzipped: Seq[Seq[String]] = animalDf.select(animalDf.col("Animal"), col("Climate"), column("Animal"), $"Climate", expr("Country"))
				.collect()
				.toSeq
				.map(_.toSeq.asInstanceOf[Seq[String]])
				.transpose

			//colsA(0) should contain atLeastOneElementOf(Animal.values.map(_.toString))
			Animal.values.map(_.toString) should contain allElementsOf animalSeqUnzipped(0)
			Climate.values.map(_.toString) should contain allElementsOf animalSeqUnzipped(1)
			Animal.values.map(_.toString) should contain allElementsOf animalSeqUnzipped(2)
			Climate.values.map(_.toString) should contain allElementsOf animalSeqUnzipped(3)
			Country.values.map(_.toString) should contain allElementsOf animalSeqUnzipped(4)
		}


		/**
		 * SOURCE: spark-test-repo
		 * 	- https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L160
		 */
		it("selecting all columns (using star)"){
			val listOfAllRows: Seq[Row] = animalDf.collect().toSeq

			animalDf.select("*").collect().toSeq should contain allElementsOf listOfAllRows
			animalDf.select($"*").collect().toSeq should contain allElementsOf listOfAllRows
			animalDf.select(col("*")).collect().toSeq should contain allElementsOf listOfAllRows
			animalDf.select(column("*")).collect().toSeq should contain allElementsOf listOfAllRows
			animalDf.select(animalDf.col("*")).collect().toSeq should contain allElementsOf listOfAllRows
			animalDf.select(animalDf("*")).collect().toSeq should contain allElementsOf listOfAllRows
			animalDf.select(expr("*")).collect().toSeq should contain allElementsOf listOfAllRows
		}


		// TODO select by renaming. Example:
		// empDF.select($"*", sumTest as "running_total").show
		// Source = https://hyp.is/LMOsMpwxEe6XKGPBSFlVcw/alvinhenrick.com/2017/05/16/apache-spark-analytical-window-functions/
	}
}

