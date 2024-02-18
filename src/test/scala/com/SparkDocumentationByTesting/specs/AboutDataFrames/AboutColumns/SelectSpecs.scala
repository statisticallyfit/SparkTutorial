package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns


import org.apache.spark.sql.{DataFrame, Row, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._


import utilities.GeneralUtils._
import com.data.util.EnumHub._
import utilities.EnumUtils.implicits._
import utilities.DFUtils
import DFUtils.TypeAbstractions._
import DFUtils.implicits._

//import com.SparkSessionForTests
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper // intercept
import com.SparkDocumentationByTesting.CustomMatchers

import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._
import TradeDf._
import AnimalDf._
import ArtistDf._
import Artist._


import World.Africa._
import World.Europe._
import World.NorthAmerica._
import World.SouthAmerica._
import World._
import World.Asia._
import World.Oceania._
import World.CentralAmerica._



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



	// TODO test select produced by binary operator = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L154

	// TODO select after doing operations on a column = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L199-L260

	describe("Selecting..."){

		describe("Selecting column names"){

			// SOURCE: BillChambers_Chp5
			it("selecting by string column name") {

				animalDf.select("Animal").collect().toSeq should contain allOf(
					Row(Animal.Cat.WildCat.Lion.name),
					Row(Animal.SeaCreature.Dolphin.name),
					Row(Animal.Elephant.name),
					Row(Animal.Bird.Eagle.GoldenEagle.name)
				)
			}

			// SOURCE: BillChambers_Chp5
			it("selecting by symbol ($) column name") {

				//println(flightDf.select($"DEST_COUNTRY_NAME").collect().toSeq)
				animalDf.select($"Climate").collectEnumCol[Climate] should contain allElementsOf coupleOfClimates
			}

			// SOURCE: BillChambers_Chp5
			it("selecting by col() functions") {
				animalDf.select(animalDf.col("World")).collectEnumCol[World].distinct should contain allElementsOf coupleOfCountries

				animalDf.select(col("Animal")).collectEnumCol[Animal].distinct should contain allElementsOf coupleOfAnimals

				animalDf.select(column("Climate")).collectEnumCol[Climate] should contain allElementsOf coupleOfClimates

				val apostropheWay: Seq[Climate] = animalDf.select('Climate).collectEnumCol[Climate]
				val symbolWay: Seq[Climate] = animalDf.select(Symbol("Climate")).collectEnumCol[Climate]
				apostropheWay should equal(symbolWay)
				apostropheWay should contain allElementsOf coupleOfClimates
			}

			it("selecting by df() functionitself") {
				animalDf.select(animalDf("World")).collectEnumCol[World].distinct should contain allElementsOf (coupleOfCountries)
			}

			/**
			 * SOURCE:
			 * 	- BillChambers_Chp5
			 */
			it("selecting by expr() and selectExpr()") {

				animalDf.select(expr("World")).collectEnumCol[World] should contain allElementsOf coupleOfCountries

				val resultDf: DataFrame = animalDf.selectExpr("World as NewCountryName", "Animal as TheZoo")
				resultDf.columns.length shouldEqual 2
				resultDf.columns shouldEqual Seq("NewCountryName", "TheZoo")
				resultDf.select(expr("NewCountryName")).collectEnumCol[World] should contain allElementsOf coupleOfCountries
				resultDf.select(expr("TheZoo")).collectEnumCol[Animal] should contain allElementsOf coupleOfAnimals
			}

			/**
			 * SOURCE:
			 * 	- BillChambers_Chp5
			 */
			// TODO find more examples of selectExpr - categorize the actions that can be done
			it("selectExpr() allows doing more operations besides selecting a column") {
				val resultDf: DataFrame = flightDf.selectExpr(
					"*", //select all original columns
					"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as isWithinCountry" // specifying if destination and origin are the same
				)

				resultDf.columns.length should be(flightDf.columns.length + 1)
				resultDf.select($"isWithinCountry").collectCol[Boolean].take(10) should contain allElementsOf (Seq(false, false, false, false, false, false, false, false, false, false))
			}

			/**
			 * SOURCE:
			 * 	- BillChambers_Chp5
			 */
			it("selectExpr(): doing aggregations") {
				val resultDf = flightDf.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))")

				resultDf.count() should be(1) // since each calculation yields a single number
				resultDf.columns.length should be(2)
				resultDf.columns should equal(Array("avg(count)", "count(DISTINCT DEST_COUNTRY_NAME)"))
				//contain allElementsOf (Seq("avg(count)", "count(distinct(DEST_COUNTRY_NAME))"))

				// ---------
				val avgExpected: Double = 1770.765625
				val avgBySelectExpr: Double = flightDf.selectExpr("avg(count)").collectCol[Double].head
				val avgBySelect: Double = flightDf.select(avg("count")).collectCol[Double].head
				val avgByGrouping: Double = flightDf.groupBy().avg("count").collectCol[Double].head // source = https://stackoverflow.com/a/44384396
				val avgByWindowingDummyCol: Double = flightDf
					.withColumn("dummyCol", lit(null))
					.withColumn("mean", avg("count").over(Window.partitionBy("dummyCol")))
					.select($"mean")
					.collectCol[Double]
					.head
				val avgByWindowingEmpty: Double = flightDf
					.withColumn("mean", avg("count").over(Window.partitionBy()))
					.select($"mean")
					.collectCol[Double]
					.head
				// source = https://stackoverflow.com/q/44382822

				// TIP: this is wrong because cannot pass avg() inside withColumn  -requires a grouping operation first (window or groupBy -- error is thrown)
				// flightDf.withColumn("avgCount", avg("count")).select("avgCount").collectCol[Double]

				Seq(avgBySelect, avgBySelectExpr, avgByGrouping, avgByWindowingEmpty, avgByWindowingDummyCol).forall(_ == avgExpected) shouldBe true

				// ---------
				val countDistinctExpected: Long = 132
				// NOTE: count returns long and would get ClassCastException is using any other type (like Int)
				val countDistinctBySelectExpr: Long = flightDf.selectExpr("count(distinct(DEST_COUNTRY_NAME))").collectCol[Long].head
				val countDistinctByOtherMethod: Long = flightDf.select($"DEST_COUNTRY_NAME").collectCol[String].distinct.length.toLong

				countDistinctBySelectExpr shouldEqual countDistinctExpected
				countDistinctByOtherMethod shouldEqual countDistinctExpected
			}

			// SOURCE: BillChambers_Chp5
			it("selecting multiple columns at the same time, using all possible methods") {


				// WARNING: cannot mix Column objects and strings
				val expectedMultiSelect: Seq[Seq[String]] = Seq(
					Seq(Animal.Cat.WildCat.Lion, Animal.Cat.WildCat.Lion, Animal.Cat.WildCat.Lion, Climate.Tundra, Africa),
					Seq(Animal.Cat.WildCat.Lion, Animal.Cat.WildCat.Lion, Animal.Cat.WildCat.Lion, Climate.Desert, Arabia),
					Seq(Animal.Hyena, Animal.Hyena, Animal.Hyena, Climate.Desert, Africa),

					Seq(Animal.Zebra, Animal.Zebra, Animal.Zebra, Climate.Arid, Africa),
					Seq(Animal.Giraffe, Animal.Giraffe, Animal.Giraffe, Climate.Tundra, Africa),
				).map(seq => seq.map(enum => enum.toString))

				// NOTE: cannot combine string colname with object colname

				val actualMultiSelect: Seq[Row] = animalDf.select(animalDf.col("Animal"), col("Animal"), column("Animal"), $"Climate", expr("World")).collect().toSeq

				// 1) Comparing row-wise
				actualMultiSelect shouldBe a[Seq[Row]]
				actualMultiSelect.map(row => row.toSeq.asInstanceOf[Seq[String]]) shouldBe a[Seq[Seq[String]]]
				actualMultiSelect.map(row => row.toSeq.asInstanceOf[Seq[String]]) should contain allElementsOf expectedMultiSelect


				// 2) Comparing col-wise
				val animalSeqUnzipped: Seq[Seq[String]] = animalDf.select(animalDf.col("Animal"), col("Climate"), column("Animal"), $"Climate", expr("World"))
					.collectAll
					.map(_.toSeq.asInstanceOf[Seq[String]])
					.transpose

				//colsA(0) should contain atLeastOneElementOf(Animal.values.map(_.toString))
				Animal.values.map(_.toString) should contain allElementsOf animalSeqUnzipped(0)
				Climate.values.map(_.toString) should contain allElementsOf animalSeqUnzipped(1)
				Animal.values.map(_.toString) should contain allElementsOf animalSeqUnzipped(2)
				Climate.values.map(_.toString) should contain allElementsOf animalSeqUnzipped(3)
				World.values.map(_.toString) should contain allElementsOf animalSeqUnzipped(4)
			}


			// SOURCE: https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L160
			it("selecting all columns (using star)") {
				val listOfAllRows: Seq[Row] = animalDf.collect().toSeq

				animalDf.select("*").collectAll should contain allElementsOf listOfAllRows
				animalDf.select($"*").collectAll should contain allElementsOf listOfAllRows
				animalDf.select(col("*")).collectAll should contain allElementsOf listOfAllRows
				animalDf.select(column("*")).collectAll should contain allElementsOf listOfAllRows
				animalDf.select(animalDf.col("*")).collectAll should contain allElementsOf listOfAllRows
				animalDf.select(animalDf("*")).collectAll should contain allElementsOf listOfAllRows
				animalDf.select(expr("*")).collectAll should contain allElementsOf listOfAllRows
			}


			// SOURCE: https://sparkbyexamples.com/spark/spark-select-columns-from-dataframe/
			it("selecting using slice()"){
				val colSlice: Seq[NameOfCol] = artistDf.columns.slice(2, 5)

				colSlice shouldEqual (Art.Literature.Genre, ArtPeriod, "TitleOfWork").tupleToNameList

				artistDf.select(colSlice.map(col(_)):_*).columns shouldEqual colSlice
			}
			// SOURCE: https://sparkbyexamples.com/spark/spark-select-columns-from-dataframe/
			it("selecting using indexing"){
				val colsChosenByIndex: Seq[NameOfCol] = Seq(artistDf.columns(1), artistDf.columns(5), artistDf.columns(7))

				colsChosenByIndex shouldEqual Seq(Art.name, "YearPublished", "PlaceOfDeath")

				artistDf.select(colsChosenByIndex.map(col(_)):_*).columns shouldEqual colsChosenByIndex
			}
			// TODO : select col by regex - hate this method
			// SOURCE: https://hyp.is/ajDLas5rEe6-qcsIUVfwzg/sparkbyexamples.com/spark/spark-select-columns-from-dataframe/

			// SOURCE: https://hyp.is/j7Bo-s5rEe62qRueMRb07w/sparkbyexamples.com/spark/spark-select-columns-from-dataframe/
			it("selecting using startsWith or endsWith on column name"){

				val startLetterCols: Array[Column] = artistDf.columns.filter(c => c.startsWith("A")).map(col(_))
				val endLetterCols: Array[Column] = artistDf.columns.filter(c => c.endsWith("r")).map(col(_))

				artistDf.select(startLetterCols:_*).columns shouldEqual Seq(Art.name, ArtPeriod.name, Architect.name, Actor.name)
				artistDf.select(endLetterCols:_*).columns shouldEqual Seq(Painter, Sculptor, Dancer, Singer, Writer, Actor).namesAll

			}
		}


		// SOURCE: https://hyp.is/QRlzcM5xEe6tPkPdFPQQ3g/sparkbyexamples.com/spark/spark-select-columns-from-dataframe/
		describe("Selecting nested Struct columns"){

			import com.data.util.DataHub.ManualDataFrames.fromSparkByExamples._

			it("can select the individual columns underneath the nested Struct"){

				dfNested.select("name.lastname", "name.firstname").collectAll shouldEqual Seq(
					Row("Smith", "James "),
					Row("", "Michael "),
					Row("Williams", "Robert "),
					Row("Jones", "Maria "),
					Row("Brown", "Jen")
				)
			}

			describe("can select ALL the individual columns underneath the nested Struct") {

				val checkRowsUnderNestedCol: Seq[Row] = Seq(
					Row("James ", "", "Smith"),
					Row("Michael ", "Rose", ""),
					Row("Robert ", "", "Williams"),
					Row("Maria ", "Anne", "Jones"),
					Row("Jen", "Mary", "Brown")
				)

				// WARNING very tricky - collectCol[Row] vs. collectAll gives error depending on if using 'name' or 'name.*'
				it("using simple column name"){
					dfNested.select("name").collectCol[Row] shouldEqual checkRowsUnderNestedCol
				}
				it("using the columnname with star"){
					dfNested.select("name.*").collectAll shouldEqual checkRowsUnderNestedCol
				}
			}
		}


		// TODO select by renaming. Example:
		// empDF.select($"*", sumTest as "running_total").show
		// Source = https://hyp.is/LMOsMpwxEe6XKGPBSFlVcw/alvinhenrick.com/2017/05/16/apache-spark-analytical-window-functions/



		/**
		 * SOURCE: spark-test-repo
		 */
		describe("Selecting column operations giving rise to new columns") {

			import com.data.util.DataHub.ManualDataFrames.XYRandDf._


			it("unary op on a column") {

				// For ints
				df.select(-$"x").collectCol[Int] shouldEqual df.collectAll.map(row => -row.getInt(0))

				// For bools
				val (t, f) = (true, false)
				val dfbool = Seq(t, t, t, f, f, t, f, t, f, t, t, t, t, f, t, f, t, t, t, t).toDF("booleans")
				dfbool.select(!$"booleans").collectCol[Boolean] shouldEqual dfbool.collectAll.map(row => !row.getBoolean(0))
			}

			// SOURCE: spark test repo:- https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L154
			it("binary op between two existing columns") {

				val zAdd: Seq[Int] = df.select(df("x") + df("y").as("z2")).collectCol[Int]
				val zCheck: Seq[Int] = df.select(df("z")).collectCol[Int]

				zAdd shouldEqual zCheck

				df.select($"x" + $"y" + 3).collectCol[Int] shouldEqual df.collectAll.map(row => row.getInt(0) + row.getInt(1) + 3)
				df.select($"x" - $"y" - 3).collectCol[Int] shouldEqual df.collectAll.map(row => row.getInt(0) - row.getInt(1) - 3)
				df.select($"x" * $"y" * 3).collectCol[Int] shouldEqual df.collectAll.map(row => row.getInt(0) * row.getInt(1) * 3)
				// TODO study better to figure out why left gives null at i = 6 while right gives Infinity and i = 6
				//df.select($"x" / $"y" + 1).collectCol[Double] shouldEqual df.collectAll.map(row => row.getInt(0).toDouble / row.getInt(1).toDouble + 1)
				// TODO figure out why right gives error divbyzero when left is fine
				//df.select($"x" % $"y" + 2).collectCol[Int] shouldEqual df.collectAll.map(row => row.getInt(0) % row.getInt(1) + 2)
			}

			// SOURCE: spark-test-repo: https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L199-L260
			it("binary op between existing column and another operand") {

				df.select($"x" + 1).collectCol[Int] shouldEqual df.collectAll.map(row => row.getInt(0) + 1)
				df.select($"x" - 1).collectCol[Int] shouldEqual df.collectAll.map(row => row.getInt(0) - 1)
				df.select($"x" * 2).collectCol[Int] shouldEqual df.collectAll.map(row => row.getInt(0) * 2)
				df.select($"x" / 5).collectCol[Double] shouldEqual df.collectAll.map(row => row.getInt(0).toDouble / 5)
				df.select($"x" % 5).collectCol[Int] shouldEqual df.collectAll.map(row => row.getInt(0) % 5)

			}

			// TODO bitwiseAnd,Or etc = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L948-L976
		}

	}

}

