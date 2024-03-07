package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns



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


//import com.SparkSessionForTests
import com.SparkDocumentationByTesting.CustomMatchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import org.scalatest.Assertions._
import utilities.SparkSessionWrapper

/**
 * SOURCE:
 * 	- chp 5 Bill Chambers
 * 	- spark by examples:
 * 		- website = https://sparkbyexamples.com/spark/spark-dataframe-withcolumn/
 * 		- code = https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/functions/AddColumn.scala
 */
class AddColSpecs extends AnyFunSpec with Matchers with SparkSessionWrapper with CustomMatchers {


	import ArtistDf._
	import Artist._


	describe("Adding columns ..."){

		val fromColnames: Seq[NameOfCol] = List(Mathematician, Engineer, Architect, Botanist, Chemist, Geologist, Doctor, Physicist,Painter, Sculptor, Musician, Dancer, Singer, Actor, Designer, Inventor, Producer, Director, Writer, Linguist).enumNames
		val fromCols: Seq[Column] = fromColnames.map(col(_))

		val GATHER_NAME: NameOfCol = "Group"
		val SKILL_COL_NAME: NameOfCol = "ListOfSkills"

		val artListDf: DataFrame = (craftDf
			.withColumn(GATHER_NAME, array(fromCols: _*))
			.withColumn(SKILL_COL_NAME, array_remove(col(GATHER_NAME), "null"))
			.drop(GATHER_NAME)
			.drop(fromColnames: _*)) // removing the null-containing columns

		// simple sanity check
		artListDf should equalDataFrame(DFUtils.gatherNonNullsToListCol(craftDf, colsToGetFrom = fromColnames, lstColname = SKILL_COL_NAME))



		describe("using withColumn()") {

			it("add one column (which can contain a list)") {
				// Verifying the new column was added
				artListDf.columns should contain(SKILL_COL_NAME)
			}

			it("add multiple columns - using chained withColumn()") {

				// Adding multiple columns using chained withColumn()
				val numArtsDf: DataFrame = artListDf
					.withColumn("NumberOfSkills", sqlSize(col(SKILL_COL_NAME)))
					.withColumn("Doubled", col("NumberOfSkills") * 2)
					.withColumn("Tripled", col("NumberOfSkills") * 3)
					.withColumn("Quadrupled", col("NumberOfSkills") * 4)

				numArtsDf.columns.length == artListDf.columns.length + 4
			}


			it("add multiple columns - using Map() passed to withColumns()") {

				val numArtsDf: DataFrame = artListDf.withColumns(Map(
					"NumberOfSkills" -> sqlSize(col(SKILL_COL_NAME)),
					"Doubled" -> col("NumberOfSkills") * 2,
					"Tripled" -> col("NumberOfSkills") * 3,
					"Quadrupled" -> col("NumberOfSkills") * 4
				))

				numArtsDf.columns.length should equal(artListDf.columns.length + 4)
				numArtsDf.columns.sameElements(artListDf.columns ++ Seq("NumberOfSkills", "Doubled", "Tripled", "Quadrupled")) should be(true)
				// contents
				numArtsDf.select(col("Tripled")).collectCol[Int] shouldEqual numArtsDf.select(col("NumberOfSkills")).collectCol[Int].map(_ * 3)
			}
		}

		// --------

		describe("using select()") {

			val numArtsDf: DataFrame = artListDf.select(col("*"),
				(sqlSize(col(SKILL_COL_NAME))).as("NumberOfSkills"),
				(col("NumberOfSkills") * 2).as("Doubled"),
				(col("NumberOfSkills") * 3).as("Tripled"),
				(col("Tripled") * 3).as("MultipliedByNine"))

			numArtsDf.columns.length should equal(artListDf.columns.length + 4)
			numArtsDf.columns.sameElements(artListDf.columns ++ Seq("NumberOfSkills", "Doubled", "Tripled", "MultipliedByNine")) should be(true)
			// contents
			numArtsDf.select(col("MultipliedByNine")).collectCol[Int] shouldEqual numArtsDf.select(col("NumberOfSkills")).collectCol[Int].map(_ * 3 * 3)
		}

	}
}
