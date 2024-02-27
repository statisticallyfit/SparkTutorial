package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns



import org.apache.spark.sql.{DataFrame, Row, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
import org.apache.spark.sql.types._

import utilities.GeneralMainUtils._
import utilities.EnumUtils.implicits._
import utilities.DFUtils
import DFUtils.implicits._
import DFUtils.TypeAbstractions._
import DFUtils.implicits._

//import com.SparkSessionForTests
import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._
/*import AnimalDf._
import TradeDf._*/
import com.data.util.EnumHub._


import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper
import com.SparkDocumentationByTesting.CustomMatchers

import scala.reflect.runtime.universe._

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

		val fromColnames: Seq[String] = List(Painter, Sculptor, Musician, Dancer, Singer, Writer, Architect, Actor).names
		val fromCols: Seq[Column] = fromColnames.map(col(_))

		val artListDf: DataFrame = artistDf
			.withColumn("ArtistGroup", array(fromCols: _*))
			.withColumn("ListOfArtSkills", array_remove(col("ArtistGroup"), "null"))
			.drop("ArtistGroup")
			.drop(fromColnames: _*) // removing the null-containing columns


		describe("using select()") {


			val numArtsDf: DataFrame = artListDf.select(col("*"),
				(sqlSize(col("ListOfArtSkills"))).as("NumberOfSkills"),
				(col("NumberOfSkills") * 2).as("Doubled"),
				(col("NumberOfSkills") * 3).as("Tripled"),
				(col("Tripled") * 3).as("MultipliedByNine"))

			numArtsDf.columns.length should equal(artListDf.columns.length + 4)
			numArtsDf.columns.sameElements(artListDf.columns ++ Seq("NumberOfSkills", "Doubled", "Tripled", "MultipliedByNine")) should be(true)
			// contents
			numArtsDf.select(col("MultipliedByNine")).collectCol[Int] shouldEqual numArtsDf.select(col("NumberOfSkills")).collectCol[Int].map(_ * 3 * 3)
		}

		// ---

		describe("using withColumn()"){

			it("add one column (which can contain a list)"){
				// Verifying the new column was added
				artListDf.columns should contain("ListOfArtSkills")
			}

			it("add multiple columns - using chained withColumn()") {

				// Adding multiple columns using chained withColumn()
				val numArtsDf: DataFrame = artListDf
					.withColumn("NumberOfSkills", sqlSize(col("ListOfArtSkills")))
					.withColumn("Doubled", col("NumberOfSkills") * 2)
					.withColumn("Tripled", col("NumberOfSkills") * 3)
					.withColumn("Quadrupled", col("NumberOfSkills") * 4)

				numArtsDf.columns.length == artListDf.columns.length + 4
			}


			it("add multiple columns - using Map() passed to withColumns()"){

				val numArtsDf: DataFrame = artListDf.withColumns(Map(
					"NumberOfSkills" -> sqlSize(col("ListOfArtSkills")),
					"Doubled" -> col("NumberOfSkills") * 2,
					"Tripled" -> col("NumberOfSkills") * 3,
					"Quadrupled" -> col("NumberOfSkills") * 4
				))

				numArtsDf.columns.length should equal ( artListDf.columns.length + 4 )
				numArtsDf.columns.sameElements(artListDf.columns ++ Seq("NumberOfSkills", "Doubled", "Tripled", "Quadrupled") ) should be (true)
				// contents
				numArtsDf.select(col("Tripled")).collectCol[Int] shouldEqual numArtsDf.select(col("NumberOfSkills")).collectCol[Int].map(_ * 3)
			}
		}

	}
}
