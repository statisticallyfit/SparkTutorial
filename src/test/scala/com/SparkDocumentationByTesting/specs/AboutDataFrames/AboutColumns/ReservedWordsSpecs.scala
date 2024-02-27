package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns




import org.apache.spark.sql.{DataFrame, Row, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
import org.apache.spark.sql.types._

import utilities.GeneralMainUtils._
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


/**
 * SOURCE:
 * 	- chp 5 Bill Chambers
 */
class ReservedWordsSpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {


	import sparkSessionWrapper.implicits._


	describe("Reserved characters - long names can include spaces or dashes and must be dealt with differently when they are treated as expressions versus just strings"){

		describe("When not putting long colname in an expression...."){

			it("... then we don't need backticks because the colname (seprated by space/dash) is just a string, not an expression") {

				val dfWithLongColumnName: DataFrame = artistDf.withColumn("This Long Column Name - for a Human", col(Human.name))

				val dfSelectNoTicks: DataFrame = dfWithLongColumnName.select(col("This Long Column Name - for a Human").as("NewHuman"))
				val dfWithColNoTicks: DataFrame = dfWithLongColumnName.withColumn("New Human", col("This Long Column Name - for a Human"))

				dfSelectNoTicks.columns shouldEqual Seq("NewHuman")
				dfWithColNoTicks.columns.last shouldEqual "New Human"
			}
		}


		describe("When putting long colname in an expression ... "){

			val dfWithLongColumnName: DataFrame = artistDf.withColumn("This Long Column Name - for a Human", col(Human.name))

			it("... names separated by spaces or dashes must be referenced using backticks"){

				val dfExprYesTicks: DataFrame = dfWithLongColumnName
					.select(expr("`This Long Column Name - for a Human`"),
						expr("`This Long Column Name - for a Human`").alias("New Human Column"))

				val dfSelectExprYesTicks: DataFrame = dfWithLongColumnName
					.selectExpr("`This Long Column Name - for a Human`",
						"`This Long Column Name - for a Human` as `New Human Column`")

				dfExprYesTicks.columns shouldEqual Seq("This Long Column Name - for a Human", "New Human Column")
				dfSelectExprYesTicks.columns shouldEqual Seq("This Long Column Name - for a Human", "New Human Column")
			}


			it("... otherwise, names separated by space/dash not referenced using backticks result in parsing error"){
				import org.apache.spark.sql.catalyst.parser.ParseException

				val pe: ParseException = intercept[ParseException]{
					dfWithLongColumnName.selectExpr("This Long Column Name - for a Human")
				}
				pe shouldBe a [ParseException]
			}
		}
	}

}
