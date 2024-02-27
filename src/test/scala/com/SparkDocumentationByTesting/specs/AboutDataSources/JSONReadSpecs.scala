package com.SparkDocumentationByTesting.specs.AboutDataSources


import org.apache.spark.sql.{Column, ColumnName, DataFrame, DataFrameReader, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
import org.apache.spark.sql.types._
import utilities.GeneralMainUtils._
import com.data.util.EnumHub._
import utilities.EnumUtils.implicits._
import utilities.DFUtils
import DFUtils.TypeAbstractions._
import DFUtils.implicits._
import org.apache.spark.SparkException

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
 *
 */
class JSONReadSpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {

	import sparkSessionWrapper.implicits._
	val sess: SparkSession = sparkSessionWrapper


	import com.data.util.DataHub.ImportedDataFrames._
	import com.SparkDocumentationByTesting.state.DataSourcesState._


	// TODO fix this file to get a json file with - corrupt records, good records (check) to be able to run the tests

	describe("Reading JSON files ...") {


		describe("Mode = 'failfast'...") {

			describe("...  throws an error when encountering corrupted records") {

				it("false for inferSchema") {

					val airplaneDf: DataFrame = (sess.read.format(FORMAT_JSON)
						.option(key = "mode", value = "failfast")
						.option(key = "header", value = true)
						.option(key = "inferSchema", value = true)
						.load(s"$DATA_PATH/$folderBlogs/$folderInputData/airplanes.csv"))

					// NO ERROR!
					airplaneDf.select("engines").collect() shouldEqual Array(
						Row("2"), Row("two"), Row("2"), Row("two"), Row("two"), Row("two"), Row("two"), Row("2"), Row("2")
					)
				}

				it("true for manual schema") {

					val airplaneDf: DataFrame = (sess.read.format(FORMAT_JSON)
						.option(key = "mode", value = "failfast")
						.option(key = "header", value = true)
						.schema(manualAirplaneSchema)
						.load(s"$DATA_PATH/$folderBlogs/$folderInputData/airplanes.csv"))

					val se: SparkException = intercept[SparkException] {
						airplaneDf.show(4)
					}

					se shouldBe a[SparkException]
					se.getMessage should include("Malformed records are detected in record parsing")
				}
			}

			describe("... lets data load without error when there are no malformed records") {

				it("true for inferSchema") {
					val flightDf: DataFrame = (sess.read.format(FORMAT_JSON)
						.option(key = "mode", value = "failfast")
						.option(key = "header", value = true)
						.option(key = "inferSchema", value = true)
						.load(s"$DATA_PATH/$folderBillChambers/$folderInputData/flight-data/csv/2010-summary.csv"))

					flightDf shouldBe a[DataFrame]
				}
				it("true for manual schema") {
					val flightDf: DataFrame = (sess.read.format(FORMAT_JSON)
						.option(key = "mode", value = "failfast")
						.option(key = "header", value = true)
						.schema(manualFlightSchema)
						.load(s"$DATA_PATH/$folderBillChambers/$folderInputData/flight-data/csv/2010-summary.csv"))

					flightDf shouldBe a[DataFrame]
				}
			}
		}


		describe("Mode = 'dropMalformed' ...") {

			describe("...  drops the rows containing malformed records") {

				it("false for inferSchema") {
					val airplaneDf: DataFrame = (sess.read.format(FORMAT_JSON)
						.option(key = "mode", value = "dropMalformed")
						.option(key = "header", value = true)
						.option(key = "inferSchema", value = true)
						.load(s"$DATA_PATH/$folderBlogs/$folderInputData/airplanes.csv"))

					airplaneDf.select("engines").collect() shouldEqual Array(
						Row("2"), Row("two"), Row("2"), Row("two"), Row("two"), Row("two"), Row("two"), Row("2"), Row("2")
					)
				}

				it("true for manual schema") {
					val airplaneDf: DataFrame = (sess.read.format(FORMAT_JSON)
						.option(key = "mode", value = "dropMalformed")
						.option(key = "header", value = true)
						.schema(manualAirplaneSchema)
						.load(s"$DATA_PATH/$folderBlogs/$folderInputData/airplanes.csv"))

					airplaneDf.select("engines").collectCol[Int] shouldEqual Seq(2, 2, 2, 2)
				}
			}

			describe("... lets data load when there are no malformed records") {

				it("true for inferSchema") {
					val flightDf: DataFrame = (sess.read.format(FORMAT_JSON)
						.option(key = "mode", value = "failfast")
						.option(key = "header", value = true)
						.option(key = "inferSchema", value = true)
						.load(s"$DATA_PATH/$folderBillChambers/$folderInputData/flight-data/csv/2010-summary.csv"))

					flightDf shouldBe a[DataFrame]
				}
				it("true for manual schema") {
					val flightDf: DataFrame = (sess.read.format(FORMAT_JSON)
						.option(key = "mode", value = "failfast")
						.option(key = "header", value = true)
						.schema(manualFlightSchema)
						.load(s"$DATA_PATH/$folderBillChambers/$folderInputData/flight-data/csv/2010-summary.csv"))

					flightDf shouldBe a[DataFrame]
				}
			}
		}


		describe("Mode = 'permissive' ...") {

			describe("...places null in the rows containing malformed records") {

				it("false for inferSchema") {

					val airplaneDf: DataFrame = (sess.read.format(FORMAT_JSON)
						.option(key = "mode", value = "permissive")
						.option(key = "header", value = true)
						.option(key = "inferSchema", value = true)
						.load(s"$DATA_PATH/$folderBlogs/$folderInputData/airplanes.csv"))

					val two = "two"
					airplaneDf.select("engines").collectCol[String] shouldEqual Seq(2, two, 2, two, two, two, two, 2, 2).map(_.toString)

					airplaneDf.filter(col("engines").isNull).count() shouldEqual 0
				}

				it("true for manual schema") {
					val airplaneDf: DataFrame = (sess.read.format(FORMAT_JSON)
						.option(key = "mode", value = "permissive")
						.option(key = "header", value = true)
						.schema(manualAirplaneSchema)
						.load(s"$DATA_PATH/$folderBlogs/airplanes.csv"))

					airplaneDf.select("engines").collectCol[Int] shouldEqual Seq(2, 2, 2, 2)
				}
			}

			describe("... lets data load when there are no malformed records") {

				it("true for inferSchema") {
					val flightDf: DataFrame = (sess.read.format(FORMAT_JSON)
						.option(key = "mode", value = "permissive")
						.option(key = "header", value = true)
						.option(key = "inferSchema", value = true)
						.load(s"$DATA_PATH/$folderBillChambers/$folderInputData/flight-data/csv/2010-summary.csv"))

					flightDf shouldBe a[DataFrame]
				}
				it("true for manual schema") {
					val flightDf: DataFrame = (sess.read.format(FORMAT_JSON)
						.option(key = "mode", value = "permissive")
						.option(key = "header", value = true)
						.schema(manualFlightSchema)
						.load(s"$DATA_PATH/$folderBillChambers/$folderInputData/flight-data/csv/2010-summary.csv"))

					flightDf shouldBe a[DataFrame]
				}
			}
		}
	}
}
