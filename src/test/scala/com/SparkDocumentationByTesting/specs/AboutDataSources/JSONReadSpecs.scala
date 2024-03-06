package com.SparkDocumentationByTesting.specs.AboutDataSources



import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, Dataset, SparkSession, Column, ColumnName}
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
import org.apache.spark.SparkException
import com.SparkDocumentationByTesting.CustomMatchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import org.scalatest.Assertions._
import utilities.SparkSessionWrapper



/**
 *
 */
class JSONReadSpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {

	import sparkSessionWrapper.implicits._
	val sess: SparkSession = sparkSessionWrapper


	import utilities.DataHub.ImportedDataFrames._
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
