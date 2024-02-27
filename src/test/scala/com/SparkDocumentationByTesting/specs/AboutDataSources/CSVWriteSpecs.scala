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
class CSVWriteSpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {

	import sparkSessionWrapper.implicits._
	val sess: SparkSession = sparkSessionWrapper


	import com.data.util.DataHub.ImportedDataFrames._
	import com.SparkDocumentationByTesting.state.DataSourcesState._

	describe("Writing a CSV file"){


		it("save mode = overwrite"){
			// creating a sample csv file so we can write to it
			val anyCsvFile: DataFrame = (sess.read.format(FORMAT_CSV)
				.option(key = "header", value = true)
				.option(key = "mode", value = "failfast")
				.schema(manualFlightSchema)
				.load(s"$DATA_PATH/$folderBillChambers/$folderInputData/flight-data/csv/2010-summary.csv"))

			(anyCsvFile.write.format(FORMAT_CSV)
				.mode(saveMode = "overwrite")
				.option(key = "sep", value = "\t") // separate by tab
				.save(path = s"$DATA_PATH/$folderBillChambers/$folderOutputData/flightDataAsTSV.tsv"))
		}
		it("save mode = append"){

		}
		it("save mode = errorIfExists"){

		}
		it("save mode = ignore"){

		}


		// TODO test these different save mode options
		// TODO test how number of files output corresponds to number of partitions in the dataframe
	}
}
