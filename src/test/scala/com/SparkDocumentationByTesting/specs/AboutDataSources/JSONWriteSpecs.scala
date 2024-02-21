package com.SparkDocumentationByTesting.specs.AboutDataSources

import org.apache.spark.sql.{Column, ColumnName, DataFrame, DataFrameReader, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
import org.apache.spark.sql.types._
import utilities.GeneralUtils._
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


	// TODO update json write specs

	describe("Writing JSON file..."){

		//csvFile.write.format(FORMAT_JSON).mode("overwrite").save(PATHHERE)
	}
}
