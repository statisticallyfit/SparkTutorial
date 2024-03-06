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
class JSONWriteSpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {

	import sparkSessionWrapper.implicits._

	val sess: SparkSession = sparkSessionWrapper


	import utilities.DataHub.ImportedDataFrames._
	import com.SparkDocumentationByTesting.state.DataSourcesState._


	// TODO update json write specs

	describe("Writing JSON file..."){

		//csvFile.write.format(FORMAT_JSON).mode("overwrite").save(PATHHERE)
	}
}
