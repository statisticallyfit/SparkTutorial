package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._

import utilities.DFUtils
import utilities.DFUtils.implicits._

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
 *
 */
class AboutLiterals extends AnyFunSpec with Matchers  with SparkSessionWrapper {


	import com.SparkDocumentationByTesting.state.ColumnTestsState._
	import TradeState._
	import AnimalState._
	import sparkSessionWrapper.implicits._

	describe("Literal"){

		it("is an explicit value, not an entire column, like a constant value"){
			tradeDf.
		}
		it("is an expression and can be treated as one"){

		}
	}
}
