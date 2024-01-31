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
class LiteralSpecs extends AnyFunSpec with Matchers  with SparkSessionWrapper {


	import com.SparkDocumentationByTesting.state.SpecState._
	import TradeState._
	import AnimalState._
	import sparkSessionWrapper.implicits._

	describe("Literal"){

		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
		 */
		it("is an explicit value, not an entire column, like a constant value"){
			val resultDf: DataFrame = tradeDf.select(expr("*"), lit(1))

			resultDf.columns.length should be(tradeDf.columns.length + 1)
			resultDf.columns should contain theSameElementsInOrderAs (tradeDf.columns :+ "lit(1)")
			resultDf.select($"lit(1)").collectCol[Int].take(5) should equal (Seq(1,1,1,1,1))
		}

		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
 		 */
		it("is an expression and can be treated as such (can call alias(), as() on them)"){

			val resultDf_alias: DataFrame = tradeDf.select(expr("*"), lit(null).alias("NullType"))

			resultDf_alias.columns.length should be(tradeDf.columns.length + 1)
			resultDf_alias.columns should contain theSameElementsInOrderAs (tradeDf.columns :+ "NullType")
			resultDf_alias.select($"NullType").collectCol[Int].take(5) should equal(Seq(null, null, null, null, null))

			// ------------
			val resultDf_as: DataFrame = tradeDf.select(expr("*"), lit(5).alias("Five"))

			resultDf_as.columns.length should be(tradeDf.columns.length + 1)
			resultDf_as.columns should contain theSameElementsInOrderAs (tradeDf.columns :+ "Five")
			resultDf_as.select($"Five").collectCol[Int].take(3) should equal(Seq(5, 5, 5))
		}
	}
}
