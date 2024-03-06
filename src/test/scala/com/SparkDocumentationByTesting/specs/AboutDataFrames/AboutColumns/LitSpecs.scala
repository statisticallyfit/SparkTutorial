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
 *
 */
class LitSpecs extends AnyFunSpec with Matchers  with SparkSessionWrapper {


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
			resultDf.columns should contain theSameElementsInOrderAs (tradeDf.columns :+ "1") // NOTE columname of lit(1) is "1"
			resultDf.select($"1").collectCol[Int].take(5) should equal (Seq(1,1,1,1,1))
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
	// TODO typedLit = https://hyp.is/i_SgSMuAEe64rGtTBZ5Mdg/sparkbyexamples.com/spark/spark-add-new-column-to-dataframe/
}
