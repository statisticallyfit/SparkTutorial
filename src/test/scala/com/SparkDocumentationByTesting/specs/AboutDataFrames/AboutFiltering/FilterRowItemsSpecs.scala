package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutFiltering


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
 *
 */
class FilterRowItemsSpecs   extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper  {

	import sparkSessionWrapper.implicits._

	// TODO categorize the different methods of filtering row elements here (removing nulls within row elements)

	val df = Seq(("a", "b", "null", "d"), ("null", "f", "m", "null"), ("null", "null", "d", "null")).toDF("word_0", "word_1", "word_2", "word_3")

	// array_except way
	val res0_0 = df.select(array("*").as("all")).withColumn("collected", array_except(col("all"), array(lit("null"))))

	// array_remove way
	val res0 = df.select(array("*").as("all")).withColumn("collected", array_remove(col("all"), "null"))

	// UDF way
	def arrayNullFilter = udf((arr: Seq[String]) => arr.filter(x => x != "null"))
	val res2 = df.select(array("*").as("all")).withColumn("test", arrayNullFilter(col("all")))

	// map way
	val rowlen = df.columns.length
	val res3 = df.map(row =>
		(0 until rowlen).map(i => row.getString(i)).filter(_ != "null")
	)
}
