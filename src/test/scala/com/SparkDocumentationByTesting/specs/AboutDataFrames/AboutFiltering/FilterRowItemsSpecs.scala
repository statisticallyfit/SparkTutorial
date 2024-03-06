package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutFiltering


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
