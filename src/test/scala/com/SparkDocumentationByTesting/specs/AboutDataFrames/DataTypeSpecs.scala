package com.SparkDocumentationByTesting.specs.AboutDataFrames


import org.apache.spark.sql.{Column, ColumnName, DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
import org.apache.spark.sql.types._
import utilities.GeneralMainUtils._
import com.data.util.EnumHub._
import utilities.EnumUtils.implicits._
import utilities.DFUtils
import DFUtils.TypeAbstractions._
import DFUtils.implicits._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.scalatest.Assertion

//import com.SparkSessionForTests
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper // intercept
import com.SparkDocumentationByTesting.CustomMatchers

import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._
import com.data.util.DataHub.ManualDataFrames.fromSparkByExamples._
import TradeDf._
import AnimalDf._
import ArtistDf._
import Artist._


/**
 *
 */
class DataTypeSpecs extends AnyFunSpec with Matchers  with SparkSessionWrapper {


	import com.SparkDocumentationByTesting.state.SpecState._
	import sparkSessionWrapper.implicits._


	describe ("DataTypes ..."){


		/**
		 * TODO datatypes
		 * 	- sparkbyexamples = https://sparkbyexamples.com/spark/spark-sql-dataframe-data-types/
		 * 	- spark repo = https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/types/DataTypeSuite.scala
		 */
	}

}
