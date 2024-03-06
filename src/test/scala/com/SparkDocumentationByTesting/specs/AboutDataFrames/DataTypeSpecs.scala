package com.SparkDocumentationByTesting.specs.AboutDataFrames


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
