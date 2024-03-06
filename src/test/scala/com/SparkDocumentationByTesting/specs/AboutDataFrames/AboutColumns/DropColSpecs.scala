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
class DropColSpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {


	import sparkSessionWrapper.implicits._

	describe("Dropping columns ..."){


		it("can drop one column"){

			val tradeColnames: Seq[NameOfCol] = (Company, Instrument.FinancialInstrument, "Amount", Transaction, World).tupleToStringList

			tradeDf.columns shouldEqual tradeColnames

			tradeDf.drop(Instrument.FinancialInstrument.enumName).columns shouldEqual (Company, "Amount", Transaction, World).tupleToStringList
			tradeDf.drop(col(World.enumName)).columns shouldEqual (Company, Instrument.FinancialInstrument, "Amount", Transaction).tupleToStringList
		}

		it("can drop multiple columns"){

			val dropStrCols: Seq[NameOfCol] = List(Human, Craft, Genre, Writer, Singer, Actor, Botanist, Geologist, Doctor).enumNames
			craftDf.drop(dropStrCols:_*).columns shouldEqual (ArtistDf.colnamesCraft diff dropStrCols)

			val dropCols: Seq[Column] = dropStrCols.map(col(_))
			craftDf.drop(dropCols.head, dropCols.tail:_*).columns shouldEqual (ArtistDf.colnamesCraft diff dropStrCols)
		}

	}
}
