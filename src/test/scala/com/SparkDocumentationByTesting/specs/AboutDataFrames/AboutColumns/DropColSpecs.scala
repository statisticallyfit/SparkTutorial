package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns




import org.apache.spark.sql.{DataFrame, Row, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
import org.apache.spark.sql.types._

import utilities.GeneralUtils._
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
class DropColSpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {


	import sparkSessionWrapper.implicits._

	describe("Dropping columns ..."){


		it("can drop one column"){

			val tradeColnames: Seq[NameOfCol] = (Company, Instrument.FinancialInstrument, "Amount", Transaction, World).tupleToNameList

			tradeDf.columns shouldEqual tradeColnames

			tradeDf.drop(Instrument.FinancialInstrument.name).columns shouldEqual (Company, "Amount", Transaction, World).tupleToNameList
			tradeDf.drop(col(World.name)).columns shouldEqual (Company, Instrument.FinancialInstrument, "Amount", Transaction).tupleToNameList
		}

		it("can drop multiple columns"){

			import Art.Literature.Genre

			val artistColnames: Seq[NameOfCol] = (Human, Art, Genre, ArtPeriod, "TitleOfWork", "YearPublished", "PlaceOfBirth", "PlaceOfDeath", Painter, Sculptor, Musician, Dancer, Singer, Writer, Architect, Actor).tupleToNameList

			val dropStrCols: Seq[NameOfCol] = List(Human.name, Art.name, Genre.name, Writer.name, Singer.name, Actor.name)
			artistDf.drop(dropStrCols:_*).columns shouldEqual (artistColnames diff dropStrCols)

			val dropCols: Seq[Column] = dropStrCols.map(col(_))
			artistDf.drop(dropCols.head, dropCols.tail:_*).columns shouldEqual (artistColnames diff dropStrCols)
		}

	}
}
