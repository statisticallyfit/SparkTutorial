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

import Animal._ ; import SeaCreature._; import Whale._; import Bird._ ; import Eagle._ ;
import Rodent._; import Squirrel._ ; import WeaselMustelid._ ; import Camelid._
import Equine._; import Horse._; import Bear._ ; import Deer._; import Monkey._; import Ape._
import Insect._; import Reptile._; import Lizard._; import Amphibian._; import Frog._
import Cat._ ; import DomesticCat._ ; import WildCat._; import Canine._; import WildCanine._; import DomesticDog._; import Fox._
// TODO update with new animals made

import ClimateZone._
import Biome._; import Forest._; import Marine._; import Freshwater._; import Saltwater._; import Grassland._; import TundraBiome._
import Instrument._; import FinancialInstrument._ ; import Commodity._ ; import Transaction._


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

			tradeDf.drop(Instrument.FinancialInstrument.enumName).columns shouldEqual tradeDf.columns.toSeq.remove(FinancialInstrument.enumName)

			tradeDf.drop(col(World.enumName)).columns shouldEqual tradeDf.columns.toSeq.remove(World.enumName)
		}

		it("can drop multiple columns"){

			val dropStrCols: Seq[NameOfCol] = List(Human, Craft, Genre, Writer, Singer, Actor, Botanist, Geologist, Doctor).enumNames
			craftDf.drop(dropStrCols:_*).columns shouldEqual (ArtistDf.colnamesCraft diff dropStrCols)

			val dropCols: Seq[Column] = dropStrCols.map(col(_))
			craftDf.drop(dropCols.head, dropCols.tail:_*).columns shouldEqual (ArtistDf.colnamesCraft diff dropStrCols)
		}

	}
}
