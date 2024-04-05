package com.SparkDocumentationByTesting.state



import org.apache.spark.sql.catalyst.expressions._ //genericrowwithschema...
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, Dataset, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize }
import org.apache.spark.sql.types._
//import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions._

import utilities.DFUtils; import DFUtils._ ; import DFUtils.TypeAbstractions._; import DFUtils.implicits._
import utilities.GeneralMainUtils._
import utilities.GeneralMainUtils.implicits._
import utilities.DataHub.ImportedDataFrames.fromBillChambersBook._
import utilities.DataHub.ManualDataFrames.fromEnums._
import utilities.DataHub.ManualDataFrames.fromSparkByExamples._
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
import Biome._; import Forest._; import Marine._; import Freshwater._; import Saltwater._; import Grassland._; import Tundra._
import Instrument._; import FinancialInstrument._ ; import Commodity._ ; import Transaction._

import World.Africa._
import World.Europe._
import World.NorthAmerica._
import World.SouthAmerica._
import World._
import World.Asia._
import World.Oceania._
import World.CentralAmerica._



/**
 *
 */
object ArraySpecState {

	/**
	 * State for functions like array_except, array_remove, etc which compare two array cols within the df.
	 */
	object SQLArrayComparisonTypeFunctionState {


		val animalArrayDf: DataFrame = (animalDf.groupBy(ClimateZone.enumName, Biome.enumName)
			.agg(collect_list(col(Animal.enumName)).as("ArrayAnimal"),
				collect_list(col("Amount")).as("ArrayAmount"),
				collect_list(col(World.enumName)).as("ArrayWorld"),
				//collect_list(col())
			))

		// -----------------------------------------------------------------------------------------------


		// Creating the udf that labels the location to its parent country
		/**
		 * Example usage:
		 * toParentCountry("Moscow") -> Russia
		 * toParentCountry("Milan") -> Europe
		 */
		val toParentCountry: EnumString => EnumString = location => Seq(World.withName(location)).returnMultiParent.head.enumName
		val udfToParentCountry: UserDefinedFunction = udf(toParentCountry(_: EnumString): EnumString)


		// Creating the df1 that is grouped by World (parent) and maps the parent world to array of animals, so in SouthAmerica, there are a list of animals.
		val udfdf1: DataFrame = animalDf.select(udfToParentCountry(col(World.enumName)).alias("ParentLocation"), col(Animal.enumName)) // udfdf

		val parentADf: DataFrame = (udfdf1 // grpdf
			.groupBy("ParentLocation")
			.agg(array_distinct(collect_list(col(Animal.enumName))).alias("ArrayAnimalP"))
			.filter(! col("ParentLocation").isInCollection(Seq("null"))))

		// Creating the df2 thati s grouped by climate zone, and has array of animals per climate zone
		val climateADf: DataFrame = (animalDf
			.groupBy(ClimateZone.enumName)
			.agg(array_distinct(collect_list(col(Animal.enumName))).as("ArrayAnimalC")))

		// Appending the dfs
		val climateParentAnimalsDf: DataFrame = climateADf.appendDf(parentADf)




		// --------------------------------------------------------------------------------------------

		/**
		 * 1) group by parent location in animaldf
		 * 2) group conditional on parent location, by climate zone (so climate within parent loc)
		 * 3) get animal array as conditional on 2) then place that here for the climate col, isntead of this random ordering climate
		 */
		val udfdf2 = (animalDf.select(udfToParentCountry(col(World.enumName)).alias("ParentLocation"), col(Animal.enumName), col(ClimateZone.enumName), col(Biome.enumName)))

		// Effort at trying to make climate conditional on parentlocation
		val parentCADf = (udfdf2.groupBy("ParentLocation")
			.agg(array_distinct(collect_list(col(ClimateZone.enumName))).as("ArrayClimate"),
				array_distinct(collect_list(col(Animal.enumName))).as("ArrayAnimalPC"))
			.filter(!col("ParentLocation").isInCollection(Seq("null"))))

	}

}
