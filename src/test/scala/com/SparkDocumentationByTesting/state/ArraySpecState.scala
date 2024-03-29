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


		// Creating the udf that labels the location to its parent country
		/**
		 * Example usage:
		 * toParentCountry("Moscow") -> Russia
		 * toParentCountry("Milan") -> Europe
		 */
		val toParentCountry: EnumString => EnumString = location => Seq(World.withName(location)).returnMultiParent.head.enumName
		val udfToParentCountry: UserDefinedFunction = udf(toParentCountry(_: EnumString): EnumString)


		// Creating the df1 that is grouped by World (parent) and maps the parent world to array of animals, so in SouthAmerica, there are a list of animals.
		val parentLocationToAnimalDf: DataFrame = animalDf.select(udfToParentCountry(col(World.enumName)).alias("ParentLocation"), col(Animal.enumName)) // udfdf

		val parentLocationGroupAnimalDf: DataFrame = (parentLocationToAnimalDf // grpdf
			.groupBy("ParentLocation")
			.agg(collect_list(col(Animal.enumName)).alias("ArrayAnimalWorld"))
			.filter(! col("ParentLocation").isInCollection(Seq("null"))))

		// Creating the df2 thati s grouped by climate zone, and has array of animals per climate zone
		val climateGroupAnimalDf: DataFrame = animalDf.groupBy(ClimateZone.enumName).agg(collect_list(col(Animal.enumName)).as("ArrayAnimalClimate"))

		// Appending the dfs
		val res = climateGroupAnimalDf.appendDf(parentLocationGroupAnimalDf)

		// TODO major fix
		/**
		 * 1) group by parent location in animaldf
		 * 2) group conditional on parent location, by climate zone (so climate within parent loc)
		 * 3) get animal array as conditional on 2) then place that here for the climate col, isntead of this random ordering climate
		 */
		val udfdf = (animalDf.select(udfToParentCountry(col(World.enumName)).alias("ParentLocation"), col(Animal.enumName), col(ClimateZone.enumName), col(Biome.enumName)))
		val grpdf = (udfdf.groupBy("ParentLocation")
			.agg(collect_list(col(ClimateZone.enumName)).as("ArrayClimate"), collect_list(col(Animal.enumName)).as("ArrayAnimal"))
			)

		val distinctclimatedf = grpdf.select(col("ParentLocation"), array_distinct(col("ArrayClimate")).as("DistinctClimate"), col("ArrayAnimal"))
		val explodeclimatedf = distinctclimatedf.select(col("ParentLocation"), explode(col("DistinctClimate")).as("NewClimate"), col("ArrayAnimal"))

		// HELP this way1 below is not grouping by parent first then climate while way2 below is -- why?

		// way1
		udfdf.groupBy(col("ParentLocation"), col("ClimateZone")).agg(collect_list(col("Animal")).as("AWC"))
		// way 2
		val climateprepdf = (grpdf.withColumn("expc", explode(col("ArrayClimate")))
			.withColumn("expa", explode(col("ArrayAnimal")))
			.select("ParentLocation", "expc", "expa")
			.groupBy("ParentLocation", "expc").agg(collect_list(col("expa")).as("arraya")))

		climateprepdf.groupBy("expc").agg(array_distinct(flatten(collect_list(col("arraya")))).as("AWC"))
		// TODO now decide how this result compares to the original climate grouping df and then how both each compare to the parent-loc grouping - do array_except
	}

}
