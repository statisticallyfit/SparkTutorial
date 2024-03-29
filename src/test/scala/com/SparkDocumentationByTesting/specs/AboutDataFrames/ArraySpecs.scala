package com.SparkDocumentationByTesting.specs.AboutDataFrames




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

//import com.SparkSessionForTests
import com.SparkDocumentationByTesting.CustomMatchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import org.scalatest.Assertions._
import utilities.SparkSessionWrapper



/**
 *
 */
class ArraySpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {

	/*import AnimalState._
	import FlightState._
	import TradeState._*/

	import sparkSessionWrapper.implicits._


	import utilities.DataHub.ManualDataFrames.ArrayNumDf._

	// NOTE: say df.show(n, false) so not truncate the columns which contain the arrays as rows.

	/**
	 * SOURCE:
	 * 	- https://medium.com/expedia-group-tech/deep-dive-into-apache-spark-array-functions-720b8fbfa729
	 */
	describe("Array SQL Functions"){

		val animalArrayDf: DataFrame = (animalDf.groupBy(ClimateZone.enumName, Biome.enumName)
			.agg(collect_list(col(Animal.enumName)).as("ArrayAnimal"),
				collect_list(col("Amount")).as("ArrayAmount"),
				collect_list(col(World.enumName)).as("ArrayWorld"),
				//collect_list(col())
			))

		describe("array_contains(): returns BOOL, answers whether a particular element is within the array-column"){


			it("array_contains(): answers strict equality"){

				val containsBearDf: DataFrame = animalArrayDf.withColumn("ContainsResult", array_contains(col("ArrayAnimal"), Bear.enumName))
				val numBear: Int = containsBearDf.select("ContainsResult").collectCol[Boolean].count(_ == true)

				numBear shouldEqual 1
			}

			it("udf method: can check kind-of relationship between elements, not just equality, like array_contains()"){

				import scala.util.Try
				def checkBearFamily(n: EnumString) = Try {
					Bear.withName(n)
				} // .toOption

				// TODO how to check whether an elemen tis of instance Bear??? how tu se udf, gives error help?
				val filterBearType: Seq[String] => Boolean = (animals) => {
					animals.exists((am: String) => checkBearFamily(am).isSuccess)
					//animals.head.toSeq.asInstanceOf[Seq[String]].exists(am => checkerBear(am).isDefined)
				}
				// NOTE: must put types explicitly or else get error
				// SOURCE: chp 6 bill chambers
				val bearUdf: UserDefinedFunction = udf(filterBearType(_: Seq[String]): Boolean)

				val containsBearKindDf: DataFrame = animalArrayDf.select(col("ArrayAnimal"), bearUdf(col("ArrayAnimal")).as("ContainsResult"))

				val numKindsOfBear: Int = containsBearKindDf.select("ContainsResult").collectCol[Boolean].count(_ == true)

				val expectedBearKindSchema: StructType = containsBearKindDf.schema

				val expectedContainsBearKindRows: Seq[Row] = Seq(
					(Seq(Pelican), false),
					(Seq(Camel, Falcon, Falcon, Hyena, Hyena, SandCat), false),
					(Seq(Crocodile, Termite, Gorilla, Panther, Tiger, Jaguar, Butterfly, Panda, Leopard, Jellyfish, Dragonfly, Flamingo, Ocelot, Termite, Leopard, Ocelot, Termite, Snake, Dolphin, Jellyfish, Howler, Butterfly, Termite, Capuchin, Leopard, Butterfly, Spider, Rat, Lemur, Dragonfly, Beetle, Lemur, Mustang, Dragonfly, DutchWarmbloodHorse, Clydesdale), true),
					(Seq(Bee, Clam, IberianLynx, Macaque, RoeDeer, Goldfinch, Falcon, GoldenEagle, RedDeer, RedDeer, Sparrow, Robin, Canary, BrownBear, Weasel, Goldfinch, Otter, Ferret, Marten, RedDeer, Lynx, Mouse, Marmot), true),
					(Seq(Oyster, Beaver, Falcon, Mink), false),
					(Seq(BrownBear), true),
					(Seq(Swan), false)
				).toRows(expectedBearKindSchema)

				expectedContainsBearKindRows.forall(row => containsBearKindDf.collectAll.contains(row)) shouldEqual true

				// Showing how the udf method is different than the array_contains method
				numKindsOfBear should be > 1
			}

		}


		it("array_distinct(): returns only distinct values within the array column"){

			val distinctDf: DataFrame = animalArrayDf.withColumn("ArrayDistinct", array_distinct(col("ArrayAnimal")))

			val actualNumDistinct: Seq[Int] = distinctDf.select("ArrayDistinct").collectSeqCol[Animal].map(_.length)
			val actualNumOriginal: Seq[Int] = animalArrayDf.select("ArrayAnimal").collectSeqCol[Animal].map(_.length)
			val expectedNumDistinct: Seq[Int] = animalArrayDf.select("ArrayAnimal").collectSeqCol[Animal].map(_.distinct.length)

			actualNumDistinct shouldEqual expectedNumDistinct

			val originalSizesMinusDistinctSizes: Seq[Int] = actualNumOriginal.zip(actualNumDistinct).map{ case (originalSize, distinctSize) => originalSize - distinctSize }

			originalSizesMinusDistinctSizes.exists(_ > 0) shouldEqual true // meaningt he distinct took out some duplicates
		}






		it("array_zip"){

		}
		it("array_overlap"){

		}
	}

}
