package com.SparkDocumentationByTesting.specs.AboutDataFrames


import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, ColumnName, DataFrame, DataFrameReader, DataFrameWriter, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
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



	// NOTE: say df.show(n, false) so not truncate the columns which contain the arrays as rows.

	/**
	 * SOURCE:
	 * 	- https://medium.com/expedia-group-tech/deep-dive-into-apache-spark-array-functions-720b8fbfa729
	 */
	describe("Array SQL Functions"){


		describe("array_contains: returns BOOL, answers whether a particular element is within the array-column"){


			import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._


			it("array_contains: answers strict equality"){

				val containsBearDf: DataFrame = animalArrayDf.withColumn("ContainsResult", array_contains(col("ArrayAnimal"), Bear.enumName))
				val numBear: Int = containsBearDf.select("ContainsResult").collectCol[Boolean].count(_ == true)

				numBear shouldEqual 1
			}

			it("udf method: can check kind-of relationship between elements, not just equality, like array_contains()"){

				import scala.util.Try
				def checkBearFamily(n: EnumString) = Try {
					Bear.withName(n)
				} // .toOption

				// Checking if element is of instance bear
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


		it("array_distinct: returns only distinct values within the array column"){


			import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._

			val distinctDf: DataFrame = animalArrayDf.withColumn("ArrayDistinct", array_distinct(col("ArrayAnimal")))

			val actualNumDistinct: Seq[Int] = distinctDf.select("ArrayDistinct").collectSeqCol[Animal].map(_.length)
			val actualNumOriginal: Seq[Int] = animalArrayDf.select("ArrayAnimal").collectSeqCol[Animal].map(_.length)
			val expectedNumDistinct: Seq[Int] = animalArrayDf.select("ArrayAnimal").collectSeqCol[Animal].map(_.distinct.length)

			actualNumDistinct shouldEqual expectedNumDistinct

			val originalSizesMinusDistinctSizes: Seq[Int] = actualNumOriginal.zip(actualNumDistinct).map{ case (originalSize, distinctSize) => originalSize - distinctSize }

			originalSizesMinusDistinctSizes.exists(_ > 0) shouldEqual true // meaningt he distinct took out some duplicates
		}



		it("array_except: returns elements from first array that are not in the second array (like set subtract)"){
			import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._

			// NOTE: could use Animal and collectSeqEnumCol[Animal] but it takes too long
			/*val pws = parentCADf.select("ArrayAnimalPC").collectSeqEnumCol[Animal]
			val cs = climateParentAnimalsDf.select("ArrayAnimalClimate").collectSeqEnumCol[Animal]*/

			val pcs: Seq[Seq[EnumString]] = parentCADf.select("ArrayAnimalPC").collectSeqCol[String]
			val cs: Seq[Seq[EnumString]] = climateParentAnimalsDf.select("ArrayAnimalC").collectSeqCol[String]
			val ps: Seq[Seq[EnumString]] = climateParentAnimalsDf.select("ArrayAnimalP").collectSeqCol[String]

			//NOTE: clarification:  p == parent == parent world == location
			val expectedPCCDiffs: Seq[Seq[EnumString]] = pcs.zip(cs).map{ case (locationClimateAnimals, climateAnimals) => locationClimateAnimals.toSet.diff(climateAnimals.toSet).toSeq }
			val expectedPCPDiffs: Seq[Seq[EnumString]] = pcs.zip(ps).map{ case (locationClimateAnimals, locationAnimals) => locationClimateAnimals.toSet.diff(locationAnimals.toSet).toSeq }

			/**
			 * Comparing animals:
			 * 1) conditioned on Parent-Climate with those conditioned just on Climate, (ExceptPC_C) then
			 * 2) conditioned on Parent-Cliamte with those conditioned just on Parent, (ExceptPC_W)
			 */
			val actualDiffsDf: DataFrame = (parentCADf.appendDf(climateParentAnimalsDf)
				.select(array_distinct(array_except(col("ArrayAnimalPC"), col("ArrayAnimalC"))).as("ExceptPC_C"),
					array_distinct(array_except(col("ArrayAnimalPC"), col("ArrayAnimalP"))).as("ExceptPC_P")
				))
			val actualPCCDiffs: Seq[Seq[EnumString]] = actualDiffsDf.select("ExceptPC_C").collectSeqCol[String]
			val actualPCPDiffs: Seq[Seq[EnumString]] = actualDiffsDf.select("ExceptPC_P").collectSeqCol[String]

			// NOTE: weird yields false if elements within each list are not directly aligned (e.g. Tiger is at index 1 in actuallist while is at index10 in dexpected list for pccs)
			actualPCCDiffs.map(_.sorted) shouldEqual expectedPCCDiffs.map(_.sorted) // TODO why error here?
			actualPCPDiffs.map(_.sorted) shouldEqual expectedPCPDiffs.map(_.sorted)

		}


		it("array_intersect: returns elements common from both arrays, like set intersect"){

			import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._

			val pcs: Seq[Seq[EnumString]] = parentCADf.select("ArrayAnimalPC").collectSeqCol[String]
			val cs: Seq[Seq[EnumString]] = climateParentAnimalsDf.select("ArrayAnimalC").collectSeqCol[String]
			val ps: Seq[Seq[EnumString]] = climateParentAnimalsDf.select("ArrayAnimalP").collectSeqCol[String]

			//NOTE: clarification:  p == parent == parent world == location
			val expectedPCCIntersects: Seq[Seq[EnumString]] = pcs.zip(cs).map { case (locationClimateAnimals, climateAnimals) => locationClimateAnimals.toSet.intersect(climateAnimals.toSet).toSeq }
			val expectedPCPIntersects: Seq[Seq[EnumString]] = pcs.zip(ps).map { case (locationClimateAnimals, locationAnimals) => locationClimateAnimals.toSet.intersect(locationAnimals.toSet).toSeq }

			/**
			 * Comparing animals:
			 * 1) conditioned on Parent-Climate with those conditioned just on Climate, (ExceptPC_C) then
			 * 2) conditioned on Parent-Cliamte with those conditioned just on Parent, (ExceptPC_W)
			 */
			val actualIntersectsDf: DataFrame = (parentCADf.appendDf(climateParentAnimalsDf)
				.select(array_distinct(array_intersect(col("ArrayAnimalPC"), col("ArrayAnimalC"))).as("IntersectPC_C"),
					array_distinct(array_intersect(col("ArrayAnimalPC"), col("ArrayAnimalP"))).as("IntersectPC_P")
				))
			val actualPCCIntersects: Seq[Seq[EnumString]] = actualIntersectsDf.select("IntersectPC_C").collectSeqCol[String]
			val actualPCPIntersects: Seq[Seq[EnumString]] = actualIntersectsDf.select("IntersectPC_P").collectSeqCol[String]

			// NOTE: weird yields false if elements within each list are not directly aligned (e.g. Tiger is at index 1 in actuallist while is at index10 in dexpected list for pccs)
			actualPCCIntersects.map(_.sorted) shouldEqual expectedPCCIntersects.map(_.sorted) // TODO why error here?
			actualPCPIntersects.map(_.sorted) shouldEqual expectedPCPIntersects.map(_.sorted)
		}


		it("array_join: joins all the array elements given a delimiter"){

			import utilities.DataHub.ManualDataFrames.ArrayNumDf._

			val delimiter: String = ","

			val arrayJoinDf: DataFrame = (arrayGroupDf.select(
				col("col1"),
				col("ArrayCol2"),
				array_join(col("ArrayCol2"), delimiter).as("JoinArrayCol2")
			))

			val arrayJoinRows: Seq[Row] = Seq(
				("x", Array(4, 6, 7, 9, 2), "4,6,7,9,2"),
				("z", Array(7, 5, 1, 4, 7, 1), "7,5,1,4,7,1"),
				("a", Array(3, 8, 5, 3), "3,8,5,3")
			).toRows(arrayJoinDf.schema)

			arrayJoinDf.collectAll shouldEqual arrayJoinRows
		}

		it("array_max: returns maximum element in the array that is located in the row"){

			import utilities.DataHub.ManualDataFrames.ArrayNumDf._

			val arrayMaxDf: DataFrame = arrayGroupDf.select(col("col1"), col("ArrayCol2"), array_max(col("ArrayCol2")).as("ArrayMax2"), col("ArrayCol3"), array_max(col("ArrayCol3")).as("ArrayMax3"))

			// TESTING way 1
			arrayMaxDf.select("ArrayCol2").collectSeqCol[Int].map(_.max) shouldEqual arrayMaxDf.select("ArrayMax2").collectCol[Int]
			arrayMaxDf.select("ArrayCol3").collectSeqCol[Int].map(_.max) shouldEqual arrayMaxDf.select("ArrayMax3").collectCol[Int]

			// TESTING way 2
			val arrayMaxRows: Seq[Row] = Seq(
				("x",
					Array(4, 6, 7, 9, 2), 9,
					Array(1, 2, 3, 7, 7), 7),
				("z",
					Array(7, 5, 1, 4, 7, 1), 7,
					Array(3, 2, 8, 9, 4, 9), 9),
				("a",
					Array(3, 8, 5, 3), 8,
					Array(4, 5, 2, 8), 8)
			).toRows(targetSchema = arrayMaxDf.schema)

			arrayMaxDf.collectAll shouldEqual arrayMaxRows
		}


		it("array_min: returns minimum element in the array that is located in the row") {

			import utilities.DataHub.ManualDataFrames.ArrayNumDf._

			val arrayMinDf: DataFrame = arrayGroupDf.select(col("col1"), col("ArrayCol2"), array_min(col("ArrayCol2")).as("ArrayMin2"), col("ArrayCol3"), array_min(col("ArrayCol3")).as("ArrayMin3"))

			// TESTING way 1
			arrayMinDf.select("ArrayCol2").collectSeqCol[Int].map(_.min) shouldEqual arrayMinDf.select("ArrayMin2").collectCol[Int]
			arrayMinDf.select("ArrayCol3").collectSeqCol[Int].map(_.min) shouldEqual arrayMinDf.select("ArrayMin3").collectCol[Int]

			// TESTING way 2
			val arrayMinRows: Seq[Row] = Seq(
				("x",
					Array(4, 6, 7, 9, 2), 2,
					Array(1, 2, 3, 7, 7), 1),
				("z",
					Array(7, 5, 1, 4, 7, 1), 1,
					Array(3, 2, 8, 9, 4, 9), 2),
				("a",
					Array(3, 8, 5, 3), 3,
					Array(4, 5, 2, 8), 2)
			).toRows(targetSchema = arrayMinDf.schema)

			arrayMinDf.collectAll shouldEqual arrayMinRows
		}


		it("array_position: returns position of first occurrence of the specified element. " +
			"If element is not present in the array that is in the row, then the function returns 0 " +
			"(NOTE: position is index-1 based not index-0 based)") {

			import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._


			val arrayPositionDf: DataFrame = (animalArrayDf.select(col("ArrayAnimal"),
				array_position(col("ArrayAnimal"), Camel.enumName).as("PosAnimal"),
				col("ArrayWorld"),
				array_position(col("ArrayWorld"), Brazil.enumName).as("PosWorld")))

			val posAnimal: Seq[Int] = arrayPositionDf.select("PosAnimal").collectCol[Int]
			val posWorld: Seq[Int] = arrayPositionDf.select("PosWorld").collectCol[Int]

			posAnimal.exists(_ > 0) shouldEqual true
			posWorld.exists(_ > 0) shouldEqual true
			/*posAnimal shouldEqual Seq(2, 0, 0, 0, 0, 6, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
			posWorld shouldEqual Seq(0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)*/
		}


		describe("array_remove()"){
			it("array_remove: removes all occurrences of a given element from the array"){

				// TODO check that this method yields fewer differences than for udf. 
			}

			it("array_remove() is different from udf that can remove all instances of a given element"){

				import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._

				import scala.util.Try

				def checkIsNorthAmerica(n: EnumString) = Try {
					NorthAmerica.withName(n)
				} // .toOption

				// Checking if element is of instance NorthAmerica
				val filterNorthAmerica: Seq[String] => Seq[String] = (locations) => {
					locations.filterNot((loc: String) => checkIsNorthAmerica(loc).isSuccess)
					//animals.head.toSeq.asInstanceOf[Seq[String]].exists(am => checkerBear(am).isDefined)
				}
				// NOTE: must put types explicitly or else get error
				// SOURCE: chp 6 bill chambers
				val northAmericaUdf: UserDefinedFunction = udf(filterNorthAmerica(_: Seq[String]): Seq[String])

				val removeNorthAmericaDf: DataFrame = animalArrayDf.select(col("ArrayWorld"), northAmericaUdf(col("ArrayWorld")).as("RemoveResult"))

				// Checking that north america has been removed
				val noNorthAmericas: Seq[Seq[World]] = removeNorthAmericaDf.select("RemoveResult").collectSeqEnumCol[World]
				noNorthAmericas.forall(! _.isInstanceOf[NorthAmerica]) shouldEqual true

				val worldsWithNorthAmerica: Seq[Seq[String]] = removeNorthAmericaDf.select("ArrayWorld").collectSeqCol[String]
				worldsWithNorthAmerica.zip(noNorthAmericas).exists{case (l1, l2) => l1.length - l2.length > 0}
			}
		}


		it("array_repeat: repeats the given element the specified number of times"){

		}


		it("array_zip"){

		}
		it("array_overlap"){

		}
	}

}
