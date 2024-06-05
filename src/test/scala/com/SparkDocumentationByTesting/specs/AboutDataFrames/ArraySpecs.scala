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
import utilities.DataHub.ManualDataFrames.ArrayDf._
import utilities.DataHub.ManualDataFrames.fromSparkByExamples._
import scala.Double.NaN
import utilities.GeneralMainUtils.Helpers._
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
	describe("Array SQL Functions") {


		describe("array_contains: returns BOOL, answers whether a particular element is within the array-column") {


			import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._


			it("array_contains: answers strict equality") {

				val containsBearDf: DataFrame = animalArrayDf.withColumn("ContainsResult", array_contains(col("ArrayAnimal"), Bear.enumName))
				val numBear: Int = containsBearDf.select("ContainsResult").collectCol[Boolean].count(_ == true)

				numBear shouldEqual 1
			}

			it("udf method: can check kind-of relationship between elements, not just equality, like array_contains()") {

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


		it("array_distinct: returns only distinct values within the array column") {


			import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._

			val distinctDf: DataFrame = animalArrayDf.withColumn("ArrayDistinct", array_distinct(col("ArrayAnimal")))

			val actualNumDistinct: Seq[Int] = distinctDf.select("ArrayDistinct").collectSeqCol[Animal].map(_.length)
			val actualNumOriginal: Seq[Int] = animalArrayDf.select("ArrayAnimal").collectSeqCol[Animal].map(_.length)
			val expectedNumDistinct: Seq[Int] = animalArrayDf.select("ArrayAnimal").collectSeqCol[Animal].map(_.distinct.length)

			actualNumDistinct shouldEqual expectedNumDistinct

			val originalSizesMinusDistinctSizes: Seq[Int] = actualNumOriginal.zip(actualNumDistinct).map { case (originalSize, distinctSize) => originalSize - distinctSize }

			originalSizesMinusDistinctSizes.exists(_ > 0) shouldEqual true // meaningt he distinct took out some duplicates
		}


		it("array_except: returns elements from first array that are not in the second array (like set subtract)") {
			import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._

			// NOTE: could use Animal and collectSeqEnumCol[Animal] but it takes too long
			/*val pws = parentCADf.select("ArrayAnimalPC").collectSeqEnumCol[Animal]
			val cs = climateParentAnimalsDf.select("ArrayAnimalClimate").collectSeqEnumCol[Animal]*/

			val pcs: Seq[Seq[EnumString]] = parentCADf.select("ArrayAnimalPC").collectSeqCol[String]
			val cs: Seq[Seq[EnumString]] = climateParentAnimalsDf.select("ArrayAnimalC").collectSeqCol[String]
			val ps: Seq[Seq[EnumString]] = climateParentAnimalsDf.select("ArrayAnimalP").collectSeqCol[String]

			//NOTE: clarification:  p == parent == parent world == location
			val expectedPCCDiffs: Seq[Seq[EnumString]] = pcs.zip(cs).map { case (locationClimateAnimals, climateAnimals) => locationClimateAnimals.toSet.diff(climateAnimals.toSet).toSeq }
			val expectedPCPDiffs: Seq[Seq[EnumString]] = pcs.zip(ps).map { case (locationClimateAnimals, locationAnimals) => locationClimateAnimals.toSet.diff(locationAnimals.toSet).toSeq }

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


		it("array_intersect: returns elements common from both arrays, like set intersect") {

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


		it("array_join: joins all the array elements given a delimiter") {


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

		it("array_max: returns maximum element in the array that is located in the row") {


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


		describe("array_remove()") {

			it("array_remove: removes all occurrences of a given element from the array") {

				import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._

				// Mini-exercise: first find the most common element
				def findMostCommonOccurrence[A]: Seq[A] => A = (elems: Seq[A]) => {
					val cnts: Seq[Int] = elems.map { currElem => elems.count(_ == currElem) }

					val mostCommonElement: A = cnts.forall(_ == 1) match {
						// if all appear same amount of times, then return first element
						case true => elems.head
						// if indeed an element appears more than others (max) then return that one
						case false => elems.zip(cnts).sortBy { case (k, v) => v }.last._1
					}

					mostCommonElement
				}

				val udfMostCommonElem: UserDefinedFunction = udf(findMostCommonOccurrence[String](_: Seq[String]): String)

				val mostCommonElemDf: DataFrame = (animalArrayDf
					.select(col("ArrayAnimal"),
						udfMostCommonElem(col("ArrayAnimal")).as("MostCommonAnimal")))

				// The focus: removing: remove this (most common) element.
				// NOTE: new thing learned: can remove dynamic element, like a column element that changes with each row, (the element you remove doesn't have to be static)
				val arrayRemoveDf: DataFrame = mostCommonElemDf.withColumn("RemoveResult", array_remove(col("ArrayAnimal"), col("MostCommonAnimal")))

				// Now assert it was removed:
				val removed: Seq[Seq[String]] = arrayRemoveDf.select("RemoveResult").collectSeqCol[String]
				val mostCommon: Seq[String] = arrayRemoveDf.select("MostCommonAnimal").collectCol[String]

				removed.zip(mostCommon).forall { case (rs, c) => !rs.contains(c) } shouldEqual true
				//removed.zip(mostCommon).forall{ case (rs, c) => rs shouldNot contain c}
			}

			it("udf, instead, can remove all instances of a given element") {

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
				noNorthAmericas.forall(!_.isInstanceOf[NorthAmerica]) shouldEqual true

				val worldsWithNorthAmerica: Seq[Seq[String]] = removeNorthAmericaDf.select("ArrayWorld").collectSeqCol[String]
				worldsWithNorthAmerica.zip(noNorthAmericas).exists { case (l1, l2) => l1.length - l2.length > 0 }
			}
		}


		describe("array_repeat: repeats the given element the specified number of times") {

			it("can repeat an integer number of items") {

				val repeatByIntDf: DataFrame = tradeDf.withColumn("RepeatInstrument", array_repeat(col(FinancialInstrument.enumName), 3))

				repeatByIntDf.select("RepeatInstrument").collectSeqCol[String].forall(_.length == 3) shouldEqual true

			}

			it("can repeat an element a number of times defined by the right Column argument") {

				import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._

				// Mini-exercise: repeat the least common element per row as many times as the most common element appears in that row.

				def findOccurrence[A](elems: Seq[A], mostCommon: Boolean = true): A = {
					val cnts: Seq[Int] = elems.map { currElem => elems.count(_ == currElem) }

					val mostOrLeastCommonElement: A = cnts.forall(_ == 1) match {
						// if all appear same amount of times, then return first element
						case true => elems.head
						// if indeed an element appears more than others (max) then return that one
						case false => mostCommon match {
							case true => elems.zip(cnts).maxBy { case (k, v) => v }._1
							case false => elems.zip(cnts).minBy { case (k, v) => v }._1
						}
					}

					mostOrLeastCommonElement
				}

				// First step: find the most common element
				val udfMostCommonElem: UserDefinedFunction = udf(findOccurrence[String](_: Seq[String], true: Boolean): String)
				// Second step: find the least common element
				val udfLeastCommonElem: UserDefinedFunction = udf(findOccurrence[String](_: Seq[String], false: Boolean): String)

				// Third step: find the count of how many times the most common element appears in the row (udf)
				def countOccurrence[A](elem: A, seq: Seq[A]): Int = {
					seq.count(_ == elem)
				}

				val udfCountOccurrence: UserDefinedFunction = udf(countOccurrence[String](_: String, _: Seq[String]): Int)

				// Fourth step: add most common, its count, least common elements as cols to the df
				val mostCommonCountLeastDf: DataFrame = animalArrayDf.select(
					col("ArrayAnimal"),
					udfMostCommonElem(col("ArrayAnimal")).alias("MostCommon"),
					udfCountOccurrence(col("MostCommon"), col("ArrayAnimal")).alias("CountMostCommon"),
					udfLeastCommonElem(col("ArrayAnimal")).alias("LeastCommon")
				)

				// Fifth step: repeat the least common element as many times as most common element appears (udf)
				val repeatDf: DataFrame = mostCommonCountLeastDf.withColumn("RepeatLeastCommonLikeMostCommon",
					array_repeat(col("LeastCommon"), col("CountMostCommon"))
				)

				// Assert that most common element OCCURS as many times as the LEASTCOMMON element in the last col of this dataframe
				val numCountsCommonElems: Seq[Int] = repeatDf.select("CountMostCommon").collectCol[Int]
				val leastCommonElems: Seq[Seq[String]] = repeatDf.select("RepeatLeastCommonLikeMostCommon").collectSeqCol[String]

				// Showing that the least common elements were repeated as many times as the most common elements.
				leastCommonElems.map(_.length).zip(numCountsCommonElems).forall { case (leastCommonLst, cntMostCommon) => leastCommonLst == cntMostCommon } shouldEqual true

			}
		}



		/**
		 * SOURCE:
		 * 	- https://towardsdatascience.com/the-definitive-way-to-sort-arrays-in-spark-1224f5529961
		 */

		describe("Array sorting: multiple examples using sort_array, array_sort, sortBy, & co. ") {


			import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._


			describe("sorting using: sort_array") {


				it("way 1: sort_array() for simple case: Sorts the input array in ascending or descending order." +
					"NaN is greater than any non-NaN elements for double/float type. " +
					"Null elements will be placed at the beginning of the returned array in ascending order or at the end of the returned array in descending order.") {


					val sortArrayAscDf: DataFrame = arrayNullGroupDf.select(col("col1"), sort_array(col("ArrayCol2"), asc = true))

					val expectedSortArrayAscRows: Seq[Row] = Seq(
						("x", Array(1, 1.3, 2, 2, 4, 6, 7.6, 8, 9, NaN, NaN, null, null).map(getSimpleString)),
						("z", Array(0.3, 1.1, 1.2, 4, 5.8, 7, 7.5, 8.8, NaN, NaN, null, null, null).map(getSimpleString)),
						("a", Array(2, 3, 3, 3.4, 4, 5, 8.1, NaN, NaN, null, null).map(getSimpleString))
					).toRows(sortArrayAscDf.schema)

					sortArrayAscDf.collectAll shouldEqual expectedSortArrayAscRows


					// ---

					val sortArrayDescDf: DataFrame = arrayNullGroupDf.select(col("col1"), sort_array(col("ArrayCol2"), asc = false))

					val actualRows: Seq[Row] = sortArrayDescDf.collect().toSeq

					val expectedRows: Seq[Row] = Seq(
						("x", Array(null, null, NaN, NaN, 9, 8, 7.6, 6, 4, 2, 2, 1.3, 1).map(getSimpleString)),
						("z", Array(null, null, null, NaN, NaN, 8.8, 7.5, 7, 5.8, 4, 1.2, 1.1, 0.3).map(getSimpleString)),
						("a", Array(null, null, NaN, NaN, 8.1, 5, 4, 3.4, 3, 3, 2).map(getSimpleString))
					).toRows(sortArrayDescDf.schema)

					actualRows shouldEqual expectedRows
				}


				/**
				 * SOURCES:
				 * 	- https://stackoverflow.com/questions/73259833/sort-array-of-structs
				 */

				it("sort_array: case for data that has multiple fields, so sorting by manipulating the fields via transform()"){


					import utilities.DataHub.ManualDataFrames.ArrayDf._
					import personInfo._


					val sortArrayDf: DataFrame = (personDf
						.withColumn("rearrangeMidFirst", transform(col("yourArray"), elem => struct(
							elem.getField("middleInitialThrice"),
							elem.getField("id"),
							elem.getField("name"),
							elem.getField("age"),
							elem.getField("addressNumber"))))
						.withColumn("sortedByMiddle", sort_array(col("rearrangeMidFirst"))))


					// Test the order of mid,names in the result
					val actualSortArraySeq: Seq[SortByMidStruct[PersonMidIdNameAgeStruct]] = (sortArrayDf
						.as[SortByMidStruct[PersonMidIdNameAgeStruct]]
						.collect().toSeq /*
						.map((rms: SortByMidStruct[PersonMidIdNameAgeStruct]) => rms.sortedByMiddle.map(person => (person.middleInitialThrice, person.id, person.name, person.age)))*/)

					actualSortArraySeq shouldEqual expectedSortArraySeq
				}
			}


			// TESTING: array_sort of sorting on keys using comparator
			/**
			 * SOURCES:
			 * 	- https://hyp.is/lVKHevUrEe6Hpq_i_GK6Sg/towardsdatascience.com/the-definitive-way-to-sort-arrays-in-spark-1224f5529961
			 * 	- https://juejin.cn/s/spark%20sql%20sort%20array%20of%20struct
			 */
			it("sorting using: array_sort() + comparator passed to udf") {

				val funcMiddleInitialComparator: (PersonStruct, PersonStruct) => Int = (p1, p2) => if (p1.middleInitialThrice < p2.middleInitialThrice) -1 else if (p1.middleInitialThrice == p2.middleInitialThrice) 0 else 1
				val funcNameComparator: (PersonStruct, PersonStruct) => Int = (p1, p2) => if (p1.name < p2.name) -1 else if (p1.name == p2.name) 0 else 1
				val funcIDComparator: (PersonStruct, PersonStruct) => Int = (p1, p2) => if(p1.id < p2.id) -1 else if (p1.id == p2.id) 0 else 1
				val funcAgeComparator: (PersonStruct, PersonStruct) => Int = (p1, p2) => if(p1.age < p2.age) -1 else if (p1.age == p2.age) 0 else 1
				val funcAddressComparator: (PersonStruct, PersonStruct) => Int = (p1, p2) => if(p1.addressNumber < p2.addressNumber) -1 else if (p1.addressNumber == p2.addressNumber) 0 else 1


				val udfMiddleInitialComparator: UserDefinedFunction = udf(funcMiddleInitialComparator(_: PersonStruct, _: PersonStruct): Int)
				val udfNameComparator: UserDefinedFunction = udf(funcNameComparator(_: PersonStruct, _: PersonStruct): Int)
				val udfIDComparator: UserDefinedFunction = udf(funcIDComparator(_: PersonStruct, _: PersonStruct): Int)
				val udfAgeComparator: UserDefinedFunction = udf(funcAgeComparator(_: PersonStruct, _: PersonStruct): Int)
				val udfAddressComparator: UserDefinedFunction = udf(funcAddressComparator(_: PersonStruct, _: PersonStruct): Int)


				val arraySortComparatorUdfDf: DataFrame = (personRDD.toDF()
					.withColumn("sortedByMiddle", array_sort(col("yourArray"), comparator = (p1, p2) => udfMiddleInitialComparator(p1, p2)))
					.withColumn("sortedByName", array_sort(col("sortedByMiddle"), comparator = (p1, p2) => udfNameComparator(p1, p2)))
					.withColumn("sortedByAge", array_sort(col("sortedByName"), comparator = (p1, p2) => udfAgeComparator(p1, p2)))
					.withColumn("sortedByID", array_sort(col("sortedByAge"), comparator = (p1, p2) => udfIDComparator(p1, p2)))
					.withColumn("sortedByAddress", array_sort(col("sortedByID"), comparator = (p1, p2) => udfAddressComparator(p1, p2)))
					)


				// NOTE: collecting the items as Row objects will result in error for innermost struct, classcasexception Seq[Nothing] so easiest to convert to dataset then get the rwos as objects.

				val actualUdfMidSortSeq: Seq[SortByMidStruct[PersonStruct]] = (arraySortComparatorUdfDf.as[SortByMidStruct[PersonStruct]].collect().toSeq)
				val actualUdfSortMidThenNameSeq: Seq[SortByNameStruct[PersonStruct]] = arraySortComparatorUdfDf.as[SortByNameStruct[PersonStruct]].collect().toSeq
				val actualUdfSortMidThenNameAge: Seq[SortByAgeStruct[PersonStruct]] = arraySortComparatorUdfDf.as[SortByAgeStruct[PersonStruct]].collect().toSeq
				val actualUdfSortMidThenNameAgeID: Seq[SortByIDStruct[PersonStruct]] = arraySortComparatorUdfDf.as[SortByIDStruct[PersonStruct]].collect().toSeq

				expectedUdfComparatorMidSortSeq shouldEqual actualUdfMidSortSeq

				// Checking order of names after sorting by middle initial:
				expectedNames_afterMid shouldEqual actualUdfMidSortSeq.map(smi => smi.sortedByMiddle.map(ps => ps.name))

				// Checking order of middle initial after sorting by middle initial
				expectedMiddles_afterMid shouldEqual actualUdfMidSortSeq.map(smi => smi.sortedByMiddle.map(ps => ps.middleInitialThrice))

				// Checking order of names after sorting by mid, then name
				expectedNames_afterMidThenName shouldEqual actualUdfSortMidThenNameSeq.map(smni => smni.sortedByName.map(ps => ps.name))
				// Checking order of names after sorting by mid, then name, then id
				expectedNames_afterMidThenNameAge shouldEqual actualUdfSortMidThenNameAge.map(sma => sma.sortedByAge.map(ps => ps.name))
				// Checking order of names after sorting by mid, name, id, age
				expectedNames_afterMidThenNameAgeID shouldEqual actualUdfSortMidThenNameAgeID.map(smida => smida.sortedByID.map(ps => ps.name))

				// TODO why does the dataset contain all colnames while after collect() only the smni arg is available?
			}





			// TESTING: sorting using transform, array_sort, map_from_entries

			describe("sorting using: array_sort + transform + map_from_entries") {


				/**
				 * WAY 1: sql string code
				 *
				 * SOURCES:
				 * 	- https://hyp.is/OM9XcvT4Ee6oIhtawMKnnQ/archive.ph/2021.05.23-062738/https://towardsdatascience.com/did-you-know-this-in-spark-sql-a7398bfcc41e
				 */
				it("way 1: using sql string code") {

					val actualDf: DataFrame = personDf.withColumn("sortedByMiddle", expr(
						"array_sort(yourArray,	(left, right) -> case when left.middleInitialThrice < right.middleInitialThrice then -1 when left.middleInitialThrice > right.middleInitialThrice then 1 else 0 end)"))

					expectedUdfComparatorMidSortSeq shouldEqual actualDf.as[SortByMidStruct[PersonStruct]]
				}

				/**
				 * WAY 2: spark code
				 *
				 * SOURCES:
				 * Converting to Map:
				 * 	- https://sparkbyexamples.com/spark/spark-sql-map-functions/#map-from-entries
				 * 	- https://sparkbyexamples.com/spark/spark-how-to-convert-structtype-to-a-maptype/
				 *       Sorting by keys from a map type:
				 * 	- https://stackoverflow.com/questions/72652903/return-map-values-sorted-by-keys?rq=3
				 * 	- https://stackoverflow.com/questions/65929879/sort-by-key-in-map-type-column-for-each-row-in-spark-dataframe#:~:text=You%20can%20first%20get%20the,two%20arrays%20using%20map_from_arrays%20function.
				 */
				it("way 2: using spark code") {

					// WARNING: prerequisite to have unique map keys

					// Rearranging the struct to be nested so it can be converted to map (from two pairs)
					val twoFieldsDf: DataFrame = (personUniqueMidDf
						.withColumn("twoFields",transform(
							col("yourArray"),
							elem => struct(elem.getField("middleInitialThrice"),
								struct(
									elem.getField("id"),
									elem.getField("name"),
									elem.getField("addressNumber"),
									elem.getField("age")
								)
							)
						)))

					// Creating map from the nested struct
					val mapFieldsDf: DataFrame = (twoFieldsDf
						.withColumn("mapEntries", map_from_entries(col("twoFields"))))

					// Sorting by map keys
					val sortedNestedStructDf: DataFrame = (mapFieldsDf
						.withColumn("sortedValues", transform(
							array_sort(map_keys(col("mapEntries"))),
							k => struct(k.as("middleInitialThrice"), col("mapEntries")(k).as("rest"))
						)))

					// Rearranging the struct to be flattened and in the same order as PersonStruct
					val sortedStructDf: DataFrame = sortedNestedStructDf.withColumn("sortedStructs", transform(col("sortedValues"),
						stc => struct(stc.getField("rest").getField("id"),
							stc.getField("rest").getField("name"),
							stc.getField("middleInitialThrice"),
							stc.getField("rest").getField("addressNumber"),
							stc.getField("rest").getField("age")
						)))

					val actualArraySortTransformMapMidSort: Dataset[SortByMidStruct[PersonStruct]] = (sortedStructDf
						.withColumnRenamed("sortedStructs", "sortedByMiddle")
						.as[SortByMidStruct[PersonStruct]])

					expectedArraySortTransformMapMidSort shouldEqual actualArraySortTransformMapMidSort.collect.toSeq
				}
			}


			// TESTING: explode + sort on columns
			it("sorting using: explode + map_from_entries() + sort() + groupBy") {


				import utilities.DataHub.ManualDataFrames.ArrayDf._
				import personInfo._

				/**
				 * SOURCES:
				 * 	- sort on column = https://medium.com/@sfranks/i-had-trouble-finding-a-nice-example-of-how-to-have-an-udf-with-an-arbitrary-number-of-function-9d9bd30d0cfc
				 * 	- sort each col = https://sparkbyexamples.com/spark/spark-sort-column-in-descending-order/
				 * 	- https://sparkbyexamples.com/spark/spark-how-to-sort-dataframe-column-explained/
				 *
				 */

				val explodeSortDf_1: DataFrame = (personDf
					.withColumn("twoFields", transform(col("yourArray"), elem =>
						struct(
							elem.getField("middleInitialThrice"),
							elem.getField("id")))
					)
					.withColumn("explodeElem", explode(col("twoFields")))
					//.withColumn("mapEntries", map_from_entries(col("twoFields")))
					.withColumn("toMap", map_from_entries(array(col("explodeElem"))))
					.select(col("groupingKey"), col("explodeElem"), explode(col("toMap"))))


				val explodeSortDf_2a: DataFrame = (explodeSortDf_1
					.sort(col("groupingKey").asc, col("key").asc, col("value").asc) // sorting on the property
					.groupBy("groupingKey").agg(collect_list(col("explodeElem")).as("sortedByMiddle")) // grouping to make array of structs again
					)

				val explodeSortDf_2b: DataFrame = (explodeSortDf_1
					.sort(col("explodeElem.middleInitialThrice").asc, col("explodeElem.id").asc)
					.groupBy("groupingKey").agg(collect_list(col("explodeElem")).as("sortedByMiddle"))
					)

				explodeSortDf_2a.collect.toSeq shouldEqual explodeSortDf_2b.collect.toSeq

				// ---------------------------

				val actualExplodeSortTups: Array[Seq[(String, Int)]] = explodeSortDf_2a.as[SortByMidStruct[PersonMidIDStruct]].collect().map(dpm => dpm.sortedByMiddle.map(ps => (ps.middleInitialThrice, ps.id)))

				actualExplodeSortTups shouldEqual expectedExplodeSortTups

				// --------------------------

				val explodeSortDf_3: Dataset[Row] = (personDf
					.withColumn("explodeElems", explode(col("yourArray")))
					.sort(col("explodeElems.middleInitialThrice").asc,
						col("explodeElems.id").asc,
						col("explodeElems.age").asc,
						col("explodeElems.addressNumber").asc,
						col("explodeElems.name").asc)
					.drop(col("yourArray"))
					.groupBy("groupingKey")
					.agg(collect_list(col("explodeElems")).as("sorted"))
					.sort(col("groupingKey").asc)
					/*.as[SortStruct[PersonMidFirstStruct]]*/)

				val actualExplodeMultiSortTups: Seq[Seq[PersonStruct]] = (explodeSortDf_3
					.as[SortStruct[PersonStruct]]
					.collect.toSeq
					.map(dgs => dgs.sorted.sortBy(ps => (ps.middleInitialThrice, ps.id, ps.age, ps.addressNumber, ps.name)))
					)


				actualExplodeMultiSortTups shouldEqual expectedExplodeMultiSortTups
			}







			// TESTING: (explode) + grouping, array_sort + on property
			/**
			 * SOURCES:
			 * 	- https://hyp.is/cm1Z7vBCEe6jf0Piuu3GLg/www.geeksforgeeks.org/sorting-an-array-of-a-complex-data-type-in-spark/
			 */
			it("sorting using: explode + grouping + array_sort on property") {

				val explodeArraySortDf_1: DataFrame = (personDf
					.withColumn("explodeElems", explode(col("yourArray")))
					.groupBy("groupingKey")
					.agg(array_sort(collect_list(struct(
						col("explodeElems.middleInitialThrice"),
						col("explodeElems.name"),
						col("explodeElems.id")

					))).as("sortedByMiddle")))

				val actualExplodeArraySortTups: Seq[Seq[(String, String, Int)]] = (explodeArraySortDf_1
					.as[SortByMidStruct[PersonMidNameIDStruct]]
					.collect().toSeq
					.map(strct => strct.sortedByMiddle.map(p => (p.middleInitialThrice, p.name, p.id))))

				expectedExplodeArraySortTups shouldEqual actualExplodeArraySortTups
			}





			// TESTING:  udf + sortby on property + seq[Row] way

			/**
			 * SOURCES:
			 * 	- https://medium.com/@sfranks/i-had-trouble-finding-a-nice-example-of-how-to-have-an-udf-with-an-arbitrary-number-of-function-9d9bd30d0cfc
			 * 	- https://hyp.is/ykf9tPHREe6drgNjikNkyQ/newbedev.com/how-to-sort-array-of-struct-type-in-spark-dataframe-by-particular-column
			 * 	- https://stackoverflow.com/questions/49671354/how-to-sort-array-of-struct-type-in-spark-dataframe-by-particular-field
			 * 	- https://stackoverflow.com/questions/59999974/scala-spark-udf-filter-array-of-struct
			 * 	- https://stackoverflow.com/questions/47507767/sort-array-of-structs-in-spark-dataframe?rq=3
			 * 	- https://stackoverflow.com/questions/59901941/spark-udf-to-custom-sort-array-of-structs?rq=3
			 * 	- https://stackoverflow.com/questions/38739124/how-to-sort-arrayrow-by-given-column-index-in-scala?rq=3
			 */

			describe("sorting using: udf + sortBy on property + Seq[Row] as argument to udf, converting from Row to Class as intermediate step in udf") {


				// SOURCE: https://stackoverflow.com/questions/59901941/spark-udf-to-custom-sort-array-of-structs?rq=3
				it("(stackoverflow example)"){


					// WARNING: never include the data declaration here because the udf will give error "No TypeTag available for Score"
					// SOURCE = https://intellipaat.com/community/18751/scala-spark-app-with-no-typetag-available-error-in-def-main-style-app
					// case class Score(id: Int, num: Int)

					// -----------------------------------------
					// Way 1: creating this data set and grouping by id

					val inputDf: DataFrame = Seq((1, 2, 1), (1, 3, -3), (1, 4, 2)).toDF("id1", "id2", "num")

					val tempDf: DataFrame = (inputDf
						.groupBy(col("id1"))
						.agg((collect_set(struct(col("id2"), col("num")))).as("scoreList")))

					// -----------------------------------------
					// Way 2: creating this data set and showing manually how to input data so result looks like the result after grouping (tempDf)
					val data: Seq[Row] = Seq(
						Row(1, Array(Row(2, 1), Row(3, -3), Row(4, 2))),
					)
					val innerSchema: StructType = new StructType().add("id2", IntegerType).add("num", IntegerType)
					val fullSchema: StructType = new StructType().add("id1", IntegerType).add("scoreList", ArrayType(innerSchema))

					val df: DataFrame = sparkSessionWrapper.createDataFrame(sparkSessionWrapper.sparkContext.parallelize(data), fullSchema)

					// ------------------------------------------

					// TODO temp and df are interchangeable

					val funcGivenSeqRowSortToScores: Seq[Row] => Score = (lst: Seq[Row]) => {
						lst.map { case Row(n: Int, age: Int) => Score(n, age) }.minBy(_.num)
					}

					val udfSortScoreList: UserDefinedFunction = udf(funcGivenSeqRowSortToScores(_: Seq[Row]): Score)

					val resultSeqRowUdfDf: DataFrame = (df
						.select(col("id1"), udfSortScoreList(col("scoreList")).as("result"))
						.select(col("id1"), col("result.*")))

					resultSeqRowUdfDf.collectAll shouldEqual Seq(Row(1, Row(3, -3)))

				}


				it("(person example)"){

					val funcGivenSeqRowSortPersons: Seq[Row] => Seq[PersonStruct] = (lst: Seq[Row]) => {
						lst.map { case Row(id: Int, name: String, mid: String, addr: String, age: Int) => PersonStruct(id, name, mid, addr, age) }
							.sortBy((p: PersonStruct) => (p.middleInitialThrice, p.name, p.id))
					}
					val udfSortPersons: UserDefinedFunction = udf(funcGivenSeqRowSortPersons(_: Seq[Row]): Seq[PersonStruct])

					val resultUdfSortByPropertyUsingSeqRowToClass: Seq[SortStruct[PersonStruct]] = (personDf
						.select(col("groupingKey"), udfSortPersons(col("yourArray")).as("sorted"))
						.as[SortStruct[PersonStruct]]
						.collect().toSeq)

					expectedUdfSortByPropertyUsingSeqRowToClass shouldEqual resultUdfSortByPropertyUsingSeqRowToClass
					expectedUdfSortByPropertyUsingSeqRowToClass shouldEqual expectedUdfSortByPropertyUsingSeqAccessRowToClass

				}
			}






			// TESTING: udf + sortBy on property + class/record/dataset
			/**
			 * SOURCES: (Filtered feature)
			 * 	- https://stackoverflow.com/questions/59999974/scala-spark-udf-filter-array-of-struct
			 */
			// NOTE: this method keeps the groupingKey even without explicitly including it
			it("sorting using: class/dataset + udf + sortBy on property") {


				def funcGivenSeqRowAccessThenSortPersons(seq: Seq[Row]): Seq[PersonStruct] = (seq
					.sortBy((row: Row) => (row.getAs[String]("middleInitialThrice"), row.getAs[String]("name"), row.getAs[Int]("id")))
					.map(row => PersonStruct(
						row.getInt(0),
						row.getString(1),
						row.getString(2),
						row.getString(3),
						row.getInt(4)))
					)

				val udfSortPersons: UserDefinedFunction = udf(funcGivenSeqRowAccessThenSortPersons(_: Seq[Row]): Seq[PersonStruct])

				val resultUdfSortByPropertyUsingSeqAccessRowToClass: Seq[SortStruct[PersonStruct]] = (personDf
					.withColumn("sorted", udfSortPersons(col("yourArray")))
					.drop("yourArray")
					.as[SortStruct[PersonStruct]]
					.collect().toSeq)

				expectedUdfSortByPropertyUsingSeqAccessRowToClass shouldEqual resultUdfSortByPropertyUsingSeqAccessRowToClass

			}



			// TESTING: using sort + class/object/dataset/rdd property way

			/**
			 * SOURCES: (YourStruct)
			 * 	- https://stackoverflow.com/questions/54954732/spark-scala-filter-array-of-structs-without-explode
			 * 	- https://stackoverflow.com/questions/28543510/spark-sort-records-in-groups
			 * 	- https://stackoverflow.com/questions/62218496/how-to-convert-a-dataframe-map-column-to-a-struct-column/62218822#62218822
			 */
			it("sorting using: class/dataset + sortBy on property") {

				import sparkSessionWrapper.sqlContext.implicits._

				val resultSortByPropertyOnClass: Dataset[(String, Seq[PersonStruct])] = (personDs
					.map((record: Record) => (record.groupingKey, record.yourArray.sortBy((p: PersonStruct) => p.middleInitialThrice))))

				expectedSortByPropertyOnClass shouldEqual resultSortByPropertyOnClass
			}



			// TESTING: class/dataset + sortBy on property + groupByKey, mapGroups
			/**
			 * SOURCES: (Record)
			 * 	- https://hyp.is/HKPEUvLhEe6w6uuMg43GDQ/newbedev.com/how-to-sort-array-of-struct-type-in-spark-dataframe-by-particular-column
			 */
			it("sorting using: class/dataset + groupByKey, mapGroups + sortBy on property") {

				val resultGroupByKeySortByPropertyOnClass_1: Dataset[(String, Seq[(Int, String, String, String, Int)])] = personRecDs.groupByKey(_.groupingKey).mapGroups((groupKey: String, objs: Iterator[RecordRaw]) => (groupKey, objs.toSeq.flatMap(_.yourArray.sortBy(_._3))))

				val resultGroupByKeySortByPropertyOnClass_2: Dataset[(String, Seq[PersonStruct])] = personDs.groupByKey(_.groupingKey).mapGroups((groupKey: String, objs: Iterator[Record]) => (groupKey, objs.toSeq.flatMap(_.yourArray.sortBy(_.middleInitialThrice))))

				resultGroupByKeySortByPropertyOnClass_1.map(tup => tup._2.map(p => PersonStruct(p._1, p._2, p._3, p._4, p._5))) shouldEqual resultGroupByKeySortByPropertyOnClass_2
			}



			it("array_sort: simple, sorts array in ascending order") {



				// TODO left off here - research this one find model

			}
		}


		it("array_zip") {

		}
		it("array_overlap") {

		}
	}

}
