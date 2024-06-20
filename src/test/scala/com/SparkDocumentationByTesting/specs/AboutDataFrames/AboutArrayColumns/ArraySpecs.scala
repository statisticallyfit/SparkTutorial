package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutArrayColumns

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
//import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions._
import utilities.DFUtils
import utilities.DFUtils.TypeAbstractions._
import utilities.DFUtils.implicits._
import utilities.DataHub.ManualDataFrames.ArrayDf._
import utilities.DataHub.ManualDataFrames.fromEnums.TradeDf._
import utilities.DataHub.ManualDataFrames.fromEnums._
import utilities.EnumHub.Animal.Bear._
import utilities.EnumHub.Animal.Bird.Eagle._
import utilities.EnumHub.Animal.Bird._
import utilities.EnumHub.Animal.Camelid._
import utilities.EnumHub.Animal.Canine.WildCanine._
import utilities.EnumHub.Animal.Canine._
import utilities.EnumHub.Animal.Cat.WildCat._
import utilities.EnumHub.Animal.Cat._
import utilities.EnumHub.Animal.Deer._
import utilities.EnumHub.Animal.Equine.Horse._
import utilities.EnumHub.Animal.Equine._
import utilities.EnumHub.Animal.Insect._
import utilities.EnumHub.Animal.Monkey.Ape._
import utilities.EnumHub.Animal.Monkey._
import utilities.EnumHub.Animal.Reptile._
import utilities.EnumHub.Animal.Rodent.Squirrel._
import utilities.EnumHub.Animal.Rodent._
import utilities.EnumHub.Animal.SeaCreature._
import utilities.EnumHub.Animal.WeaselMustelid._
import utilities.EnumHub.Animal._
import utilities.EnumHub._
import utilities.EnumUtils.implicits._
import utilities.GeneralMainUtils.Helpers._
import utilities.GeneralMainUtils.implicits._

import scala.Double.NaN
// TODO update with new animals made

import utilities.EnumHub.Instrument._
import utilities.EnumHub.World.SouthAmerica._
import utilities.EnumHub.World._

//import com.SparkSessionForTests
import com.SparkDocumentationByTesting.CustomMatchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
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


		describe("array_contains") {


			import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._


			describe("array_contains: returns BOOL, answers whether a particular element is within the array-column") {

				it("array_contains (property): answers strict equality") {

					val containsBearDf: DataFrame = animalArrayDf.withColumn("ContainsResult", array_contains(col("ArrayAnimal"), Bear.enumName))
					val numBear: Int = containsBearDf.select("ContainsResult").collectCol[Boolean].count(_ == true)

					numBear shouldEqual 1
				}

				it("udf method (property): can check kind-of relationship between elements, not just equality, like array_contains()") {

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
		}


		describe("array_distinct") {

			it("array_distinct (property): returns only distinct values within the array column") {


				import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._

				val distinctDf: DataFrame = animalArrayDf.withColumn("ArrayDistinct", array_distinct(col("ArrayAnimal")))

				val actualNumDistinct: Seq[Int] = distinctDf.select("ArrayDistinct").collectSeqCol[Animal].map(_.length)
				val actualNumOriginal: Seq[Int] = animalArrayDf.select("ArrayAnimal").collectSeqCol[Animal].map(_.length)
				val expectedNumDistinct: Seq[Int] = animalArrayDf.select("ArrayAnimal").collectSeqCol[Animal].map(_.distinct.length)

				actualNumDistinct shouldEqual expectedNumDistinct

				val originalSizesMinusDistinctSizes: Seq[Int] = actualNumOriginal.zip(actualNumDistinct).map { case (originalSize, distinctSize) => originalSize - distinctSize }

				originalSizesMinusDistinctSizes.exists(_ > 0) shouldEqual true // meaningt he distinct took out some duplicates
			}
		}

		describe("array_except"){

			it("array_except (property): returns elements from first array that are not in the second array (like set subtract)") {
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
		}


		describe("array_join"){

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
		}



		describe("array_max, array_min"){

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
		}




		describe("array_position"){

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
		}


		describe("array_remove") {

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

			it("udf, in comparison, can remove all instances of a given element") {

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


		describe("array_repeat"){

			describe("array_repeat: repeats the given element the specified number of times") {

				it("array_repeat (property): can repeat an integer number of items") {

					val repeatByIntDf: DataFrame = tradeDf.withColumn("RepeatInstrument", array_repeat(col(FinancialInstrument.enumName), 3))

					repeatByIntDf.select("RepeatInstrument").collectSeqCol[String].forall(_.length == 3) shouldEqual true

				}

				it("array_repeat (property): can repeat an element a number of times defined by the right Column argument") {

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
		}


		describe("array_union"){

			import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._

			val resultUnionDf: DataFrame = animalsTwoColDf.withColumn("result", array_union(col("Group1"), col("Group2")))


			it("union - the arrays from the two columns are overlapped"){
				val groupCol1: Seq[Seq[Animal]] = animalsTwoColDf.select("Group1").collectSeqEnumCol[Animal]
				val groupCol2: Seq[Seq[Animal]] = animalsTwoColDf.select("Group2").collectSeqEnumCol[Animal]

				// Create the check for union of two columns, manual way
				val checkUnionCol: Seq[List[Animal]] = groupCol1.zip(groupCol2).map{ case (lst1, lst2) => lst1.toSet.union(lst2.toSet).toList }

				// Retrieve the calculated result union of two columns
				val resultUnionCol: Seq[Seq[Animal]] = resultUnionDf.select("result").collectSeqEnumCol[Animal]

				// TODO weird - comparison of out-of-order elements only works when in string format AND ordered - why??
				(checkUnionCol.zip(resultUnionCol) // NOTE: making explicit the need to sort + stringify
					.map{ case (c, r) => (c.enumNames.sorted, r.enumNames.sorted)}
					.forall{ case (cs, rs) => cs == rs} )
			}

			it("union - the length of the resulting array is equal to the lengths of the original arrays minus the length of what is common between them"){

				// Lengths of the spark-calculated union between two columns
				val resultUnionCol: Seq[Seq[Animal]] = resultUnionDf.select("result").collectSeqCol[Animal]
				val lenResult: Seq[Int] = resultUnionCol.map(_.length)


				// Lengths of the original arrays
				val groupCol1: Seq[Seq[Animal]] = animalsTwoColDf.select("Group1").collectSeqEnumCol[Animal]
				val lenGroup1: Seq[Int] = groupCol1.map(_.length)
				val groupCol2: Seq[Seq[Animal]] = animalsTwoColDf.select("Group2").collectSeqEnumCol[Animal]
				val lenGroup2: Seq[Int] = groupCol2.map(_.length)

				// Length of the manually-calculated union between the two columns
				val checkUnionCol: Seq[List[Animal]] = groupCol1.zip(groupCol2).map{ case (lst1, lst2) => lst1.toSet.union(lst2.toSet).toList }
				val lenCheckUnion: Seq[Int] = checkUnionCol.map(_.length)

				// Length of the common array (common between the original arrays)
				val commonsCol: Seq[Seq[Animal]] = groupCol1.zip(groupCol2).map{ case (lst1, lst2) => lst1.toSet.intersect(lst2.toSet).toList }
				val lenCommons: Seq[Int] = commonsCol.map(_.length)


				// Assertions
				lenResult shouldEqual lenCheckUnion

				lenResult should equal (lenGroup1.zip(lenGroup2).map{ case (ng1, ng2) => ng1 + ng2}.zip(lenCommons).map{case (ntotal, ncommon) => ntotal - ncommon})
			}
		}


		describe("array_intersect") {

			import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._

			val resultIntersectDf: DataFrame = animalsTwoColDf.withColumn("result", array_intersect(col("Group1"), col("Group2")))


			it("intersect - only the common elements between the array columns are kept") {
				val groupCol1: Seq[Seq[Animal]] = animalsTwoColDf.select("Group1").collectSeqEnumCol[Animal]
				val groupCol2: Seq[Seq[Animal]] = animalsTwoColDf.select("Group2").collectSeqEnumCol[Animal]

				// Create the check for union of two columns, manual way
				val checkIntersectCol: Seq[List[Animal]] = groupCol1.zip(groupCol2).map { case (lst1, lst2) => lst1.toSet.intersect(lst2.toSet).toList }

				// Retrieve the calculated result union of two columns
				val resultIntersectCol: Seq[Seq[Animal]] = resultIntersectDf.select("result").collectSeqEnumCol[Animal]

				// TODO weird - comparison of out-of-order elements only works when in string format AND ordered - why??
				(checkIntersectCol.zip(resultIntersectCol) // NOTE: making explicit the need to sort + stringify
					.map { case (c, r) => (c.enumNames.sorted, r.enumNames.sorted) }
					.forall { case (cs, rs) => cs == rs })
			}
		}



		describe("arrays_overlap"){

			it("arrays_overlap: boolean function, checks if at least one element is common between the two array columns. " +
				"Returns true if at least one element is common in both arrays. " +
				"Returns false otherwise. " +
				"Returns null if at least one of the arrays is null. "){


			}
		}



		it("array_zip") {

		}
		it("array_overlap") {

		}
	}

}
