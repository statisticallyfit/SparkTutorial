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
		val udfdf2: DataFrame = (animalDf.select(udfToParentCountry(col(World.enumName)).alias("ParentLocation"), col(Animal.enumName), col(ClimateZone.enumName), col(Biome.enumName)))

		// Effort at trying to make climate conditional on parentlocation
		val parentCADf: Dataset[Row] = (udfdf2.groupBy("ParentLocation")
			.agg(array_distinct(collect_list(col(ClimateZone.enumName))).as("ArrayClimate"),
				array_distinct(collect_list(col(Animal.enumName))).as("ArrayAnimalPC"))
			.filter(!col("ParentLocation").isInCollection(Seq("null"))))



		// ----------------------------------------------------------------------------------------------

		import utilities.DataHub.ManualDataFrames.ArrayDf._
		import personInfo._


		// Names after sorting first time on middle initial
		val expectedNames_afterMid: Seq[Seq[String]] = Seq(
			List(Naza, Nesryn, Niki, Nicole, Nanette, Nina),
			List(Hazel, Harriet, Henry, Harry, Hannah),
			List(Katerina, Catherine, Dmitry, Vesper, Yigor, Tyler, Tijah, Tatiana),
			List(Blake, Brianna, Bonnie, Berenice, Bridget, Bella, Xenia, Natalia, Penelope, Pauline),
			List(Liliana, Helen, Astrid, Amber, Hugo, Jasper, Victor, Quan, Quinn),
			List(Sascha, Selene, Sarah, Sophie, Stacey, Sabrielle, Sabrina, Sigurd)
		)


		// MiddleInitial after sorting first time on middle initial
		val expectedMiddles_afterMid: Seq[Seq[String]] = Seq(
			List(nnn, nnn, nnn, nnn, nnn, nnn),
			List(hhh, hhh, hhh, hhh, hhh),
			List(iii, iii, kkk, kkk, vvv, vvv, vvv, vvv),
			List(bbb, bbb, bbb, bbb, bbb, bbb, eee, nnn, ppp, ppp),
			List(ddd, ggg, jjj, jjj, xxx, xxx, yyy, zzz, zzz),
			List(ooo, ooo, ooo, ooo, ooo, ooo, ooo, ooo)
		)

		// Names after sorting first on Middle then on Name
		val expectedNames_afterMidThenName: Seq[Seq[String]] = Seq(
			List(Nanette, Naza, Nesryn, Nicole, Niki, Nina),
			List(Hannah, Harriet, Harry, Hazel, Henry),
			List(Catherine, Dmitry, Katerina, Tatiana, Tijah, Tyler, Vesper, Yigor),
			List(Bella, Berenice, Blake, Bonnie, Brianna, Bridget, Natalia, Pauline, Penelope, Xenia),
			List(Amber, Astrid, Helen, Hugo, Jasper, Liliana, Quan, Quinn, Victor),
			List(Sabrielle, Sabrina, Sarah, Sascha, Selene, Sigurd, Sophie, Stacey)
		)

		// ID's after sorting first by mid, then name, then id
		val expectedNames_afterMidThenNameAge: Seq[Seq[String]] = Seq(
			List(Nina, Nanette, Naza, Nesryn, Nicole, Niki),
			List(Hannah, Harriet, Harry, Hazel, Henry),
			List(Tatiana, Tijah, Tyler, Yigor, Katerina, Dmitry, Vesper, Catherine),
			List(Xenia, Natalia, Bella, Berenice, Blake, Bonnie, Brianna, Bridget, Pauline, Penelope),
			List(Amber, Quan, Quinn, Astrid, Jasper, Helen, Hugo, Liliana, Victor),
			List(Stacey, Sigurd, Sophie, Sabrielle, Sabrina, Sarah, Sascha, Selene)
		)


		// Ages after sorting first by mid, then name, then id, then age
		val expectedNames_afterMidThenNameAgeID: Seq[Seq[String]] = Seq(
			List(Nina, Nanette, Naza, Nesryn, Nicole, Niki),
			List(Hannah, Harriet, Harry, Hazel, Henry),
			List(Yigor, Tatiana, Tyler, Katerina, Vesper, Tijah, Catherine, Dmitry),
			List(Natalia, Penelope, Xenia, Pauline, Bella, Berenice, Blake, Bonnie, Brianna, Bridget),
			List(Jasper, Victor, Quan, Helen, Hugo, Amber, Astrid, Liliana, Quinn),
			List(Sabrielle, Stacey, Sigurd, Sophie, Sabrina, Sarah, Sascha, Selene)
		)

		// ---------------------------------------------------------------------------------------------------

		val expectedSortArraySeq: Seq[SortByMidStruct[PersonMidIdNameAgeStruct]] = Seq(
			SortByMidStruct("n", List(
				PersonMidIdNameAgeStruct(nnn, 1, Nanette, 15),
				PersonMidIdNameAgeStruct(nnn, 1, Naza, 15),
				PersonMidIdNameAgeStruct(nnn, 1, Nesryn, 15),
				PersonMidIdNameAgeStruct(nnn, 1, Nicole, 15),
				PersonMidIdNameAgeStruct(nnn, 1, Niki, 15),
				PersonMidIdNameAgeStruct(nnn, 1, Nina, 15))
			),
			SortByMidStruct("h", List(
				PersonMidIdNameAgeStruct(hhh, 5, Hannah, 5),
				PersonMidIdNameAgeStruct(hhh, 5, Harriet, 5),
				PersonMidIdNameAgeStruct(hhh, 5, Harry, 5),
				PersonMidIdNameAgeStruct(hhh, 5, Hazel, 5),
				PersonMidIdNameAgeStruct(hhh, 5, Henry, 5))
			),
			SortByMidStruct("c", List(
				PersonMidIdNameAgeStruct(iii, 4, Katerina, 19),
				PersonMidIdNameAgeStruct(iii, 17, Catherine, 90),
				PersonMidIdNameAgeStruct(kkk, 9, Vesper, 25),
				PersonMidIdNameAgeStruct(kkk, 34, Dmitry, 23),
				PersonMidIdNameAgeStruct(vvv, 1, Yigor, 11),
				PersonMidIdNameAgeStruct(vvv, 3, Tatiana, 10),
				PersonMidIdNameAgeStruct(vvv, 3, Tyler, 10),
				PersonMidIdNameAgeStruct(vvv, 10, Tijah, 10))
			),
			SortByMidStruct("b", List(
				PersonMidIdNameAgeStruct(bbb, 9, Bella, 19),
				PersonMidIdNameAgeStruct(bbb, 9, Berenice, 19),
				PersonMidIdNameAgeStruct(bbb, 9, Blake, 19),
				PersonMidIdNameAgeStruct(bbb, 9, Bonnie, 19),
				PersonMidIdNameAgeStruct(bbb, 9, Brianna, 19),
				PersonMidIdNameAgeStruct(bbb, 9, Bridget, 19),
				PersonMidIdNameAgeStruct(eee, 4, Xenia, 9),
				PersonMidIdNameAgeStruct(nnn, 1, Natalia, 15),
				PersonMidIdNameAgeStruct(ppp, 1, Penelope, 52),
				PersonMidIdNameAgeStruct(ppp, 5, Pauline, 52))
			),
			SortByMidStruct("a", List(
				PersonMidIdNameAgeStruct(ddd, 8, Liliana, 40),
				PersonMidIdNameAgeStruct(ggg, 3, Helen, 30),
				PersonMidIdNameAgeStruct(jjj, 7, Amber, 11),
				PersonMidIdNameAgeStruct(jjj, 7, Astrid, 12),
				PersonMidIdNameAgeStruct(xxx, 1, Jasper, 27),
				PersonMidIdNameAgeStruct(xxx, 3, Hugo, 30),
				PersonMidIdNameAgeStruct(yyy, 2, Victor, 45),
				PersonMidIdNameAgeStruct(zzz, 3, Quan, 11),
				PersonMidIdNameAgeStruct(zzz, 10, Quinn, 11))
			),
			SortByMidStruct("s", List(
				PersonMidIdNameAgeStruct(ooo, 14, Sabrielle, 22),
				PersonMidIdNameAgeStruct(ooo, 20, Sabrina, 22),
				PersonMidIdNameAgeStruct(ooo, 20, Sarah, 22),
				PersonMidIdNameAgeStruct(ooo, 20, Sascha, 22),
				PersonMidIdNameAgeStruct(ooo, 20, Selene, 22),
				PersonMidIdNameAgeStruct(ooo, 20, Sigurd, 21),
				PersonMidIdNameAgeStruct(ooo, 20, Sophie, 21),
				PersonMidIdNameAgeStruct(ooo, 20, Stacey, 14)))
		)

		// ------------------------------------------------------------------------------------------------

		val expectedUdfComparatorMidSortSeq: Seq[SortByMidStruct[PersonStruct]] = Seq(
			SortByMidStruct("n", List(
				PersonStruct(1, Naza, nnn, "131", 15),
				PersonStruct(1, Nesryn, nnn, "128", 15),
				PersonStruct(1, Niki, nnn, "155", 15),
				PersonStruct(1, Nicole, nnn, "154", 15),
				PersonStruct(1, Nanette, nnn, "152", 13),
				PersonStruct(1, Nina, nnn, "140", 12))),

			SortByMidStruct("h", List(
				PersonStruct(5, Hazel, hhh, "143", 5),
				PersonStruct(5, Harriet, hhh, "142", 5),
				PersonStruct(5, Henry, hhh, "111", 5),
				PersonStruct(5, Harry, hhh, "992", 5),
				PersonStruct(5, Hannah, hhh, "934", 5))),

			SortByMidStruct("c", List(
				PersonStruct(4, Katerina, iii, "19", 19),
				PersonStruct(17, Catherine, iii, "138", 90),
				PersonStruct(34, Dmitry, kkk, "787", 23),
				PersonStruct(9, Vesper, kkk, "348", 25),
				PersonStruct(1, Yigor, vvv, "34", 11),
				PersonStruct(3, Tyler, vvv, "111", 10),
				PersonStruct(10, Tijah, vvv, "0", 10),
				PersonStruct(3, Tatiana, vvv, "123", 10))),

			SortByMidStruct("b", List(
				PersonStruct(9, Blake, bbb, "445", 19),
				PersonStruct(9, Brianna, bbb, "442", 19),
				PersonStruct(9, Bonnie, bbb, "441", 19),
				PersonStruct(9, Berenice, bbb, "430", 19),
				PersonStruct(9, Bridget, bbb, "412", 19),
				PersonStruct(9, Bella, bbb, "417", 19),
				PersonStruct(4, Xenia, eee, "13", 9),
				PersonStruct(1, Natalia, nnn, "678", 15),
				PersonStruct(1, Penelope, ppp, "345", 52),
				PersonStruct(5, Pauline, ppp, "111", 52))),

			SortByMidStruct("a", List(
				PersonStruct(8, Liliana, ddd, "332", 40),
				PersonStruct(3, Helen, ggg, "191", 30),
				PersonStruct(7, Astrid, jjj, "555", 12),
				PersonStruct(7, Amber, jjj, "443", 11),
				PersonStruct(3, Hugo, xxx, "324", 30),
				PersonStruct(1, Jasper, xxx, "1", 27),
				PersonStruct(2, Victor, yyy, "223", 45),
				PersonStruct(3, Quan, zzz, "345", 11),
				PersonStruct(10, Quinn, zzz, "345", 11))),

			SortByMidStruct("s", List(
				PersonStruct(20, Sascha, ooo, "112", 22),
				PersonStruct(20, Selene, ooo, "134", 22),
				PersonStruct(20, Sarah, ooo, "122", 22),
				PersonStruct(20, Sophie, ooo, "156", 21),
				PersonStruct(20, Stacey, ooo, "189", 14),
				PersonStruct(14, Sabrielle, ooo, "444", 22),
				PersonStruct(20, Sabrina, ooo, "433", 22),
				PersonStruct(20, Sigurd, ooo, "332", 21)))

		)

		// ----------------------------------------------------------------------------------------------------------------

		val expectedArraySortTransformMapMidSort: Seq[SortByMidStruct[PersonStruct]] = Seq(
			SortByMidStruct("c", List(
				PersonStruct(17, Catherine, ccc, "138", 90),
				PersonStruct(34, Dmitry, fff, "787", 23),
				PersonStruct(9, Vesper, hhh, "348", 25),
				PersonStruct(4, Katerina, iii, "19", 19),
				PersonStruct(3, Tatiana, mmm, "123", 10),
				PersonStruct(1, Yigor, ooo, "34", 11),
				PersonStruct(3, Tyler, qqq, "111", 10),
				PersonStruct(10, Tijah, vvv, "0", 10))),

			SortByMidStruct("b", List(
				PersonStruct(4, Xenia, bbb, "13", 9),
				PersonStruct(1, Natalia, eee, "678", 15),
				PersonStruct(1, Naza, nnn, "131", 15),
				PersonStruct(1, Penelope, ppp, "345", 52),
				PersonStruct(1, Nesryn, rrr, "128", 15),
				PersonStruct(5, Pauline, ttt, "111", 52))),

			SortByMidStruct("a", List(
				PersonStruct(7, Astrid, aaa, "555", 12),
				PersonStruct(8, Liliana, ddd, "332", 40),
				PersonStruct(3, Helen, ggg, "191", 30),
				PersonStruct(7, Amber, jjj, "443", 11),
				PersonStruct(10, Quinn, kkk, "345", 11),
				PersonStruct(3, Hugo, lll, "324", 30),
				PersonStruct(1, Jasper, xxx, "1", 27),
				PersonStruct(2, Victor, yyy, "223", 45),
				PersonStruct(3, Quan, zzz, "345", 11)))

		)

		// ----------------------------------------------------------------------------------------------------------------

		val expectedExplodeMultiSortTups: Seq[SortStruct[PersonStruct]] = Seq(
			SortStruct("a", List(
				PersonStruct(8, Liliana, ddd, "332", 40),
				PersonStruct(3, Helen, ggg, "191", 30),
				PersonStruct(7, Amber, jjj, "443", 11),
				PersonStruct(7, Astrid, jjj, "555", 12),
				PersonStruct(1, Jasper, xxx, "1", 27),
				PersonStruct(3, Hugo, xxx, "324", 30),
				PersonStruct(2, Victor, yyy, "223", 45),
				PersonStruct(3, Quan, zzz, "345", 11),
				PersonStruct(10, Quinn, zzz, "345", 11))
			),
			SortStruct("b", List(
				PersonStruct(9, Bridget, bbb, "412", 19),
				PersonStruct(9, Bella, bbb, "417", 19),
				PersonStruct(9, Berenice, bbb, "430", 19),
				PersonStruct(9, Bonnie, bbb, "441", 19),
				PersonStruct(9, Brianna, bbb, "442", 19),
				PersonStruct(9, Blake, bbb, "445", 19),
				PersonStruct(4, Xenia, eee, "13", 9),
				PersonStruct(1, Natalia, nnn, "678", 15),
				PersonStruct(1, Penelope, ppp, "345", 52),
				PersonStruct(5, Pauline, ppp, "111", 52))
			),
			SortStruct("c", List(
				PersonStruct(4, Katerina, iii, "19", 19),
				PersonStruct(17, Catherine, iii, "138", 90),
				PersonStruct(9, Vesper, kkk, "348", 25),
				PersonStruct(34, Dmitry, kkk, "787", 23),
				PersonStruct(1, Yigor, vvv, "34", 11),
				PersonStruct(3, Tyler, vvv, "111", 10),
				PersonStruct(3, Tatiana, vvv, "123", 10),
				PersonStruct(10, Tijah, vvv, "0", 10))
			) ,
			SortStruct("h", List(
				PersonStruct(5, Henry, hhh, "111", 5),
				PersonStruct(5, Harriet, hhh, "142", 5),
				PersonStruct(5, Hazel, hhh, "143", 5),
				PersonStruct(5, Hannah, hhh, "934", 5),
				PersonStruct(5, Harry, hhh, "992", 5))
			),
			SortStruct("n", List(
				PersonStruct(1, Nesryn, nnn, "128", 15),
				PersonStruct(1, Naza, nnn, "131", 15),
				PersonStruct(1, Nina, nnn, "140", 15),
				PersonStruct(1, Nanette, nnn, "152", 15),
				PersonStruct(1, Nicole, nnn, "154", 15),
				PersonStruct(1, Niki, nnn, "155", 15))
			),
			SortStruct("s", List(
				PersonStruct(14, Sabrielle, ooo, "444", 22),
				PersonStruct(20, Stacey, ooo, "189", 14),
				PersonStruct(20, Sophie, ooo, "156", 21),
				PersonStruct(20, Sigurd, ooo, "332", 21),
				PersonStruct(20, Sascha, ooo, "112", 22),
				PersonStruct(20, Sarah, ooo, "122", 22),
				PersonStruct(20, Selene, ooo, "134", 22),
				PersonStruct(20, Sabrina, ooo, "433", 22))
			)
		)

		// -------------------------------------------------------------------------------------------

		val expectedExplodeSortTups: Array[List[(String, Int)]] = Array(
			List((nnn, 1), (nnn, 1), (nnn, 1), (nnn, 1), (nnn, 1), (nnn, 1)),
			List((hhh, 5), (hhh, 5), (hhh, 5), (hhh, 5), (hhh, 5)),
			List((iii, 4), (iii, 17), (kkk, 9), (kkk, 34), (vvv, 1), (vvv, 3), (vvv, 3), (vvv, 10)),
			List((bbb, 9), (bbb, 9), (bbb, 9), (bbb, 9), (bbb, 9), (bbb, 9), (eee, 4), (nnn, 1), (ppp, 1), (ppp, 5)),
			List((ddd, 8), (ggg, 3), (jjj, 7), (jjj, 7), (xxx, 1), (xxx, 3), (yyy, 2), (zzz, 3), (zzz, 10)),
			List((ooo, 14), (ooo, 20), (ooo, 20), (ooo, 20), (ooo, 20), (ooo, 20), (ooo, 20), (ooo, 20))
		)


		// -------------------------------------------------------------------------------------------

		val expectedExplodeArraySortTups: Seq[List[(String, String, Int)]] = Seq(
			List((nnn, Nanette, 1), (nnn, Naza, 1), (nnn, Nesryn, 1), (nnn, Nicole, 1), (nnn, Niki, 1), (nnn, Nina, 1)),
			List((hhh, Hannah, 5), (hhh, Harriet, 5), (hhh, Harry, 5), (hhh, Hazel, 5), (hhh, Henry, 5)),
			List((iii, Catherine, 17), (iii, Katerina, 4), (kkk, Dmitry, 34), (kkk, Vesper, 9), (vvv, Tatiana, 3), (vvv, Tijah, 10), (vvv, Tyler, 3), (vvv, Yigor, 1)),
			List((bbb, Bella, 9), (bbb, Berenice, 9), (bbb, Blake, 9), (bbb, Bonnie, 9), (bbb, Brianna, 9), (bbb, Bridget, 9), (eee, Xenia, 4), (nnn, Natalia, 1), (ppp, Pauline, 5), (ppp, Penelope, 1)),
			List((ddd, Liliana, 8), (ggg, Helen, 3), (jjj, Amber, 7), (jjj, Astrid, 7), (xxx, Hugo, 3), (xxx, Jasper, 1), (yyy, Victor, 2), (zzz, Quan, 3), (zzz, Quinn, 10)),
			List((ooo, Sabrielle, 14), (ooo, Sabrina, 20), (ooo, Sarah, 20), (ooo, Sascha, 20), (ooo, Selene, 20), (ooo, Sigurd, 20), (ooo, Sophie, 20), (ooo, Stacey, 20))
		)


		// -------------------------------------------------------------------------------------------


		val expectedUdfSortByPropertyUsingSeqRowToClass: Seq[SortStruct[PersonStruct]] = Seq(
			SortStruct("a", List(
				PersonStruct(8, Liliana, ddd, "332", 40),
				PersonStruct(3, Helen, ggg, "191", 30),
				PersonStruct(7, Amber, jjj, "443", 11),
				PersonStruct(7, Astrid, jjj, "555", 12),
				PersonStruct(3, Hugo, xxx, "324", 30),
				PersonStruct(1, Jasper, xxx, "1", 27),
				PersonStruct(2, Victor, yyy, "223", 45),
				PersonStruct(3, Quan, zzz, "345", 11),
				PersonStruct(10, Quinn, zzz, "345", 11))
			),
			SortStruct("b", List(
				PersonStruct(9, Bella, bbb, "417", 19),
				PersonStruct(9, Berenice, bbb, "430", 19),
				PersonStruct(9, Blake, bbb, "445", 19),
				PersonStruct(9, Bonnie, bbb, "441", 19),
				PersonStruct(9, Brianna, bbb, "442", 19),
				PersonStruct(9, Bridget, bbb, "412", 19),
				PersonStruct(4, Xenia, eee, "13", 9),
				PersonStruct(1, Natalia, nnn, "678", 15),
				PersonStruct(5, Pauline, ppp, "111", 52),
				PersonStruct(1, Penelope, ppp, "345", 52))
			),
			SortStruct("c", List(
				PersonStruct(17, Catherine, iii, "138", 90),
				PersonStruct(4, Katerina, iii, "19", 19),
				PersonStruct(34, Dmitry, kkk, "787", 23),
				PersonStruct(9, Vesper, kkk, "348", 25),
				PersonStruct(3, Tatiana, vvv, "123", 10),
				PersonStruct(10, Tijah, vvv, "0", 10),
				PersonStruct(3, Tyler, vvv, "111", 10),
				PersonStruct(1, Yigor, vvv, "34", 11))
			),
			SortStruct("n", List(
				PersonStruct(1, Nanette, nnn, "152", 15),
				PersonStruct(1, Naza, nnn, "131", 15),
				PersonStruct(1, Nesryn, nnn, "128", 15),
				PersonStruct(1, Nicole, nnn, "154", 15),
				PersonStruct(1, Niki, nnn, "155", 15),
				PersonStruct(1, Nina, nnn, "140", 15))
			),
			SortStruct("h", List(
				PersonStruct(5, Hannah, hhh, "934", 5),
				PersonStruct(5, Harriet, hhh, "142", 5),
				PersonStruct(5, Harry, hhh, "992", 5),
				PersonStruct(5, Hazel, hhh, "143", 5),
				PersonStruct(5, Henry, hhh, "111", 5))
			),
			SortStruct("s", List(
				PersonStruct(14, Sabrielle, ooo, "444", 22),
				PersonStruct(20, Sabrina, ooo, "433", 22),
				PersonStruct(20, Sarah, ooo, "122", 22),
				PersonStruct(20, Sascha, ooo, "112", 22),
				PersonStruct(20, Selene, ooo, "134", 22),
				PersonStruct(20, Sigurd, ooo, "332", 21),
				PersonStruct(20, Sophie, ooo, "156", 21),
				PersonStruct(20, Stacey, ooo, "189", 14))
			)
		)

		// -------------------------------------------------------------------------------------------


		val expectedUdfSortByPropertyUsingSeqAccessRowToClass: Seq[SortStruct[PersonStruct]] = Seq(
			SortStruct("a", List(
				PersonStruct(8, Liliana, ddd, "332", 40),
				PersonStruct(3, Helen, ggg, "191", 30),
				PersonStruct(7, Amber, jjj, "443", 11),
				PersonStruct(7, Astrid, jjj, "555", 12),
				PersonStruct(3, Hugo, xxx, "324", 30),
				PersonStruct(1, Jasper, xxx, "1", 27),
				PersonStruct(2, Victor, yyy, "223", 45),
				PersonStruct(3, Quan, zzz, "345", 11),
				PersonStruct(10, Quinn, zzz, "345", 11))
			),
			SortStruct("b", List(
				PersonStruct(9, Bella, bbb, "417", 19),
				PersonStruct(9, Berenice, bbb, "430", 19),
				PersonStruct(9, Blake, bbb, "445", 19),
				PersonStruct(9, Bonnie, bbb, "441", 19),
				PersonStruct(9, Brianna, bbb, "442", 19),
				PersonStruct(9, Bridget, bbb, "412", 19),
				PersonStruct(4, Xenia, eee, "13", 9),
				PersonStruct(1, Natalia, nnn, "678", 15),
				PersonStruct(5, Pauline, ppp, "111", 52),
				PersonStruct(1, Penelope, ppp, "345", 52))
			),
			SortStruct("c", List(
				PersonStruct(17, Catherine, iii, "138", 90),
				PersonStruct(4, Katerina, iii, "19", 19),
				PersonStruct(34, Dmitry, kkk, "787", 23),
				PersonStruct(9, Vesper, kkk, "348", 25),
				PersonStruct(3, Tatiana, vvv, "123", 10),
				PersonStruct(10, Tijah, vvv, "0", 10),
				PersonStruct(3, Tyler, vvv, "111", 10),
				PersonStruct(1, Yigor, vvv, "34", 11))
			),
			SortStruct("n", List(
				PersonStruct(1, Nanette, nnn, "152", 15),
				PersonStruct(1, Naza, nnn, "131", 15),
				PersonStruct(1, Nesryn, nnn, "128", 15),
				PersonStruct(1, Nicole, nnn, "154", 15),
				PersonStruct(1, Niki, nnn, "155", 15),
				PersonStruct(1, Nina, nnn, "140", 15))
			),
			SortStruct("h", List(
				PersonStruct(5, Hannah, hhh, "934", 5),
				PersonStruct(5, Harriet, hhh, "142", 5),
				PersonStruct(5, Harry, hhh, "992", 5),
				PersonStruct(5, Hazel, hhh, "143", 5),
				PersonStruct(5, Henry, hhh, "111", 5))
			),
			SortStruct("s", List(
				PersonStruct(14, Sabrielle, ooo, "444", 22),
				PersonStruct(20, Sabrina, ooo, "433", 22),
				PersonStruct(20, Sarah, ooo, "122", 22),
				PersonStruct(20, Sascha, ooo, "112", 22),
				PersonStruct(20, Selene, ooo, "134", 22),
				PersonStruct(20, Sigurd, ooo, "332", 21),
				PersonStruct(20, Sophie, ooo, "156", 21),
				PersonStruct(20, Stacey, ooo, "189", 14))
			)
		)

		// -------------------------------------------------------------------------------------------

		val expectedSortByPropertyOnClass: Seq[(String, List[PersonStruct])] = Seq(
			("a", List(
				PersonStruct(8, Liliana, ddd, "332", 40),
				PersonStruct(3, Helen, ggg, "191", 30),
				PersonStruct(7, Astrid, jjj, "555", 12),
				PersonStruct(7, Amber, jjj, "443", 11),
				PersonStruct(3, Hugo, xxx, "324", 30),
				PersonStruct(1, Jasper, xxx, "1", 27),
				PersonStruct(2, Victor, yyy, "223", 45),
				PersonStruct(3, Quan, zzz, "345", 11),
				PersonStruct(10, Quinn, zzz, "345", 11))
			),
			("b", List(
				PersonStruct(9, Blake, bbb, "445", 19),
				PersonStruct(9, Brianna, bbb, "442", 19),
				PersonStruct(9, Bonnie, bbb, "441", 19),
				PersonStruct(9, Berenice, bbb, "430", 19),
				PersonStruct(9, Bridget, bbb, "412", 19),
				PersonStruct(9, Bella, bbb, "417", 19),
				PersonStruct(4, Xenia, eee, "13", 9),
				PersonStruct(1, Natalia, nnn, "678", 15),
				PersonStruct(1, Penelope, ppp, "345", 52),
				PersonStruct(5, Pauline, ppp, "111", 52))
			),
			("c", List(
				PersonStruct(4, Katerina, iii, "19", 19),
				PersonStruct(17, Catherine, iii, "138", 90),
				PersonStruct(34, Dmitry, kkk, "787", 23),
				PersonStruct(9, Vesper, kkk, "348", 25),
				PersonStruct(1, Yigor, vvv, "34", 11),
				PersonStruct(3, Tyler, vvv, "111", 10),
				PersonStruct(10, Tijah, vvv, "0", 10),
				PersonStruct(3, Tatiana, vvv, "123", 10))
			),
			("n", List(PersonStruct(1, Naza, nnn, "131", 15),
				PersonStruct(1, Nesryn, nnn, "128", 15),
				PersonStruct(1, Niki, nnn, "155", 15),
				PersonStruct(1, Nicole, nnn, "154", 15),
				PersonStruct(1, Nanette, nnn, "152", 15),
				PersonStruct(1, Nina, nnn, "140", 15))
			),
			("h", List(PersonStruct(5, Hazel, hhh, "143", 5),
				PersonStruct(5, Harriet, hhh, "142", 5),
				PersonStruct(5, Henry, hhh, "111", 5),
				PersonStruct(5, Harry, hhh, "992", 5),
				PersonStruct(5, Hannah, hhh, "934", 5))
			),
			("s", List(
				PersonStruct(20, Sascha, ooo, "112", 22),
				PersonStruct(20, Selene, ooo, "134", 22),
				PersonStruct(20, Sarah, ooo, "122", 22),
				PersonStruct(20, Sophie, ooo, "156", 21),
				PersonStruct(20, Stacey, ooo, "189", 14),
				PersonStruct(14, Sabrielle, ooo, "444", 22),
				PersonStruct(20, Sabrina, ooo, "433", 22),
				PersonStruct(20, Sigurd, ooo, "332", 21))
			)
		)

		// -------------------------------------------------------------------------------------------



		// Data for union, intersect ...
		val animalsTwoColSeq: Seq[(Animal, List[Animal], List[Animal])] = Seq((Cat, List(Lion, Cheetah, Lynx, Bobcat, Tiger), List(Tiger, Panther, Lynx, Cougar, Puma, Ocelot)),
			(Canine, List(Poodle, Labrador, Wolf, Coyote, RedFox, GreyFox), List(GoldenRetriever, Fox, ArcticFox, Pomeranian, Wolf, Jackal, RedFox)),
			(Bird, List(Owl, Pelican, Vulture, Hawk, Flamingo, Bluejay, Crow, Falcon, Starling, Parrot), List(Robin, Swan, Chickadee, Woodpecker, Raven, Bluejay, Starling, Nightingale, Swallow, Magpie, Canary)),
			(Bear, List(BlackBear, GrizzlyBear), List(Koala, Panda)),
			(SeaCreature, List(Anemone, Marlin, AngelFish, Pearl, Oyster, Dolphin, Whale), List(Pearl, Dolphin, Clam, Coral, Shrimp, Flounder, Shark, Whale)),
			(Amphibian, List(BlueFrog, PoisonDartFrog, Newt, Toad), List(Toad, GlassFrog, RedFrog, Toad, BlueFrog)),
			(Rodent, List(Chinchilla, Squirrel, BrownSquirrel, Chipmunk, Groundhog, Marmot), List(Beaver, Procupine, Mouse, Groundhog, Marmot, Chipmunk)))

		val animalsTwoColStrSeq: Seq[(String, Seq[String], Seq[String])] = animalsTwoColSeq.map(tup => tup match {
			case (n, g1, g2) => (n.enumName, g1.enumNames, g2.enumNames)
		})

		val animalsTwoColDf: DataFrame = sparkSessionWrapper.createDataFrame(animalsTwoColStrSeq).toDF("AnimalType", "Group1", "Group2")
		//val animalsTwoColDf: DataFrame = animalsTwoColStrSeq.toDF("AnimalType", "Group1", "Group2") // TODO figure this out why not working

	}

}
