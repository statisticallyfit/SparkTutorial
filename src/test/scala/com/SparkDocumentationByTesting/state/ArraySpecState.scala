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



		// ----------------------------------------------------------------------------------------------

		import utilities.DataHub.ManualDataFrames.ArrayDf._
		import personInfo._


		val expectedSortArraySeq: Seq[SortByMidStruct[PersonMidFirstStruct]] = Seq(
			SortByMidStruct[PersonMidFirstStruct]("n", List(
				PersonMidFirstStruct(nnn, 1, 13, "152", Nanette),
				PersonMidFirstStruct(nnn, 1, 15, "131", Naza),
				PersonMidFirstStruct(nnn, 1, 15, "128", Nesryn),
				PersonMidFirstStruct(nnn, 1, 15, "154", Nicole),
				PersonMidFirstStruct(nnn, 1, 15, "155", Niki),
				PersonMidFirstStruct(nnn, 1, 12, "140", Nina))),
			SortByMidStruct[PersonMidFirstStruct]("h", List(
				PersonMidFirstStruct(hhh, 5, 5, "934", Hannah),
				PersonMidFirstStruct(hhh, 5, 5, "142", Harriet),
				PersonMidFirstStruct(hhh, 5, 5, "992", Harry),
				PersonMidFirstStruct(hhh, 5, 5, "143", Hazel),
				PersonMidFirstStruct(hhh, 5, 5, "111", Henry))),
			SortByMidStruct[PersonMidFirstStruct]("c", List(
				PersonMidFirstStruct(iii, 4, 19, "19", Katerina),
				PersonMidFirstStruct(iii, 17, 90, "138", Catherine),
				PersonMidFirstStruct(kkk, 9, 25, "348", Vesper),
				PersonMidFirstStruct(kkk, 34, 23, "787", Dmitry),
				PersonMidFirstStruct(vvv, 1, 11, "34", Yigor),
				PersonMidFirstStruct(vvv, 3, 10, "123", Tatiana),
				PersonMidFirstStruct(vvv, 3, 10, "111", Tyler),
				PersonMidFirstStruct(vvv, 10, 10, "0", Tijah))),
			SortByMidStruct[PersonMidFirstStruct]("b", List(
				PersonMidFirstStruct(bbb, 9, 19, "417", Bella),
				PersonMidFirstStruct(bbb, 9, 19, "430", Berenice),
				PersonMidFirstStruct(bbb, 9, 19, "445", Blake),
				PersonMidFirstStruct(bbb, 9, 19, "441", Bonnie),
				PersonMidFirstStruct(bbb, 9, 19, "442", Brianna),
				PersonMidFirstStruct(bbb, 9, 19, "412", Bridget),
				PersonMidFirstStruct(eee, 4, 9, "13", Xenia),
				PersonMidFirstStruct(nnn, 1, 15, "678", Natalia),
				PersonMidFirstStruct(ppp, 1, 52, "345", Penelope),
				PersonMidFirstStruct(ppp, 5, 52, "111", Pauline))),
			SortByMidStruct[PersonMidFirstStruct]("a", List(
				PersonMidFirstStruct(ddd, 8, 40, "332", Liliana),
				PersonMidFirstStruct(ggg, 3, 30, "191", Helen),
				PersonMidFirstStruct(jjj, 7, 11, "443", Amber),
				PersonMidFirstStruct(jjj, 7, 12, "555", Astrid),
				PersonMidFirstStruct(xxx, 1, 27, "1", Jasper),
				PersonMidFirstStruct(xxx, 3, 30, "324", Hugo),
				PersonMidFirstStruct(yyy, 2, 45, "223", Victor),
				PersonMidFirstStruct(zzz, 3, 11, "345", Quan),
				PersonMidFirstStruct(zzz, 10, 11, "345", Quinn))),
			SortByMidStruct[PersonMidFirstStruct]("s", List(
				PersonMidFirstStruct(ooo, 14, 22, "444", Sabrielle),
				PersonMidFirstStruct(ooo, 20, 22, "433", Sabrina),
				PersonMidFirstStruct(ooo, 20, 22, "122", Sarah),
				PersonMidFirstStruct(ooo, 20, 22, "112", Sascha),
				PersonMidFirstStruct(ooo, 20, 22, "134", Selene),
				PersonMidFirstStruct(ooo, 20, 21, "332", Sigurd),
				PersonMidFirstStruct(ooo, 20, 21, "156", Sophie),
				PersonMidFirstStruct(ooo, 20, 14, "189", Stacey)))
		)



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

		val expectedSortExplode = Seq(
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

		// ---------------------------------------------------

		val expectedExplodeMultiSort: Seq[List[PersonStruct]] = Seq(
			List(PersonStruct(8, Liliana, ddd, "332", 40),
				PersonStruct(3, Helen, ggg, "191", 30),
				PersonStruct(7, Amber, jjj, "443", 11),
				PersonStruct(7, Astrid, jjj, "555", 12),
				PersonStruct(1, Jasper, xxx, "1", 27),
				PersonStruct(3, Hugo, xxx, "324", 30),
				PersonStruct(2, Victor, yyy, "223", 45),
				PersonStruct(3, Quan, zzz, "345", 11),
				PersonStruct(10, Quinn, zzz, "345", 11)),

			List(PersonStruct(9, Bridget, bbb, "412", 19),
				PersonStruct(9, Bella, bbb, "417", 19),
				PersonStruct(9, Berenice, bbb, "430", 19),
				PersonStruct(9, Bonnie, bbb, "441", 19),
				PersonStruct(9, Brianna, bbb, "442", 19),
				PersonStruct(9, Blake, bbb, "445", 19),
				PersonStruct(4, Xenia, eee, "13", 9),
				PersonStruct(1, Natalia, nnn, "678", 15),
				PersonStruct(1, Penelope, ppp, "345", 52),
				PersonStruct(5, Pauline, ppp, "111", 52)),

			List(PersonStruct(4, Katerina, iii, "19", 19),
				PersonStruct(17, Catherine, iii, "138", 90),
				PersonStruct(9, Vesper, kkk, "348", 25),
				PersonStruct(34, Dmitry, kkk, "787", 23),
				PersonStruct(1, Yigor, vvv, "34", 11),
				PersonStruct(3, Tyler, vvv, "111", 10),
				PersonStruct(3, Tatiana, vvv, "123", 10),
				PersonStruct(10, Tijah, vvv, "0", 10)),

			List(PersonStruct(5, Henry, hhh, "111", 5),
				PersonStruct(5, Harriet, hhh, "142", 5),
				PersonStruct(5, Hazel, hhh, "143", 5),
				PersonStruct(5, Hannah, hhh, "934", 5),
				PersonStruct(5, Harry, hhh, "992", 5)),

			List(PersonStruct(1, Nesryn, nnn, "128", 15),
				PersonStruct(1, Naza, nnn, "131", 15),
				PersonStruct(1, Nina, nnn, "140", 15),
				PersonStruct(1, Nanette, nnn, "152", 15),
				PersonStruct(1, Nicole, nnn, "154", 15),
				PersonStruct(1, Niki, nnn, "155", 15)),

			List(PersonStruct(14, Sabrielle, ooo, "444", 22),
				PersonStruct(20, Stacey, ooo, "189", 14),
				PersonStruct(20, Sophie, ooo, "156", 21),
				PersonStruct(20, Sigurd, ooo, "332", 21),
				PersonStruct(20, Sascha, ooo, "112", 22),
				PersonStruct(20, Sarah, ooo, "122", 22),
				PersonStruct(20, Selene, ooo, "134", 22),
				PersonStruct(20, Sabrina, ooo, "433", 22))
		)

	}

}
