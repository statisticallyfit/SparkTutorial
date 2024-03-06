package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutFiltering



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


//import com.SparkSessionForTests
import com.SparkDocumentationByTesting.CustomMatchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import org.scalatest.Assertions._
import utilities.SparkSessionWrapper


/**
 * SOURCE: spark-test-repo:
 * 	-
 */
class WhenOtherwiseSpecs extends AnyFunSpec with Matchers with SparkSessionWrapper with CustomMatchers {


	/**
	 * SOURCE: Spark-by-examples:
	 * 	- webpage = https://sparkbyexamples.com/spark/spark-case-when-otherwise-example/#and-or
	 * 	- code = https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/functions/WhenOtherwise.scala
 	 */


	describe("Creating new columns using ..."){




		describe("when() (single): states what do output when a column matches the condition"){

			import utilities.DataHub.ManualDataFrames.fromEnums.AnimalDf._


			describe("when(), no otherwise()"){
				val catDf: DataFrame = animalDf.withColumn("Speak",
					when(col(Animal.enumName) === "Lion", "roar"))
			}
			describe("when().otherwise()"){

			}

			describe("when(), using ||"){

				// Using manual ||
				val stripedAnimals: Seq[Animal] = List(Animal.Cat.WildCat.Tiger, Animal.Zebra, Animal.SeaCreature.Clownfish, Animal.Bird.Bluejay)

				val dfStripes: DataFrame = animalDf.select(col("*"),
					when(col(Animal.enumName) === Animal.Cat.WildCat.Tiger.enumName ||
						col(Animal.enumName) === Animal.Zebra.enumName ||
						col(Animal.enumName) === Animal.SeaCreature.Clownfish.enumName ||
						col(Animal.enumName) === Animal.Bird.Bluejay.enumName,
						"Stripes"
					).alias("IsAnimalStriped")
				)

				val animalsCaseWhen: Seq[Animal] = dfStripes.filter(col("IsAnimalStriped").isNotNull)
					.select(col(Animal.enumName))
					.collectEnumCol[Animal]
					.distinct

				stripedAnimals should contain allElementsOf animalsCaseWhen


			}
			describe("when(), using &&"){

				val dfFish: DataFrame = animalDf.withColumn("FishFromWarmClimate",
					when(
						(col(Animal.enumName) isInCollection Animal.SeaCreature.values.typeNames) &&
						(col(Climate.enumName) isInCollection ClimateTemperature.HOT.typeNames),
						"SunnyFish"
					)
				)
				// Checking all SunnyFish are indeed SeaCreatures
				val fishCaseWhen: Seq[Animal] = dfFish.filter(col("FishFromWarmClimate").isNotNull)
					.select(col(Animal.enumName)) //, col(Climate.name))
					.collectEnumCol[Animal]
				Animal.SeaCreature.values should contain allElementsOf fishCaseWhen


				// Checking Fish from warm climate indeed are only in HOT climate.
				val climateHotCaseWhen: Seq[Climate] = dfFish.filter(col("FishFromWarmClimate").isNotNull)
					.select(col(Climate.enumName))
					.collectEnumCol[Climate]
				climateHotCaseWhen.toSet.subsetOf(ClimateTemperature.HOT.toSet) should be (true)
			}
		}

		// ---------------------------

		describe("when(), chained: to impose multiple conditions at once"){


			describe("when().when().when()..when(), no otherwise()"){

				import utilities.DataHub.ManualDataFrames.fromEnums.TradeDf._

				val ctry: Column = col(World.enumName)

				// Excluding countries that belong in multiple hemispheres, leaving that case to 'otherwise'
				// WARNING: use :_*      ctry.isin(xs:_*)
				val ctryIsInSouthHemiOnly: Column = ctry.isin(SOUTHERN_HEMI.typeNames: _*) and !(ctry.isin(countriesNotFromThisHemi(SH).typeNames: _*))
				val ctryIsInNorthHemiOnly: Column = ctry.isin(NORTHERN_HEMI.typeNames: _*) and !ctry.isin(countriesNotFromThisHemi(NH).typeNames: _*)
				val ctryIsInEastHemiOnly: Column = ctry.isin(EASTERN_HEMI.typeNames: _*) and !(ctry.isin(countriesNotFromThisHemi(EH).typeNames: _*))
				val ctryIsInWestHemiOnly: Column = ctry.isin(WESTERN_HEMI.typeNames: _*) and !(ctry.isin(countriesNotFromThisHemi(WH).typeNames: _*))
				val ctryIsInCentralHemiOnly: Column = ctry.isin(CENTRAL_HEMI.typeNames: _*) and !(ctry.isin(countriesNotFromThisHemi(CH).typeNames: _*))


				val hemisWithColumnDf: DataFrame = tradeDf.withColumn(Hemisphere.enumName,
					when(ctryIsInSouthHemiOnly, Hemisphere.SouthernHemisphere.enumName)
						.when(ctryIsInNorthHemiOnly, Hemisphere.NorthernHemisphere.enumName)
						.when(ctryIsInEastHemiOnly, Hemisphere.EasternHemisphere.enumName)
						.when(ctryIsInWestHemiOnly, Hemisphere.WesternHemisphere.enumName)
						.when(ctryIsInCentralHemiOnly, Hemisphere.CentralHemisphere.enumName)
				)

				it("can be done using `withColumn()` or `select()`") {

					// Checking that the withCol way is same as select way
					val hemisSelectDf: DataFrame = tradeDf.select(col("*"),
						when(ctryIsInSouthHemiOnly, Hemisphere.SouthernHemisphere.enumName)
							.when(ctryIsInNorthHemiOnly, Hemisphere.NorthernHemisphere.enumName)
							.when(ctryIsInEastHemiOnly, Hemisphere.EasternHemisphere.enumName)
							.when(ctryIsInWestHemiOnly, Hemisphere.WesternHemisphere.enumName)
							.when(ctryIsInCentralHemiOnly, Hemisphere.CentralHemisphere.enumName)
							.alias(Hemisphere.enumName)
					)
					hemisWithColumnDf should equalDataFrame(hemisSelectDf)
				}

				it("when(): provides output for cases matching the condition. " +
					"In other words, non-null elements are exactly the ones matching one of the 'when' conditions") {

					val countriesCaseWhen: Seq[World] = hemisWithColumnDf
						.where(col(Hemisphere.enumName).isNotNull)
						.select(col(World.enumName))
						.collectEnumCol[World]
					//.collectCol[Country, Country.type](Country) // NOTE must call with this weird syntax or else won't work.

					countriesCaseWhen should contain allElementsOf singleHemiCountries
					multiHemiCountries.toSet.intersect(countriesCaseWhen.toSet).isEmpty should be(true)
				}

				it("when(): outputs null when cases don't match the condition. " +
					"In other words, null elements are exactly the ones not fitting in one of the 'when' conditions") {
					// Asserting that the countries from multiple hemispheres got the NULL assignment

					val countriesCaseOutOfWhen: Set[World] = hemisWithColumnDf.filter(col(Hemisphere.enumName) <=> null)
						.select(col(World.enumName))
						.collectEnumCol[World].toSet

					countriesCaseOutOfWhen should contain allElementsOf multiHemiCountries.toSet
					singleHemiCountries.toSet.intersect(countriesCaseOutOfWhen).isEmpty should be(true)
				}

				it("when().when()...when() using ||"){

					import utilities.DataHub.ManualDataFrames.fromEnums.AnimalDf._

					// Using fold to build up the condition expression
					val houseCats: Seq[EnumString] = Animal.Cat.DomesticCat.values.typeNames
					val wildcats: Seq[EnumString] = Animal.Cat.WildCat.values.typeNames

					val isDomesticCatCondition: Column = houseCats.tail.foldLeft(col(Animal.enumName) === houseCats.head)((accExpr: Column, nextCat: EnumString) => accExpr || (col(Animal.enumName) === nextCat))

					val isWildCatCondition: Column = wildcats.tail.foldLeft(col(Animal.enumName) === wildcats.head)((accExpr: Column, nextCat: EnumString) => accExpr || (col(Animal.enumName) === nextCat))

					val dfCatSpeak: DataFrame = animalDf.withColumn("CatSpeak",
						when(isDomesticCatCondition, "Meow")
							.when(isWildCatCondition, "Roar")
					)
					// Verify the "Meowers" are the domestic cats
					houseCats should contain allElementsOf
						dfCatSpeak.filter(col("CatSpeak") === "Meow")
						.select(col(Animal.enumName))
						.collectCol[Animal]

					// Verify the domestic cats only meow
					val meowingHouseCats: Seq[String] = dfCatSpeak.filter(col(Animal.enumName).isin(houseCats:_*))
						.select("CatSpeak")
						.collectCol[String]
						.distinct
					meowingHouseCats.length shouldEqual 1
					meowingHouseCats.head shouldEqual "Meow"

					// Verify the "Roarers" are the wild cats
					wildcats should contain allElementsOf
						dfCatSpeak.filter(col("CatSpeak") === "Roar")
							.select(col(Animal.enumName))
							.collectCol[Animal]

					// Verify the wild cats only make roars
					val roaringWildCats: Seq[String] = dfCatSpeak.filter(col(Animal.enumName).isin(wildcats: _*))
						.select("CatSpeak")
						.collectCol[String]
						.distinct
					roaringWildCats.length shouldEqual 1
					roaringWildCats.head shouldEqual "Roar"


				}
			}


			describe("when().when().when() .... otherwise(): otherwise lets you define an output to avoid returning null") {

				import utilities.DataHub.ManualDataFrames.fromEnums.TradeDf._

				val ctry: Column = col(World.enumName)

				// Excluding countries that belong in multiple hemispheres, leaving that case to 'otherwise'
				// WARNING: use :_*      ctry.isin(xs:_*)
				val ctryIsInSouthHemiOnly: Column = ctry.isin(SOUTHERN_HEMI.typeNames: _*) and !(ctry.isin(countriesNotFromThisHemi(SH).typeNames: _*))
				val ctryIsInNorthHemiOnly: Column = ctry.isin(NORTHERN_HEMI.typeNames: _*) and !(ctry.isin(countriesNotFromThisHemi(NH).typeNames: _*))
				val ctryIsInEastHemiOnly: Column = ctry.isin(EASTERN_HEMI.typeNames: _*) and !(ctry.isin(countriesNotFromThisHemi(EH).typeNames: _*))
				val ctryIsInWestHemiOnly: Column = ctry.isin(WESTERN_HEMI.typeNames: _*) and !(ctry.isin(countriesNotFromThisHemi(WH).typeNames: _*))
				val ctryIsInCentralHemiOnly: Column = ctry.isin(CENTRAL_HEMI.typeNames: _*) and !(ctry.isin(countriesNotFromThisHemi(CH).typeNames: _*))

				val hemisWithColumnDf: DataFrame = tradeDf.withColumn(Hemisphere.enumName,
					when(ctryIsInSouthHemiOnly, Hemisphere.SouthernHemisphere.enumName)
						.when(ctryIsInNorthHemiOnly, Hemisphere.NorthernHemisphere.enumName)
						.when(ctryIsInEastHemiOnly, Hemisphere.EasternHemisphere.enumName)
						.when(ctryIsInWestHemiOnly, Hemisphere.WesternHemisphere.enumName)
						.when(ctryIsInCentralHemiOnly, Hemisphere.CentralHemisphere.enumName)
						.otherwise("MULTIPLE")
				)

				it("can be done using `withColumn()` or `select()`") {

					// Checking that the withCol way is same as select way
					val hemisSelectDf: DataFrame = tradeDf.select(col("*"),
						when(ctryIsInSouthHemiOnly, Hemisphere.SouthernHemisphere.enumName)
							.when(ctryIsInNorthHemiOnly, Hemisphere.NorthernHemisphere.enumName)
							.when(ctryIsInEastHemiOnly, Hemisphere.EasternHemisphere.enumName)
							.when(ctryIsInWestHemiOnly, Hemisphere.WesternHemisphere.enumName)
							.when(ctryIsInCentralHemiOnly, Hemisphere.CentralHemisphere.enumName)
							.otherwise("MULTIPLE")
							.alias(Hemisphere.enumName)
					)
					hemisWithColumnDf should equalDataFrame(hemisSelectDf)
				}

				it("when(): provides output for cases matching the condition. " +
					"In other words, non-null elements are exactly the ones matching one of the 'when' conditions") {
					// Asserting that all other countries (from single hemisphere) did NOT get NULL assignment (no NULL for the single hemi countries)
					// 1) all countries with NO null are SINGLE hemi
					/*val instrumentCaseWhen: Seq[FinancialInstrument] = hemisWithColumnDf
						.filter(col("PreciousKind").isNotNull)
						.select(col(Instrument.FinancialInstrument.name))
						.collectEnumCol[FinancialInstrument]*/

					val countriesCaseWhen: Seq[World] = hemisWithColumnDf
						.where(col(Hemisphere.enumName) =!= "MULTIPLE")
						.select(col(World.enumName))
						.collectEnumCol[World]
					//.collectCol[Country, Country.type](Country) // NOTE must call with this weird syntax or else won't work.

					countriesCaseWhen should contain allElementsOf singleHemiCountries
					multiHemiCountries.toSet.intersect(countriesCaseWhen.toSet).isEmpty should be(true)
				}

				it("when(): outputs null when cases don't match the condition. " +
					"In other words, null elements are exactly the ones not fitting in one of the 'when' conditions") {
					// Asserting that the countries from multiple hemispheres got the NULL assignment

					val countriesCaseOtherwise: Seq[World] = hemisWithColumnDf.filter(col(Hemisphere.enumName) === "MULTIPLE")
						.select(col(World.enumName))
						.collectEnumCol[World]

					countriesCaseOtherwise.toSet should contain allElementsOf multiHemiCountries.toSet
					singleHemiCountries.toSet.intersect(countriesCaseOtherwise.toSet).isEmpty should be(true)
				}
			}

		}


		describe("using nested when-otherwise"){
			// TODO nested when-otherwise = https://stackoverflow.com/questions/46640862/spark-dataframe-nested-case-when-statement
		}


		describe("using 'case-when' sql on dataframes"){
			// TODO = https://hyp.is/iWo9ksgzEe6UW7tqFI4QCA/sparkbyexamples.com/spark/spark-case-when-otherwise-example/
		}




	}

	// TODO
	describe("Error handling for invalid expressions"){
		// Test error handling for invalid expressions.
		intercept[IllegalArgumentException] {
			col("key").when(col("key") === 1, -1)
		}
		intercept[IllegalArgumentException] {
			col("key").otherwise(-1)
		}
		intercept[IllegalArgumentException] {
			when(col("key") === 1, -1).otherwise(-1).otherwise(-1)
		}
	}



	//SOURCE: spark-test-repo = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L617-L636

	// when, double when, error handling for invalid expressions
}
