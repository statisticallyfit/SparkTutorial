package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutFiltering



import org.apache.spark.sql.{DataFrame, Row, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import utilities.GeneralUtils._
import utilities.EnumUtils.implicits._
import utilities.DFUtils
import DFUtils.implicits._
import DFUtils.TypeAbstractions._


//import com.SparkSessionForTests
import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._
import com.SparkDocumentationByTesting.CustomMatchers
/*import AnimalDf._
import TradeDf._*/
import com.data.util.EnumHub._

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._

import utilities.SparkSessionWrapper

import scala.reflect.runtime.universe._


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

			import com.data.util.DataHub.ManualDataFrames.fromEnums.AnimalDf._


			describe("when(), no otherwise()"){
				val catDf: DataFrame = animalDf.withColumn("Speak",
					when(col(Animal.name) === "Lion", "roar"))
			}
			describe("when().otherwise()"){

			}

			describe("when(), using ||"){

				// Using manual ||
				val stripedAnimals: Seq[Animal] = List(Animal.Cat.WildCat.Tiger, Animal.Zebra, Animal.SeaCreature.Clownfish, Animal.Bird.Bluejay)

				val dfStripes: DataFrame = animalDf.select(col("*"),
					when(col(Animal.name) === Animal.Cat.WildCat.Tiger.name ||
						col(Animal.name) === Animal.Zebra.name ||
						col(Animal.name) === Animal.SeaCreature.Clownfish.name ||
						col(Animal.name) === Animal.Bird.Bluejay.name,
						"Stripes"
					).alias("IsAnimalStriped")
				)

				val animalsCaseWhen: Seq[Animal] = dfStripes.filter(col("IsAnimalStriped").isNotNull)
					.select(col(Animal.name))
					.collectEnumCol[Animal]
					.distinct

				stripedAnimals should contain allElementsOf animalsCaseWhen


			}
			describe("when(), using &&"){

				val dfFish: DataFrame = animalDf.withColumn("FishFromWarmClimate",
					when(
						(col(Animal.name) isInCollection Animal.SeaCreature.values.names) &&
						(col(Climate.name) isInCollection ClimateTemperature.HOT.names),
						"SunnyFish"
					)
				)
				// Checking all SunnyFish are indeed SeaCreatures
				val fishCaseWhen: Seq[Animal] = dfFish.filter(col("FishFromWarmClimate").isNotNull)
					.select(col(Animal.name)) //, col(Climate.name))
					.collectEnumCol[Animal]
				Animal.SeaCreature.values should contain allElementsOf fishCaseWhen


				// Checking Fish from warm climate indeed are only in HOT climate.
				val climateHotCaseWhen: Seq[Climate] = dfFish.filter(col("FishFromWarmClimate").isNotNull)
					.select(col(Climate.name))
					.collectEnumCol[Climate]
				climateHotCaseWhen.toSet.subsetOf(ClimateTemperature.HOT.toSet) should be (true)
			}
		}

		// ---------------------------

		describe("when(), chained: to impose multiple conditions at once"){


			describe("when().when().when()..when(), no otherwise()"){

				import com.data.util.DataHub.ManualDataFrames.fromEnums.TradeDf._

				val ctry: Column = col(Country.name)

				// Excluding countries that belong in multiple hemispheres, leaving that case to 'otherwise'
				// WARNING: use :_*      ctry.isin(xs:_*)
				val ctryIsInSouthHemiOnly: Column = ctry.isin(SOUTHERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(SH).names: _*))
				val ctryIsInNorthHemiOnly: Column = ctry.isin(NORTHERN_HEMI.names: _*) and !ctry.isin(countriesNotFromThisHemi(NH).names: _*)
				val ctryIsInEastHemiOnly: Column = ctry.isin(EASTERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(EH).names: _*))
				val ctryIsInWestHemiOnly: Column = ctry.isin(WESTERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(WH).names: _*))
				val ctryIsInCentralHemiOnly: Column = ctry.isin(CENTRAL_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(CH).names: _*))


				val hemisWithColumnDf: DataFrame = tradeDf.withColumn(Hemisphere.name,
					when(ctryIsInSouthHemiOnly, Hemisphere.SouthernHemisphere.name)
						.when(ctryIsInNorthHemiOnly, Hemisphere.NorthernHemisphere.name)
						.when(ctryIsInEastHemiOnly, Hemisphere.EasternHemisphere.name)
						.when(ctryIsInWestHemiOnly, Hemisphere.WesternHemisphere.name)
						.when(ctryIsInCentralHemiOnly, Hemisphere.CentralHemisphere.name)
				)

				it("can be done using `withColumn()` or `select()`") {

					// Checking that the withCol way is same as select way
					val hemisSelectDf: DataFrame = tradeDf.select(col("*"),
						when(ctryIsInSouthHemiOnly, Hemisphere.SouthernHemisphere.name)
							.when(ctryIsInNorthHemiOnly, Hemisphere.NorthernHemisphere.name)
							.when(ctryIsInEastHemiOnly, Hemisphere.EasternHemisphere.name)
							.when(ctryIsInWestHemiOnly, Hemisphere.WesternHemisphere.name)
							.when(ctryIsInCentralHemiOnly, Hemisphere.CentralHemisphere.name)
							.alias(Hemisphere.name)
					)
					hemisWithColumnDf should equalDataFrame(hemisSelectDf)
				}

				it("when(): provides output for cases matching the condition. " +
					"In other words, non-null elements are exactly the ones matching one of the 'when' conditions") {

					val countriesCaseWhen: Seq[Country] = hemisWithColumnDf
						.where(col(Hemisphere.name).isNotNull)
						.select(col(Country.name))
						.collectEnumCol[Country]
					//.collectCol[Country, Country.type](Country) // NOTE must call with this weird syntax or else won't work.

					countriesCaseWhen should contain allElementsOf singleHemiCountries
					multiHemiCountries.toSet.intersect(countriesCaseWhen.toSet).isEmpty should be(true)
				}

				it("when(): outputs null when cases don't match the condition. " +
					"In other words, null elements are exactly the ones not fitting in one of the 'when' conditions") {
					// Asserting that the countries from multiple hemispheres got the NULL assignment

					val countriesCaseOutOfWhen: Set[Country] = hemisWithColumnDf.filter(col(Hemisphere.name) <=> null)
						.select(col(Country.name))
						.collectEnumCol[Country].toSet

					countriesCaseOutOfWhen should contain allElementsOf multiHemiCountries.toSet
					singleHemiCountries.toSet.intersect(countriesCaseOutOfWhen).isEmpty should be(true)
				}

				it("when().when()...when() using ||"){

					import com.data.util.DataHub.ManualDataFrames.fromEnums.AnimalDf._

					// Using fold to build up the condition expression
					val houseCats: Seq[EnumString] = Animal.Cat.DomesticCat.values.names
					val wildcats: Seq[EnumString] = Animal.Cat.WildCat.values.names

					val isDomesticCatCondition: Column = houseCats.tail.foldLeft(col(Animal.name) === houseCats.head)((accExpr: Column, nextCat: EnumString) => accExpr || (col(Animal.name) === nextCat))

					val isWildCatCondition: Column = wildcats.tail.foldLeft(col(Animal.name) === wildcats.head)((accExpr: Column, nextCat: EnumString) => accExpr || (col(Animal.name) === nextCat))

					val dfCatSpeak: DataFrame = animalDf.withColumn("CatSpeak",
						when(isDomesticCatCondition, "Meow")
							.when(isWildCatCondition, "Roar")
					)
					// Verify the "Meowers" are the domestic cats
					houseCats should contain allElementsOf
						dfCatSpeak.filter(col("CatSpeak") === "Meow")
						.select(col(Animal.name))
						.collectCol[Animal]

					// Verify the domestic cats only meow
					val meowingHouseCats: Seq[String] = dfCatSpeak.filter(col(Animal.name).isin(houseCats:_*))
						.select("CatSpeak")
						.collectCol[String]
						.distinct
					meowingHouseCats.length shouldEqual 1
					meowingHouseCats.head shouldEqual "Meow"

					// Verify the "Roarers" are the wild cats
					wildcats should contain allElementsOf
						dfCatSpeak.filter(col("CatSpeak") === "Roar")
							.select(col(Animal.name))
							.collectCol[Animal]

					// Verify the wild cats only make roars
					val roaringWildCats: Seq[String] = dfCatSpeak.filter(col(Animal.name).isin(wildcats: _*))
						.select("CatSpeak")
						.collectCol[String]
						.distinct
					roaringWildCats.length shouldEqual 1
					roaringWildCats.head shouldEqual "Roar"


				}
			}


			describe("when().when().when() .... otherwise(): otherwise lets you define an output to avoid returning null") {

				import com.data.util.DataHub.ManualDataFrames.fromEnums.TradeDf._

				val ctry: Column = col(Country.name)

				// Excluding countries that belong in multiple hemispheres, leaving that case to 'otherwise'
				// WARNING: use :_*      ctry.isin(xs:_*)
				val ctryIsInSouthHemiOnly: Column = ctry.isin(SOUTHERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(SH).names: _*))
				val ctryIsInNorthHemiOnly: Column = ctry.isin(NORTHERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(NH).names: _*))
				val ctryIsInEastHemiOnly: Column = ctry.isin(EASTERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(EH).names: _*))
				val ctryIsInWestHemiOnly: Column = ctry.isin(WESTERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(WH).names: _*))
				val ctryIsInCentralHemiOnly: Column = ctry.isin(CENTRAL_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(CH).names: _*))

				val hemisWithColumnDf: DataFrame = tradeDf.withColumn(Hemisphere.name,
					when(ctryIsInSouthHemiOnly, Hemisphere.SouthernHemisphere.name)
						.when(ctryIsInNorthHemiOnly, Hemisphere.NorthernHemisphere.name)
						.when(ctryIsInEastHemiOnly, Hemisphere.EasternHemisphere.name)
						.when(ctryIsInWestHemiOnly, Hemisphere.WesternHemisphere.name)
						.when(ctryIsInCentralHemiOnly, Hemisphere.CentralHemisphere.name)
						.otherwise("MULTIPLE")
				)

				it("can be done using `withColumn()` or `select()`") {

					// Checking that the withCol way is same as select way
					val hemisSelectDf: DataFrame = tradeDf.select(col("*"),
						when(ctryIsInSouthHemiOnly, Hemisphere.SouthernHemisphere.name)
							.when(ctryIsInNorthHemiOnly, Hemisphere.NorthernHemisphere.name)
							.when(ctryIsInEastHemiOnly, Hemisphere.EasternHemisphere.name)
							.when(ctryIsInWestHemiOnly, Hemisphere.WesternHemisphere.name)
							.when(ctryIsInCentralHemiOnly, Hemisphere.CentralHemisphere.name)
							.otherwise("MULTIPLE")
							.alias(Hemisphere.name)
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

					val countriesCaseWhen: Seq[Country] = hemisWithColumnDf
						.where(col(Hemisphere.name) =!= "MULTIPLE")
						.select(col(Country.name))
						.collectEnumCol[Country]
					//.collectCol[Country, Country.type](Country) // NOTE must call with this weird syntax or else won't work.

					countriesCaseWhen should contain allElementsOf singleHemiCountries
					multiHemiCountries.toSet.intersect(countriesCaseWhen.toSet).isEmpty should be(true)
				}

				it("when(): outputs null when cases don't match the condition. " +
					"In other words, null elements are exactly the ones not fitting in one of the 'when' conditions") {
					// Asserting that the countries from multiple hemispheres got the NULL assignment

					val countriesCaseOtherwise: Seq[Country] = hemisWithColumnDf.filter(col(Hemisphere.name) === "MULTIPLE")
						.select(col(Country.name))
						.collectEnumCol[Country]

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
