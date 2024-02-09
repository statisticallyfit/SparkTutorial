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




		describe("when(), singular: states what do output when a column matches the condition"){

			import com.data.util.DataHub.ManualDataFrames.fromEnums.AnimalDf._


			describe("when(), no otherwise()"){
				val catDf: DataFrame = animalDf.withColumn("Speak",
					when(col(Animal.name) === "Lion", "roar"))
			}
			describe("when().otherwise()"){

			}

			describe("when(), using ||"){
				// Using fold to build up the condition expression
				val houseCats: Seq[EnumString] = Animal.Cat.DomesticCat.values.names
				val isDomesticCatCondition: Column = houseCats.tail.foldLeft(col(Animal.name) === houseCats.head)((accExpr: Column, nextCat: EnumString) => accExpr || (col(Animal.name) === nextCat))

				val HOUSECAT_TITLE = "IsDomesticCat"
				val dfHousecat = animalDf.withColumn(HOUSECAT_TITLE, when(isDomesticCatCondition, "Meow"))

				dfHousecat.filter(col(Animal.name) isInCollection houseCats ).select(HOUSECAT_TITLE).collectCol[String].distinct.head shouldEqual "Meow"

				
				val WILDCAT_TITLE = "IsWildCat"
				val wildcats: Seq[EnumString] = Animal.Cat.WildCat.values.names
				val isWildCatCondition: Column = wildcats.tail.foldLeft(col(Animal.name) === wildcats.head)((accExpr: Column, nextCat: EnumString) => accExpr || (col(Animal.name) === nextCat))

				val dfWildcat = animalDf.withColumn(WILDCAT_TITLE, when(isWildCatCondition, "Roar"))
				dfWildcat.filter(col(Animal.name) isInCollection wildcats ).select(WILDCAT_TITLE).collectCol[String].distinct.head shouldEqual "Roar"

				// Using manual ||
				val stripedAnimals: Seq[EnumString] = List(Animal.Cat.WildCat.Tiger, Animal.Zebra, Animal.SeaCreature.Clownfish, Animal.Bird.Bluejay).names
				val isStripedCondition: Column = col(Animal.name) === Animal.Cat.WildCat.Tiger.name ||
					col(Animal.name) === Animal.Zebra.name ||
					col(Animal.name) === Animal.SeaCreature.Clownfish.name ||
					col(Animal.name) === Animal.Bird.Bluejay.name



			}
			describe("when(), using &&"){

				// TODO
				animalDf.withColumn("FishFromWarmClimate",
					when(
						(col(Animal.name) isInCollection Animal.SeaCreature.values.names) &&
						(col(Climate.name) isInCollection ClimateTemperature.HOT),
						"TropicsFish"
					)
				)

			}
		}

		// ---------------------------

		describe("when(), chained: to impose multiple conditions at once"){

			import com.data.util.DataHub.ManualDataFrames.fromEnums.TradeDf._

			val ctry: Column = col(Country.name)

			// Excluding countries that belong in multiple hemispheres, leaving that case to 'otherwise'
			// WARNING: use :_*      ctry.isin(xs:_*)
			val ctryIsInSouthHemiOnly: Column = ctry.isin(SOUTHERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(SH).names: _*))
			val ctryIsInNorthHemiOnly: Column = ctry.isin(NORTHERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(NH).names: _*))
			val ctryIsInEastHemiOnly: Column = ctry.isin(EASTERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(EH).names: _*))
			val ctryIsInWestHemiOnly: Column = ctry.isin(WESTERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(WH).names: _*))
			val ctryIsInCentralHemiOnly: Column = ctry.isin(CENTRAL_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(CH).names: _*))


			describe("when().when().when()..when(), no otherwise()"){
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
					// Asserting that all other countries (from single hemisphere) did NOT get NULL assignment (no NULL for the single hemi countries)
					// 1) all countries with NO null are SINGLE hemi
					/*val instrumentCaseWhen: Seq[FinancialInstrument] = hemisWithColumnDf
						.filter(col("PreciousKind").isNotNull)
						.select(col(Instrument.FinancialInstrument.name))
						.collectEnumCol[FinancialInstrument]*/

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


				// TODO : show chained when() are equivalent to ||
				it("TODO") {

				}
			}


			describe("when().when().when() .... otherwise(): otherwise lets you define an output to avoid returning null") {

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
					val countriesFromMultipleHemis: Set[Country] = multiHemiCountries.toSet

					val countriesCaseOutOfWhen: Set[Country] = hemisWithColumnDf.filter(col(Hemisphere.name) <=> null)
						.select(col(Country.name))
						.collectEnumCol[Country].toSet

					countriesCaseOutOfWhen should contain allElementsOf countriesFromMultipleHemis
					singleHemiCountries.toSet.intersect(countriesCaseOutOfWhen).isEmpty should be(true)
				}
			}

		}


	}

	describe("Filtering rows using ..."){
		it("when()") {

		}
		it("chained when() - to impose multiple conditions at once") {

		}

		it("when().... otherwise()") {

		}
	}



	//SOURCE: spark-test-repo = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L617-L636

	// when, double when, error handling for invalid expressions
}
