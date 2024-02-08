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


		import com.data.util.DataHub.ManualDataFrames.fromEnums.TradeDf._

		val ctry: Column = col(Country.name)

		// Excluding countries that belong in multiple hemispheres, leaving that case to 'otherwise'
		// WARNING: use :_*      ctry.isin(xs:_*)
		val ctryIsInSouthHemiOnly: Column = ctry.isin(SOUTHERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(SH).names: _*))
		val ctryIsInNorthHemiOnly: Column = ctry.isin(NORTHERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(NH).names: _*))
		val ctryIsInEastHemiOnly: Column = ctry.isin(EASTERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(EH).names: _*))
		val ctryIsInWestHemiOnly: Column = ctry.isin(WESTERN_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(WH).names: _*))
		val ctryIsInCentralHemiOnly: Column = ctry.isin(CENTRAL_HEMI.names: _*) and !(ctry.isin(countriesNotFromThisHemi(CH).names: _*))


		describe("when(): leaves 'null' for cases that don't fit the criteria"){

			import com.data.util.DataHub.ManualDataFrames.fromSparkByExamples._


			/*dfWO.withColumn("GenderRenamed",
				when(col("Gender") === "M", "Male")
					.when(col("Gender") === "F", "Female")
					.otherwise("Unknown")
			)*/
			/*val commodityWithColumnDf: DataFrame = tradeDf.withColumn("IsCommodity",
				when(col(Instrument))
			)*/



		}
		describe("chained when(): to impose multiple conditions at once"){

			val hemisWithColumnDf: DataFrame = tradeDf.withColumn(Hemisphere.name,
				when(ctryIsInSouthHemiOnly, Hemisphere.SouthernHemisphere.name)
					.when(ctryIsInNorthHemiOnly, Hemisphere.NorthernHemisphere.name)
					.when(ctryIsInEastHemiOnly, Hemisphere.EasternHemisphere.name)
					.when(ctryIsInWestHemiOnly, Hemisphere.WesternHemisphere.name)
					.when(ctryIsInCentralHemiOnly, Hemisphere.CentralHemisphere.name)
			)

			it("can be done using `withColumn()` or `select()`"){

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

			it("null elements are exactly the ones not fitting in one of the 'when' conditions"){
				// Asserting that the countries from multiple hemispheres got the NULL assignment
				val countriesFromMultipleHemis: Set[Country] = multiHemiCountries.toSet

				val countriesNull: Set[Country] = hemisWithColumnDf.filter(col(Hemisphere.name) <=> null)
					.select(col(Country.name))
					.collectCol[Country].toSet

				countriesNull should contain allElementsOf countriesFromMultipleHemis
				singleHemiCountries.toSet.intersect(countriesNull).isEmpty should be (true)
			}

			it("non-null elements are exactly the ones matching one of the 'when' conditions"){
				// Asserting that all other countries (from single hemisphere) did NOT get NULL assignment (no NULL for the single hemi countries)
				// 1) all countries with NO null are SINGLE hemi
				val countriesNotNull: Seq[Country] = hemisWithColumnDf
					.where(col(Hemisphere.name).isNotNull)
					.select(col(Country.name))
					.collectCol[Country, Country.type](Country) // NOTE must call with this weird syntax or else won't work.

				countriesNotNull should contain allElementsOf singleHemiCountries
				multiHemiCountries.toSet.intersect(countriesNotNull.toSet).isEmpty should be(true)
			}
		}

		it("when().... otherwise(): otherwise lets you define an output to avoid returning null"){

			val hemisDf: DataFrame = tradeDf.withColumn(Hemisphere.name,
				when(ctryIsInSouthHemiOnly, Hemisphere.SouthernHemisphere.name)
					.when(ctryIsInNorthHemiOnly, Hemisphere.NorthernHemisphere.name)
					.when(ctryIsInEastHemiOnly, Hemisphere.EasternHemisphere.name)
					.when(ctryIsInWestHemiOnly, Hemisphere.WesternHemisphere.name)
					.when(ctryIsInCentralHemiOnly, Hemisphere.CentralHemisphere.name)
			)

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
