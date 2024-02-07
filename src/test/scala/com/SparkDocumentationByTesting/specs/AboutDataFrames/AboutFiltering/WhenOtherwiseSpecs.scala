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
class WhenOtherwiseSpecs extends AnyFunSpec with Matchers with SparkSessionWrapper {


	/**
	 * SOURCE: Spark-by-examples:
	 * 	- webpage = https://sparkbyexamples.com/spark/spark-case-when-otherwise-example/#and-or
	 * 	- code = https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/functions/WhenOtherwise.scala
 	 */


	describe("Creating new columns using ..."){

		it("when() - leaves 'null' for cases that don't fit the criteria"){

			import com.data.util.DataHub.ManualDataFrames.fromSparkByExamples._


			/*dfWO.withColumn("GenderRenamed",
				when(col("Gender") === "M", "Male")
					.when(col("Gender") === "F", "Female")
					.otherwise("Unknown")
			)*/



		}
		it("chained when() - to impose multiple conditions at once"){

			import com.data.util.DataHub.ManualDataFrames.fromEnums.TradeDf._

			val ctry: Column = col(Country.name)

			// Excluding countries that belong in multiple hemispheres, leaving that case to 'otherwise'
			// WARNING: use :_*      ctry.isin(xs:_*)
			val ctryIsInSouthHemiOnly: Column = ctry.isin(SOUTHERN_HEMI.namesEnumOnly: _*) and !(ctry.isin(countriesNotFromThisHemi(SH).namesEnumOnly: _*))
			val ctryIsInNorthHemiOnly: Column = ctry.isin(NORTHERN_HEMI.namesEnumOnly: _*) and !(ctry.isin(countriesNotFromThisHemi(NH).namesEnumOnly: _*))
			val ctryIsInEastHemiOnly: Column = ctry.isin(EASTERN_HEMI.namesEnumOnly: _*) and !(ctry.isin(countriesNotFromThisHemi(EH).namesEnumOnly: _*))
			val ctryIsInWestHemiOnly: Column = ctry.isin(WESTERN_HEMI.namesEnumOnly: _*) and !(ctry.isin(countriesNotFromThisHemi(WH).namesEnumOnly: _*))
			val ctryIsInCentralHemiOnly: Column = ctry.isin(CENTRAL_HEMI.namesEnumOnly: _*) and !(ctry.isin(countriesNotFromThisHemi(CH).namesEnumOnly: _*))

			// check: leaving out countries that are in multiple hemispheres =

			val hemisDf: DataFrame = tradeDf.withColumn("HemisphereFromMapCenter",
				when(ctryIsInSouthHemiOnly, Hemisphere.SouthernHemisphere.name)
					.when(ctryIsInNorthHemiOnly, Hemisphere.NorthernHemisphere.name)
					.when(ctryIsInEastHemiOnly, Hemisphere.EasternHemisphere.name)
					.when(ctryIsInWestHemiOnly, Hemisphere.WesternHemisphere.name)
					.when(ctryIsInCentralHemiOnly, Hemisphere.CentralHemisphere.name)
			)

			// Assert that nulls are where the countries belong in multiple hemispheres
		}

		it("when().... otherwise()"){


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
