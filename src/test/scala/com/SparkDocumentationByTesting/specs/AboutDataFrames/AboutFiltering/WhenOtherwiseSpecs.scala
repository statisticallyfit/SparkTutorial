package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutFiltering



import org.apache.spark.sql.{DataFrame, Row, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import utilities.GeneralUtils._
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

	import com.data.util.DataHub.ManualDataFrames.fromSparkByExamples._


	describe("Creating new columns using ..."){

		it("when() - leaves 'null' for cases that don't fit the criteria"){



			/*dfWO.withColumn("GenderRenamed",
				when(col("Gender") === "M", "Male")
					.when(col("Gender") === "F", "Female")
					.otherwise("Unknown")
			)*/



		}
		it("chained when() - to impose multiple conditions at once"){


		}

		it("when().... otherwise()"){

			import com.data.util.DataHub.ManualDataFrames.fromEnums.TradeDf._

			val ctry: Column = col(Country.str)

			// Excluding countries that belong in multiple hemispheres, leaving that case to 'otherwise'
			val conditionSouth: Column = ctry isin (SOUTHERN_HEMI.map(_.str)) and ! (ctry isin(countriesFromOtherHemis(SH)) )
			val conditionNorth: Column = ctry isin (NORTHERN_HEMI.map(_.str)) and ! (ctry isin(countriesFromOtherHemis(NH)) )
			val conditionEast: Column = ctry isin (EASTERN_HEMI.map(_.str)) and ! (ctry isin(countriesFromOtherHemis(EH)) )
			val conditionWest: Column = ctry isin (WESTERN_HEMI.map(_.str)) and ! (ctry isin(countriesFromOtherHemis(WH)) )
			val conditionCentral: Column = ctry isin (CENTRAL_HEMI.map(_.str)) and ! (ctry isin(countriesFromOtherHemis(CH)) )

			// check: leaving out countries that are in multiple hemispheres =

			val hemiDf: DataFrame = tradeDf.withColumn("HemisphereFromMapCenter",
				when(conditionSouth, Hemisphere.SouthernHemisphere.str)
					.when(conditionNorth, Hemisphere.NorthernHemisphere.str)
					.when(conditionEast, Hemisphere.EasternHemisphere.str)
					.when(conditionWest, Hemisphere.WesternHemisphere.str)
					.when(conditionCentral, Hemisphere.CentralHemisphere.str)
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
