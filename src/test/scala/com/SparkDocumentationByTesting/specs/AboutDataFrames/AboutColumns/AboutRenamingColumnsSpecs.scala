package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

import utilities.DFUtils
import DFUtils.TypeAbstractions._
import DFUtils.implicits._

//import com.SparkSessionForTests
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper // intercept


import com.SparkDocumentationByTesting.CustomMatchers
import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums.{TradeDf, AnimalDf}
import TradeDf._
import AnimalDf._

import com.data.util.EnumHub._

/**
 *
 */
class AboutRenamingColumnsSpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {


	import sparkSessionWrapper.implicits._

	import com.SparkDocumentationByTesting.state.ColumnTestsState._
	import AnimalState._


	describe("Renaming columns"){
		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
		 */
		it("can rename with expr() and 'as' word"){

			val df = animalDf.select(expr("Animal as TheAnimals"))

			df.columns.length should equal (1)
			df.columns.head shouldEqual "TheAnimals"
		}

		// TODO - rename with any way of calling the column ($, col, "" etcc)  + using alias(), as(), withColumn etc
		// resultDf.select($"Animal".alias())
		// resultDf.select($"Animal".as())
		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
		 */
		it("can rename with expr() and alias()"){
			val df = animalDf.select(expr("Animal as TheAnimals_1").alias("TheAnimals_2"))

			df.columns.length should be (1)
			df.columns should equal (Seq("TheAnimals_2"))
		}

		it("can rename using withColumn()"){}

		it("can rename using withColumn() and as()"){

		}
		it("can rename with withColumnRenamed()"){

		}


	}

}
