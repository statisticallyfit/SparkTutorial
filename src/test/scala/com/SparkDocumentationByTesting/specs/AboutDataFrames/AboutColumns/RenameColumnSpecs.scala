package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

import utilities.GeneralUtils._
import utilities.EnumUtils.implicits._
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
class RenameColumnSpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {


	import sparkSessionWrapper.implicits._

	import com.SparkDocumentationByTesting.state.SpecState._
	import AnimalState._


	describe("Renaming columns"){

		/**
		 * SOURCE: spark-test-repo
		 * 	- https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L126-L131
		 */
		it("using as() word"){

			val oldColName = Animal.name
			val newColName = "The Animal Column"

			animalDf.select(col(oldColName)).columns.head shouldEqual oldColName
			animalDf.select(col(oldColName).as(newColName)).columns.head shouldEqual newColName
		}
		it("using alias() keyword"){
			val oldColName = Climate.name
			val newColName = "The Climate Column"

			animalDf.select(col(oldColName)).columns.head shouldEqual oldColName
			animalDf.select(col(oldColName).as(newColName)).columns.head shouldEqual newColName
		}
		it("using name() keyword") {
			val oldColName = Country.name
			val newColName = "The Country Column"

			animalDf.select(col(oldColName)).columns.head shouldEqual oldColName
			animalDf.select(col(oldColName).as(newColName)).columns.head shouldEqual newColName
		}

		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
		 */
		it("using expr() followed by as() keyword"){

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
		it("using expr() followed by alias()"){
			val df = animalDf.select(expr("Animal as TheAnimals_1").alias("TheAnimals_2"))

			df.columns.length should be (1)
			df.columns should equal (Seq("TheAnimals_2"))
		}

		it("using withColumn()"){}

		it("using withColumn() and as()"){

		}
		it("using withColumnRenamed()"){

		}


	}

}
