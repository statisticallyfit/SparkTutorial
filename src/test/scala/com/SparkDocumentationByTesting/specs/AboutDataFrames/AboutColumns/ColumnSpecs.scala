package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns


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
 *
 */
class ColumnSpecs extends AnyFunSpec with Matchers with SparkSessionWrapper {


	import com.SparkDocumentationByTesting.state.SpecState._
	/*import AnimalState._
	import FlightState._
	import TradeState._*/

	import sparkSessionWrapper.implicits._

	describe("Column functions"){

		it("column function used alone should yield a column (without dataframe)"){

			import AnimalDf._

			col("someColumn") shouldBe a [Column]
			column("someColumn") shouldBe a [Column]
			animalDf.col(Animal.str) shouldBe a [Column]
			expr("col(\"SomeColumnName\")") shouldBe a [Column]

		}

		// SOURCE: chp5 BillChambers
		it("column functions can be manipulated as expressions"){
			(((col("someCol") + 5) * 200) - 6) < col("otherCol") shouldBe a [Column]
			expr("(((someCol + 5) * 200) - 6) < otherCol") shouldBe a [Column]
		}

		it("withColumn() used on dataframe should add a column"){
			// WARNING: see AddingColumns specs
		}


	}

	describe("Accessing columns"){
		// NOTE: see SelectSpecs
	}

	describe("Column operations"){

		describe("comparing columns"){

			it("double equals checks equality of two Column objects"){
				$"aCol".desc == $"aCol".desc shouldBe a [Boolean]
				$"aCol".desc == $"aCol".desc shouldEqual true
				$"aCol".asc != $"aCol".desc should equal (true)
			}
			it("triple equals yields Column expression"){
				$"aCol".asc === $"aCol".desc shouldBe a [Column]
				($"aCol".desc === $"aCol".asc).toString shouldEqual "(aCol DESC NULLS LAST = aCol ASC NULLS FIRST)"
			}
		}




	}

	describe("operations on schemas of columns"){

		// NOTE: withfield, dropfield  - not on the exam
		// withField() = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L1053-L1851
		// dropField = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L1852-L2484
	}


}
