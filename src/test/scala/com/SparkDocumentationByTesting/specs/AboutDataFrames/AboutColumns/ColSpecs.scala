package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns



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
 *
 */
class ColSpecs extends AnyFunSpec with Matchers with SparkSessionWrapper {


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
			animalDf.col(Animal.enumName) shouldBe a [Column]
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
