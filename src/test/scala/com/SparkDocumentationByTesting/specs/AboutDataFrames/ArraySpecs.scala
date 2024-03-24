package com.SparkDocumentationByTesting.specs.AboutDataFrames




import org.apache.spark.sql.catalyst.expressions._ //genericrowwithschema...
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, Dataset, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize }
import org.apache.spark.sql.types._
//import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions._

import utilities.DFUtils; import DFUtils._ ; import DFUtils.TypeAbstractions._; import DFUtils.implicits._
import utilities.GeneralMainUtils._
import utilities.GeneralMainUtils.implicits._
import utilities.DataHub.ImportedDataFrames.fromBillChambersBook._
import utilities.DataHub.ManualDataFrames.fromEnums._
import utilities.DataHub.ManualDataFrames.fromSparkByExamples._
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

import Animal._ ; import SeaCreature._; import Whale._; import Bird._ ; import Eagle._ ;
import Rodent._; import Squirrel._ ; import WeaselMustelid._ ; import Camelid._
import Equine._; import Horse._; import Bear._ ; import Deer._; import Monkey._; import Ape._
import Insect._; import Reptile._; import Lizard._; import Amphibian._; import Frog._
import Cat._ ; import DomesticCat._ ; import WildCat._; import Canine._; import WildCanine._; import DomesticDog._; import Fox._
// TODO update with new animals made

import ClimateZone._
import Biome._; import Forest._; import Marine._; import Freshwater._; import Saltwater._; import Grassland._; import Tundra._
import Instrument._; import FinancialInstrument._ ; import Commodity._ ; import Transaction._

import World.Africa._
import World.Europe._
import World.NorthAmerica._
import World.SouthAmerica._
import World._
import World.Asia._
import World.Oceania._
import World.CentralAmerica._

//import com.SparkSessionForTests
import com.SparkDocumentationByTesting.CustomMatchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import org.scalatest.Assertions._
import utilities.SparkSessionWrapper



/**
 *
 */
class ArraySpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {

	/*import AnimalState._
	import FlightState._
	import TradeState._*/

	import sparkSessionWrapper.implicits._



	// NOTE: say df.show(n, false) so not truncate the columns which contain the arrays as rows.

	/**
	 * SOURCE:
	 * 	- https://medium.com/expedia-group-tech/deep-dive-into-apache-spark-array-functions-720b8fbfa729
	 */
	describe("Array SQL Functions"){

		val animalArrayDf: DataFrame = (animalDf.groupBy(ClimateZone.enumName, Biome.enumName)
			.agg(collect_list(col(Animal.enumName)).as("ArrayAnimal"),
				collect_list(col("Amount")).as("ArrayAmount"),
				collect_list(col(World.enumName)).as("ArrayWorld"),
				//collect_list(col())
			))

		it("array_contains(): returns BOOL, answers whether a particular element is within the array-column"){

			animalArrayDf.withColumn("ResultContains", array_contains(col("ArrayAnimal"), Bear.enumName))


			import scala.util.Try
			def checkerBear(n: EnumString) = Try {
				Bear.withName(n)
			}.toOption

			val LEN = animalArrayDf.count().toInt

			// TODO how to check whether an elemen tis of instance Bear??? how tu se udf, gives error help?
			val filterBearType: Seq[Row] => Boolean = (animals) => {
				//animals.exists(am => checkerBear().isDefined)
				animals.head.toSeq.asInstanceOf[Seq[String]].exists(am => checkerBear(am).isDefined)
			}
			val myudf = udf(filterBearType)
			animalArrayDf.select(col("ArrayAnimal"), myudf(col("ArrayAnimal")).as("ResultContains")).show(LEN, false)
		}
		it("array_zip"){

		}
		it("array_overlap"){

		}
	}

}
