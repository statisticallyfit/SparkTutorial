package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns



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
 * SOURCE:
 * 	- chp 5 Bill Chambers
 * 	- spark by examples:
 * 		- website = https://sparkbyexamples.com/spark/spark-dataframe-withcolumn/
 * 		- code = https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/functions/AddColumn.scala
 */
class AddColumnSpecs extends AnyFunSpec with Matchers with SparkSessionWrapper with CustomMatchers {



	describe("Adding columns ..."){

		describe("using withColumn() to ..."){

			it("add ONE column"){

				import MusicDf._




			}
			describe("add multiple columns"){

				it("using chained withColumn()"){

				}
				it("using Map() passed to withColumns()"){

				}

			}
			it("add list column"){

			}
		}

		// ---------------

		describe("using select() to add column"){

		}
	}
}
