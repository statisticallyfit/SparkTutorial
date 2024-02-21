package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns


import org.apache.spark.sql.{DataFrame, Row, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
import org.apache.spark.sql.types._

import utilities.GeneralUtils._
import com.data.util.EnumHub._
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
import com.data.util.DataHub.ManualDataFrames.fromEnums._
import TradeDf._
import AnimalDf._
import ArtistDf._
import Artist._


/**
 *
 */
class ColCastSpecs  extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {

	import sparkSessionWrapper.implicits._


	describe("Casting column to a type ..."){

		describe("can cast to any DataType"){

			it("using DataType name"){

			}
			it("using string name"){

			}
		}

		describe("can cast to a StructType (for nested column)"){

			// TODO see RenameColSpecs
		}
	}
}
