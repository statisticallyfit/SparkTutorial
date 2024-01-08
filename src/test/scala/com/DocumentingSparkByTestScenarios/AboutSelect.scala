package com.DocumentingSparkByTestScenarios


import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._

import org.apache.spark.sql.{Column, ColumnName, Row, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{avg, col, column, count, cume_dist, dense_rank, expr, lag, lead, max, min, ntile, percent_rank, rank, row_number, sum} // rangeBetween, rowsBetween

import org.apache.spark.sql.expressions.{Window, WindowSpec}

import com.SparkSessionForTests
import org.scalatest.TestSuite
import scala.reflect.runtime.universe._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._

import com.github.mrpowers.spark.fast.tests.DataFrameComparer

import org.scalatest.Assertions._ // intercept

/**
 * List testing = https://www.baeldung.com/scala/scalatest-compare-collections
 */
class AboutSelect extends AnyFunSpec with Matchers  with SparkSessionForTests {

	import sparkTestsSession.implicits._

	val rows: Seq[Row] = flightDf.collect().toSeq
	//val thirdRow: Row = rows(2)

	// Identifying the types of the columns
	flightDf.schema.map(_.dataType.typeName) shouldEqual List("string", "string", "long")

	describe("Selecting"){

		it("simple selecting via column name"){

			val countCol: Seq[Long] = flightDf.select($"count").collect().toSeq.map(row => row.getAs[Long](0))
			val subsetCountCol: Seq[Long] = Seq(15, 1, 344, 15, 62, 1, 62, 588, 40, 1, 325).map(_.toLong)

			countCol should contain atLeastOneElementOf subsetCountCol

			// Another way to test:
			countCol.zip(subsetCountCol).filter{case (v1: Long, v2: Long) => v1 == v2}.length shouldEqual subsetCountCol.length
		}


		// TODO select by renaming. Example:
		// empDF.select($"*", sumTest as "running_total").show
		// Source = https://hyp.is/LMOsMpwxEe6XKGPBSFlVcw/alvinhenrick.com/2017/05/16/apache-spark-analytical-window-functions/
	}
}
