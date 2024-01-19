package com.SparkDocumentationByTesting.AboutDataFrames

import org.apache.spark.sql.Row

//import com.SparkSessionForTests
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper // intercept



// TODO have this class underneath a trait called AboutColumns

/**
 * List testing = https://www.baeldung.com/scala/scalatest-compare-collections
 */
class AboutSelect extends AnyFunSpec with Matchers  with SparkSessionWrapper {

	import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
	import sparkSessionWrapper.implicits._


	val rows: Seq[Row] = flightDf.collect().toSeq
	//val thirdRow: Row = rows(2)

	// Identifying the types of the columns
	flightDf.schema.map(_.dataType.typeName) shouldEqual List("string", "string", "long")


	// TODO - test simple select via $, "", col, df.col, column(), ', expr, expr.alias, selectExpr
	// TODO test select multiple cols
	// TODO permutate above two

	describe("Selecting"){

		it("simple selecting via column name"){

			val countCol1: Seq[Long] = flightDf.collect().toSeq.map(row => row.getAs[Long](2))
			val countCol0: Seq[Long] = flightDf.select($"count").collect().toSeq.map(row => row.getAs[Long](0))
			val subsetCountCol: Seq[Long] = Seq(15, 1, 344, 15, 62, 1, 62, 588, 40, 1, 325).map(_.toLong) // TODO substitute this with take()

			countCol0 should contain atLeastOneElementOf subsetCountCol

			println("test1s")
			// Another way to test:
			countCol0.zip(subsetCountCol).filter{case (v1: Long, v2: Long) => v1 == v2}.length shouldEqual subsetCountCol.length
		}


		// TODO select by renaming. Example:
		// empDF.select($"*", sumTest as "running_total").show
		// Source = https://hyp.is/LMOsMpwxEe6XKGPBSFlVcw/alvinhenrick.com/2017/05/16/apache-spark-analytical-window-functions/
	}
}

