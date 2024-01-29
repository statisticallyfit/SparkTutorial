package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns


import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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
 * 	- https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L617-L636
 */
class AboutColumnSpecs extends AnyFunSpec with Matchers with SparkSessionWrapper {


	import com.SparkDocumentationByTesting.state.SpecState._
	/*import AnimalState._
	import FlightState._
	import TradeState._*/

	import sparkSessionWrapper.implicits._


	describe("Column operations"){

		/**
		 * SOURCE: spark test repo
		 * 	- https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L154
		 */
		it("binary op between columns giving rise to new column"){

			import scala.jdk.CollectionConverters._
			import scala.util.Random

			val n = 10

			val xs: Seq[Int] = Seq.fill(n)(Random.between(0, 20))
			val ys: Seq[Int] = Seq.fill(n)(Random.between(0, 20))
			val zs: Seq[Int] = xs.zip(ys).map { case (x, y) => x + y }

			val sch: StructType = DFUtils.createSchema(names = Seq("x", "y", "z"), types = Seq(IntegerType, IntegerType, IntegerType))

			val seqOfRows: Seq[Row] = Seq(xs, ys, zs).transpose.map(Row(_: _*))
			val df: DataFrame = sparkSessionWrapper.createDataFrame(seqOfRows.asJava, sch)

			val zAdd: Seq[Int] = df.select(df("x") + df("y").as("z2")).collectCol[Int]
			val zCheck: Seq[Int] = df.select(df("z")).collectCol[Int]

			zAdd shouldEqual zCheck
		}

		// TODO left off here = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L199
	}


}
