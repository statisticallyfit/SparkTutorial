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
 * SOURCE: spark-test-repo:
 * 	- https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L617-L636
 */
class AboutColumnSpecs extends AnyFunSpec with Matchers with SparkSessionWrapper {


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


		describe("operations on columns giving rise to new columns"){

			import scala.jdk.CollectionConverters._
			import scala.util.Random

			val n = 10

			val xs: Seq[Int] = Seq.fill(n)(Random.between(0, 20))
			val ys: Seq[Int] = Seq.fill(n)(Random.between(0, 20))
			val zs: Seq[Int] = xs.zip(ys).map { case (x, y) => x + y }

			val sch: StructType = DFUtils.createSchema(names = Seq("x", "y", "z"), types = Seq(IntegerType, IntegerType, IntegerType))

			val seqOfRows: Seq[Row] = Seq(xs, ys, zs).transpose.map(Row(_: _*))
			val df: DataFrame = sparkSessionWrapper.createDataFrame(seqOfRows.asJava, sch)


			it("unary op on a column"){

				df.select(-$"a").collectCol[Int] shouldEqual df.collect().toSeq.map(row => Row(-row.getInt(0)))

				val (t, f) = (true, false)
				val dfbool = Seq(t, t, t, f, f, t, f, t, f, t, t, t, t ,f, t, f, t, t, t, t).toDF("booleans")
				dfbool.select(!$"booleans").collectCol[Boolean] shouldEqual df.collect().toSeq.map(row => Row(!row.getBoolean(0)))
			}

			// SOURCE: spark test repo:- https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L154
			it("binary op between two existing columns"){

				val zAdd: Seq[Int] = df.select(df("x") + df("y").as("z2")).collectCol[Int]
				val zCheck: Seq[Int] = df.select(df("z")).collectCol[Int]

				zAdd shouldEqual zCheck

				df.select($"x" + $"y" + 3).collectCol[Int] shouldEqual df.collect().toSeq.map(row => Row(row.getInt(0) + row.getInt(1) + 3))
				df.select($"x" - $"y" - 3).collectCol[Int] shouldEqual df.collect().toSeq.map(row => Row(row.getInt(0) - row.getInt(1) - 3))
				df.select($"x" * $"y" * 3).collectCol[Int] shouldEqual df.collect().toSeq.map(row => Row(row.getInt(0) * row.getInt(1) * 3))
				df.select($"x" / $"y" + 1).collectCol[Double] shouldEqual df.collect().toSeq.map(row => Row(row.getInt(0).toDouble / row.getInt(1).toDouble + 1))
				df.select($"x" % $"y" + 2).collectCol[Int] shouldEqual df.collect().toSeq.map(row => Row(row.getInt(0) % row.getInt(1) + 2))
			}

			// SOURCE: spark-test-repo: https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L199-L260
			it("binary op between existing column and another operand"){

				df.select($"x" + 1).collectCol[Int] shouldEqual df.collect().toSeq.map(row => Row(row.getInt(0) + 1))
				df.select($"x" - 1).collectCol[Int] shouldEqual df.collect().toSeq.map(row => Row(row.getInt(0) - 1))
				df.select($"x" * 2).collectCol[Int] shouldEqual df.collect().toSeq.map(row => Row(row.getInt(0) * 2))
				df.select($"x" / 5).collectCol[Double] shouldEqual df.collect().toSeq.map(row => Row(row.getInt(0).toDouble / 5))
				df.select($"x" % 5).collectCol[Int] shouldEqual df.collect().toSeq.map(row => Row(row.getInt(0) % 5))

			}
		}

		// TODO left off here = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L199
	}


}
