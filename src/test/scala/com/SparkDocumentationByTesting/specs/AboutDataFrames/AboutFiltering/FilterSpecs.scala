package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutFiltering

import com.SparkDocumentationByTesting.CustomMatchers
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import utilities.DFUtils.implicits._


//import com.SparkSessionForTests

/*import AnimalDf._
import TradeDf._*/
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper


/**
 * SOURCE: spark-test-repo:
 * 	- https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L617-L636
 */
class FilterSpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {

	/*import AnimalState._
	import FlightState._
	import TradeState._*/

	import sparkSessionWrapper.implicits._


	// TODO restructure later
	describe("Filtering"){



		// SOURCE: spark-test-repo = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L330-L414
		describe("using binary equality ops"){


			import com.data.util.DataHub.ManualDataFrames.XYNumDf.numDf
			import com.data.util.DataHub.ManualDataFrames.XYNumOptionDf.numOptionDf

			it("==="){


				val xOnesDf: Dataset[Row] = numDf.filter($"x" === 1)
				val expectedXOnes: Seq[Row] = numDf.collectAll.filter(row => row.getInt(0) == 1)

				val bothOnesDf: Dataset[Row] = numDf.filter(($"x" === 1) && ($"y" === 1))
				val expectedBothOnes: Seq[Row] = numDf.collectAll.filter(row => (row.getInt(0) == 1) && (row.getInt(1) == 1))

				xOnesDf.collectAll shouldEqual expectedXOnes
				xOnesDf.collectAll shouldEqual Seq(
					Row(1, 4), Row(1, 1), Row(1, 1), Row(1, 14), Row(1, 7), Row(1, 1)
				)

				bothOnesDf.collectAll  shouldEqual expectedBothOnes
				bothOnesDf.collectAll shouldEqual Seq(Row(1, 1), Row(1, 1), Row(1, 1))
			}

			it("<==> (equality that works for None)"){
				import com.data.util.DataHub.ManualDataFrames.XYNumOptionDf._

				numOptionDf.filter($"xn" === null).count() should equal(0)
				numOptionDf.filter($"xn" <=> null).collectAll shouldEqual Seq(
					Row(null, 1), Row(null, 8)
				)

			}
			it("=!=  (inequality)"){

				numDf.filter($"x" =!= 1).collectAll should contain allElementsOf numDf.collectAll.filter(row => row.getInt(0) != 1)


				// NOTE: does not work when there are nulls present
				numOptionDf.filter($"xn" =!= null).count() shouldEqual 0
				// but there are nulls
				numOptionDf.select($"xn").collectCol[Int].filter(_ != null).isEmpty shouldBe false
			}

			it(">, >="){

				//val expectedGreaterThanConst: Seq[Row] = Seq(Row(8, 8), Row(10, 2), Row(7, 8), Row(8, 9), Row(7, 10))
				val dfGreaterThanVal: Dataset[Row] = numDf.filter($"x" > 5)

				dfGreaterThanVal.collectAll should equal (Seq(Row(8, 8), Row(10, 2), Row(7, 8), Row(8, 9), Row(10, 7)))//(expectedGreaterThanConst)
				dfGreaterThanVal.collectAll should equal (numDf.collectAll.filter(row => row.getInt(0) > 5))


				val dfGreaterThanCol: Dataset[Row] = numDf.filter($"x" > $"y")
				dfGreaterThanCol.collectAll should equal (Seq(Row(4, 1), Row(10, 2), Row(5, 1), Row(4, 2), Row(10, 7)) )
				 dfGreaterThanCol.collectAll should equal (numDf.collectAll.filter(row => row.getInt(0) > row.getInt(1)))


				// Can distinguish even when there are nulls.
				numOptionDf.filter($"xn" > 5).collectAll should equal (Seq(
					Row(10, null), Row(7, 8), Row(8, null), Row(7, 10)
				))


				// ----------
				numDf.filter($"y" >= 10).collectAll shouldEqual numDf.collectAll.filter(row => row.getInt(1) >= 10)
			}

			it("<, <="){

				numDf.filter($"y" < 0).collectAll shouldEqual numDf.collectAll.filter(row => row.getInt(1) < 0)
				numDf.filter(($"y" < 10) && (col("x") >= 3)).collectAll shouldEqual numDf.collectAll.filter(row => (row.getInt(1) < 10) && (row.getInt(0) >= 3))
			}
		}


		//SOURCE: spark-test-repo = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L589-L615
		describe("using binary boolean ops"){

			import com.data.util.DataHub.ManualDataFrames.BooleanData._

			it("&&"){
				val dfATrue: Dataset[Row] = booleanDf.filter($"a" && true)
				dfATrue.collectAll shouldEqual booleanDf.collectAll.filter(row => row.getBoolean(0) && true)
				dfATrue.collectAll shouldEqual Seq(Row(true, true), Row(true, false))

				val dfAFalse: Dataset[Row] = booleanDf.filter($"a" && false)
				dfAFalse.collectAll shouldEqual booleanDf.collectAll.filter(row => row.getBoolean(0) && false)
				dfAFalse.collectAll shouldEqual Seq()

				val dfAB: Dataset[Row] = booleanDf.filter($"a" && $"b")
				dfAB.collectAll shouldEqual booleanDf.collectAll.filter(row => row.getAs[Boolean](0) && row.getAs[Boolean](1))
				dfAB.collectAll shouldEqual Seq(Row(true, true))
			}
			it("||"){
				val dfATrue: Dataset[Row] = booleanDf.filter($"a" || true)
				dfATrue.collectAll shouldEqual booleanDf.collectAll.filter(row => row.getBoolean(0) || true)
				dfATrue.collectAll shouldEqual booleanDf.collectAll

				val dfAFalse: Dataset[Row] = booleanDf.filter($"a" || false)
				dfAFalse.collectAll shouldEqual booleanDf.collectAll.filter(row => row.getBoolean(0) || false)
				dfAFalse.collectAll should equal (Seq(Row(true, true), Row(true, false)))
				//dfAFalse.collectAll should contain allElementsOf booleanDf.take(2)

				val dfAB: Dataset[Row] = booleanDf.filter($"a" || $"b")
				dfAB.collectAll shouldEqual booleanDf.collectAll.filter(row => row.getAs[Boolean](0) || row.getAs[Boolean](1))
				dfAB.collectAll shouldEqual Seq(Row(true, true), Row(true, false), Row(false, true))
				//dfAB.collectAll should contain allElementsOf booleanDf.take(3)
			}
		}


		// SOURCE: spark-test-repo =  https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L416-L428
		describe("using between()"){

			import com.data.util.DataHub.ManualDataFrames.XYNumDf._

			val expectedBetweenXY: Seq[Row] = betweenDf.collectAll.filter(row => (row.getInt(1) >= row.getInt(0)) && (row.getInt(1) <= row.getInt(2)))

			val dfBetweenXY: Dataset[Row] = betweenDf.filter($"b".between($"x", $"y"))

			dfBetweenXY.collectAll should equal (expectedBetweenXY)
			dfBetweenXY.collectAll should equal (Seq(
				Row(5,7,7), Row(8,8,8), Row(-9,-7,1), Row(-11, 0, 1), Row(1, 12, 14), Row(3,5,5), Row(1,1,7), Row(7, 8,8)
			))
			// between expects col args not string args
			betweenDf.filter($"b".between("x", "y")).count() should be(0)

			// ---

			val dfBetweenYX: Dataset[Row] = betweenDf.filter($"b".between($"y", $"x"))
			val expectedBetweenYX: Seq[Row] = betweenDf.collectAll.filter(row => (row.getInt(1) >= row.getInt(2)) && (row.getInt(1) <= row.getInt(0)))
			dfBetweenYX.collectAll should equal (expectedBetweenYX)
			dfBetweenYX.collectAll should equal (Seq(
				Row(8,8,8), Row(5,3,1), Row(4,3,2)
			))
		}

		describe("using isin(), isInCollection()"){

			import com.data.util.DataHub.ManualDataFrames.XYNumDf._

			it("works when columns have similar types"){
				val dfXIsIn: Dataset[Row] = numDf.filter($"x".isin(0, 5, 8))
				val dfXIsInIter: Dataset[Row] = numDf.filter($"x".isInCollection(List(0, 5, 8)))
				val expectedXIsIn: Seq[Row] = numDf.collectAll.filter(row => row.getInt(0) == 0 || row.getInt(0) == 5 || row.getInt(0) == 8)

				dfXIsIn should equalDataFrame(dfXIsInIter)
				dfXIsIn.collectAll should equal(expectedXIsIn)
				dfXIsIn.collectAll shouldEqual Seq(
					Row(5, 7), Row(8, 8), Row(5, 1), Row(8, 9)
				)
				// ---
				val dfYIsIn: Dataset[Row] = numDf.filter($"y".isin(0, 5, 8))
				val expectedYIsIn: Seq[Row] = numDf.collectAll.filter(row => row.getInt(1) == 0 || row.getInt(1) == 5 || row.getInt(1) == 8)

				dfYIsIn.collectAll shouldEqual expectedYIsIn
				dfYIsIn.collectAll shouldEqual Seq(
					Row(8, 8), Row(3, 5), Row(7, 8)
				)

				// TODO verify auto-casting: https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L445-L451
			}

			it("throws exception when cols have too-different types"){
				import org.apache.spark.sql.AnalysisException

				val df: DataFrame = Seq((1, Seq(1)), (2, Seq(2)), (3, Seq(3))).toDF("a", "b")

				val err = intercept[AnalysisException]{
					df.filter($"a".isin($"b"))
				}
				err.getMessage should (include("[DATATYPE_MISMATCH.DATA_DIFF_TYPES] Cannot resolve \"(a IN (b))\" due to data type mismatch: Input to `in` should all be the same type, but it's [\"INT\", \"ARRAY<INT>\"]"))


			}
		}
	}

}
