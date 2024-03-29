package com.SparkDocumentationByTesting

//import org.apache.spark.sql._
import org.apache.spark.sql.{Column, ColumnName, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, column, expr, row_number, rank, dense_rank, percent_rank, ntile, cume_dist, lag, lead, min, max, avg, sum, count}
// rangeBetween, rowsBetween


import scala.util.{Try, Failure, Success}

import scala.reflect.runtime.universe._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import org.scalatest._
import org.scalatest.matchers._
import org.scalatest.Assertions._ //intercept

import org.apache.spark.sql.expressions.{Window, WindowSpec}

import com.github.mrpowers.spark.fast.tests.DataFrameComparer

/**
 * SOURCES:
 *
 * https://hyp.is/Q-Vosp32Ee6FKAPdauMFlQ/www.scalatest.org/user_guide/using_matchers
 * https://spin.atomicobject.com/2021/03/08/scalatest-custom-matchers/
 */

//object CustomMatchers extends CustomMatchers
trait CustomMatchers /*extends Matchers*/ {

	def equalDataFrame(expectedDf: DataFrame): DataFrameMatcher = new DataFrameMatcher(expectedDf)

	object Comparer extends DataFrameComparer {
		def booleanDataFrameEqualityChecker(df1: DataFrame, df2: DataFrame): Boolean = {

			/*(df1.schema == df2.schema) &&
				(df1.collect().sameElements(df2.collect()))*/
			val resTry: Try[Unit] = Try(assertSmallDataFrameEquality(df1, df2))

			resTry.isSuccess /*match {
				case Success(()) => true
				case Failure(_) => false
			}*/
			/*val result = intercept[Exception] {
				assertSmallDataFrameEquality(df1, df2)
			}

			! result.isInstanceOf[Exception] //if it is return true else false*/
		}
	}

	class DataFrameMatcher(expectedDf: DataFrame) extends Matcher[DataFrame]  {

		def apply(inputDf: DataFrame): MatchResult = {
			MatchResult(
				Comparer.booleanDataFrameEqualityChecker(inputDf, expectedDf),
				"The ipnutDf did not equal the expectedDf",
				"The inputDf equals the expectedDf when it should not"
			)
		}
	}
	//def equalDataFrame(expectedDf: DataFrame): DataFrameMatcher = new DataFrameMatcher(expectedDf)
}

object CustomMatchers extends CustomMatchers
