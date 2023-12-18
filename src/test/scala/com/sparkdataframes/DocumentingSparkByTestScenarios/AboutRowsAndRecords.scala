package com.sparkdataframes.DocumentingSparkByTestScenarios

import com.SparkSessionForTests
import com.sparkdataframes.DocumentingSparkByTestScenarios.TestData._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

//import util.DataFrameCheckUtils._
import scala.reflect.runtime.universe._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._

import org.scalatest.Assertions._ // intercept

/**
 *
 */
class AboutRowsAndRecords extends AnyFunSpec with Matchers  with SparkSessionForTests {

	// TODO find out if need to share spark session to make tests run

	import sparkTestsSession.implicits._


	val rows: Seq[Row] = flightDf.collect().toSeq
	val thirdRow: Row = rows(2)

	// Identifying the types of the columns
	flightDf.schema.map(_.dataType.typeName) shouldEqual List("string", "string", "long")

	describe("Rows") {

		describe("Accessing rows"){


			it("get(i) should return the value at position i in the Row with Any type"){

				thirdRow.get(1) shouldEqual "Ireland"
				thirdRow.get(1).isInstanceOf[Any] should be (true)

				thirdRow.get(2) shouldEqual 344
				thirdRow.get(2) shouldBe a[Any]
			}

			it("getAs[T] lets you specify the type of the item you want to get"){

				rows(4).getAs[String](0) shouldEqual "United States"
				rows(4).getAs[String](0) shouldBe a[String]

				rows(11).getAs[Long](2) shouldEqual 39
				rows(11).getAs[Long](2) shouldBe a[Long]

				// Cannot get a type that doesn't match the one specified in the function
				val catchingException = intercept[ClassCastException] {
					rows(3).getAs[String](2)
				}
				catchingException.isInstanceOf[ClassCastException] should be(true)
			}

			it("specialized get functions let you return the item with a type also"){
				rows(11).getLong(2) shouldEqual 39
				rows(11).getLong(2) shouldBe a[Long]

				// Cannot get a type that doesn't match the one specified in the function
				val catchingException = intercept[ClassCastException] {
					rows(3).getInt(2)
				}
				catchingException.isInstanceOf[ClassCastException] should be(true)

			}
		}
	}

}
