package com.SparkDocumentationByTesting.specs.AboutDataFrames

import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import org.apache.spark.sql.Row

//import com.SparkSessionForTests
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper // intercept

/**
 *
 */
class AboutRowsSpecs extends AnyFunSpec with Matchers  with SparkSessionWrapper {


	val rows: Seq[Row] = flightDf.collect().toSeq
	val thirdRow: Row = rows(2)

	// Identifying the types of the columns
	flightDf.schema.map(_.dataType.typeName) shouldEqual List("string", "string", "long")

	describe("Rows") {

		describe("Accessing rows"){

			/**
			 * SOURCE:
			 * 	- BillChambers_Chp5
			 */
			it("get(i) should return the value at position i in the Row with Any type"){

				thirdRow.get(1) shouldEqual "Ireland"
				thirdRow.get(1).isInstanceOf[Any] should be (true)

				thirdRow.get(2) shouldEqual 344
				thirdRow.get(2) shouldBe a[Any]
			}

			/**
			 * SOURCE:
			 * 	- BillChambers_Chp5
			 */
			it("getAs[T] lets you specify the type of the item you want to get"){

				rows(4).getAs[String](0) shouldEqual "United States"
				rows(4).getAs[String](0) shouldBe a[String]

				rows(2).get(1).asInstanceOf[String] shouldEqual rows(2).getAs[String](1)

				rows(11).getAs[Long](2) shouldEqual 39
				rows(11).getAs[Long](2) shouldBe a[Long]
				rows(11).get(2).asInstanceOf[Long] shouldBe a[Long]

				// Cannot get a type that doesn't match the one specified in the function
				val catchingException = intercept[ClassCastException] {
					rows(3).getAs[String](2)
				}
				catchingException.isInstanceOf[ClassCastException] should be(true)
			}

			/**
			 * SOURCE:
			 * 	- BillChambers_Chp5
			 */
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
