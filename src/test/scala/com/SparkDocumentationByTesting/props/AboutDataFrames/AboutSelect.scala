package com.SparkDocumentationByTesting.props.AboutDataFrames


import org.apache.spark.sql.{DataFrame, Row, ColumnName}
import org.apache.spark.sql.functions._

import utilities.DFUtils

//import com.SparkSessionForTests
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper // intercept


import com.data.util.DataHub._
import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._
import com.data.util.EnumHub._



import org.scalacheck._
import org.scalacheck.Prop.forAll
import org.scalatestplus.scalacheck.Checkers.check

trait AnyFunSpecAsTrait extends AnyFunSpec

/**
 *
 */
class AboutSelect extends Properties("AboutSelect") with AnyFunSpecAsTrait with Matchers  with SparkSessionWrapper{


	import sparkSessionWrapper.implicits._

	import com.SparkDocumentationByTesting.state.StateAboutSelect._

	describe("Selecting"){

		it("selecting by column name"){

			// NOTE: cannot pass type dynamically to function ... will have to just choose randomly from String colnames instead to preserve randomness but maintain type staticness.... :( ????
			val genStrColname: Gen[NameOfCol] = Gen.oneOf(DFUtils.getColnamesWithType[String](flightDf))


			def propSelectByColname(df: DataFrame, colnameToIndexMap: Map[String, Int]) = property("select by column name") = {
				val p0 = Prop.forAll(genStrColname) { anyStrColname: String =>
					//val colByOverallCollect: Seq[Long] = flightDf.collect().toSeq.map(row => row.getAs[Long](F.C3))
					//val colByOverallCollect: Seq[Long] = flightDf.collect().toSeq.map(row => row.getAs[Long](F.nameIndexMap(n)))
					val colByOverallCollect: Seq[String] = df.collect().toSeq.map(row => row.getAs[String](colnameToIndexMap(anyStrColname)))
					val colByNameSelect: Seq[String] = df.select(anyStrColname).collect().toSeq.map(row => row.getAs[String](0)) // use simple id = 0 because already selecting one column so the row will have length = 1

					val lenRowByOverallCollect: Int = df.collect().head.size
					val lenRowByNameSelect: Int = df.select($"count").collect().head.size

					colByNameSelect shouldBe a[Seq[String]]
					colByOverallCollect shouldBe a[Seq[Long]]
					colByNameSelect should equal(colByOverallCollect)

					lenRowByOverallCollect should equal(flightDf.columns.length)
					lenRowByNameSelect should equal(1)
					lenRowByOverallCollect should be >= lenRowByNameSelect
				}
				//p0.check()
				p0
			}


			val colByOverallCollect: Seq[Long] = flightDf.collect().toSeq.map(row => row.getAs[Long](F.C3))
			val colByNameSelect: Seq[Long] = flightDf.select($"count").collect().toSeq.map(row => row.getAs[Long](0)) // use simple id = 0 because already selecting one column so the row will have length = 1

			val lenRowByOverallCollect: Int = flightDf.collect().head.size
			val lenRowByNameSelect: Int = flightDf.select($"count").collect().head.size

			colByNameSelect shouldBe a[Seq[Long]]
			colByOverallCollect shouldBe a[Seq[Long]]
			colByNameSelect should equal(colByOverallCollect)

			lenRowByOverallCollect should equal(flightDf.columns.length)
			lenRowByNameSelect should equal(1)
			lenRowByOverallCollect should be >= lenRowByNameSelect
		}
	}


}
