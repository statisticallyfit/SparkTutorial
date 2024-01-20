package com.SparkDocumentationByTesting.AboutDataFrames


import org.apache.spark.sql.Row
import utilities.DFUtils

//import com.SparkSessionForTests
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper // intercept


import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._


/**
 *
 */


object StateForColumns {

	object F { // state object for flightData
		val rows: Seq[Row] = flightDf.collect().toSeq

		val C0 = rows.head.fieldIndex(flightDf.columns(0))
		val C1 = rows.head.fieldIndex(flightDf.columns(1))
		val C2 = rows.head.fieldIndex(flightDf.columns(2))

		/*val C0 = rows.head.fieldIndex("ORIGIN_COUNTRY_NAME")
		val C1 = rows.head.fieldIndex("DEST_COUNTRY_NAME")
		val C2 = rows.head.fieldIndex("count")*/

		val nameIndexMap: Map[String, Int] = DFUtils.colnamesToIndices(flightDf)
	}

	object A { // state object for animal data

		val rows: Seq[Row] = animalDf.collect().toSeq

		val C0: Int = rows.head.fieldIndex(colnamesAnimal(0))
		val C1: Int = rows.head.fieldIndex(colnamesAnimal(1))
		val C2: Int = rows.head.fieldIndex(colnamesAnimal(2))
		val C3: Int = rows.head.fieldIndex(colnamesAnimal(3))

		val nameIndexMap: Map[String, Int] = DFUtils.colnamesToIndices(animalDf)
	}
}

class AboutColumns extends AnyFunSpec with Matchers  with SparkSessionWrapper{


	import com.SparkDocumentationByTesting.AboutDataFrames.StateForColumns._


	F.nameIndexMap("ORIGIN_COUNTRY_NAME") should equal(F.C0) // TODO map these out in the "AboutColumns tests"
	F.C0 should equal (F.rows.head.fieldIndex("ORIGIN_COUNTRY_NAME"))

	// TODO make names -> ids
	// TODO use row.fieldIndex("colname") // https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RowTest.scala#L59

	// Identifying the types of the columns
	/*

	val C0 = rowsFlight.head.fieldIndex("ORIGIN_COUNTRY_NAME")
	val C1 = rowsFlight.head.fieldIndex("DEST_COUNTRY_NAME")
	val C2 = rowsFlight.head.fieldIndex("count")

	val cif: Map[String, Int] = DFUtils.colnamesToIndices(flightDf)
	cif("ORIGIN_COUNTRY_NAME") should equal(C0) // TODO map these out in the "AboutColumns tests"*/
}
