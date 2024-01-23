package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import utilities.SparkSessionWrapper

/**
 *
 */
class AboutColumnsSpecs extends AnyFunSpec with Matchers with SparkSessionWrapper {


	//F.nameIndexMap("ORIGIN_COUNTRY_NAME") should equal(F.C0) // TODO map these out in the "AboutColumns tests"
	//F.C0 should equal (F.rows.head.fieldIndex("ORIGIN_COUNTRY_NAME"))

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
