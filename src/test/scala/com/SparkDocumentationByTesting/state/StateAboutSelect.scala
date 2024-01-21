package com.SparkDocumentationByTesting.state



import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import utilities.DFUtils
import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._
import com.data.util.EnumHub._
import org.apache.spark.sql.types.DataType

/**
 *
 */

object StateAboutSelect {


	object F { // state object for flightData
		val rows: Seq[Row] = flightDf.collect().toSeq

		val C1: Int = rows.head.fieldIndex(flightDf.columns(0))
		val C2: Int = rows.head.fieldIndex(flightDf.columns(1))
		val C3: Int = rows.head.fieldIndex(flightDf.columns(2))

		/*val C0 = rows.head.fieldIndex("ORIGIN_COUNTRY_NAME")
		val C1 = rows.head.fieldIndex("DEST_COUNTRY_NAME")
		val C2 = rows.head.fieldIndex("count")*/

		val nameIndexMap: Map[String, Int] = DFUtils.colnamesToIndices(flightDf)


		val nts: Array[(String, DataType)] = flightDf.columns.zip(flightDf.schema.fields.map(_.dataType))
	}

	object A { // state object for animal data

		val rows: Seq[Row] = animalDf.collect().toSeq

		val C1: Int = rows.head.fieldIndex(colnamesAnimal(0))
		val C2: Int = rows.head.fieldIndex(colnamesAnimal(1))
		val C3: Int = rows.head.fieldIndex(colnamesAnimal(2))
		val C4: Int = rows.head.fieldIndex(colnamesAnimal(3))

		val nameIndexMap: Map[String, Int] = DFUtils.colnamesToIndices(animalDf)
	}
}
