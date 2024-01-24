package com.SparkDocumentationByTesting.specs.AboutDataFrames


import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import utilities.DFUtils
import DFUtils.implicits._
import DFUtils.TypeAbstractions._


//import com.SparkSessionForTests
import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._
import AnimalDf._
import TradeDf._
import com.data.util.EnumHub._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._

import utilities.SparkSessionWrapper




/**
 *
 */
class AboutSchemaSpecs extends AnyFunSpec with Matchers  with SparkSessionWrapper {


	import com.SparkDocumentationByTesting.state.SpecState._
	import AnimalState._
	import sparkSessionWrapper.implicits._


	describe("Schema"){

		it("has fields"){
			animalDf.schema.fields shouldBe a [Array[StructField]]

			animalDf.schema.fields should contain allElementsOf(Array(
				StructField("Animal", StringType, true),
				StructField("Amount", IntegerType, true),
				StructField("Country", StringType, true),
				StructField("Climate", StringType, true))
			)

			animalDf.schema.fields should equal (animalSchema.fields)
		}
		it("has field names"){

			val names1: Seq[String] = animalDf.schema.fields.map(_.name).toList
			val names2: Seq[String] = animalDf.schema.fieldNames.toList
			val names3: Seq[NameOfCol] = colnamesAnimal
			val names4: Seq[String] = animalSchema.names.toList

			val theNames: Seq[Seq[NameOfCol]] = List(names1, names2, names3, names4).map(_.asInstanceOf[Seq[NameOfCol]])

			theNames.map(nlst => nlst shouldBe a [ Seq[NameOfCol]])

			theNames.map(nlst => nlst should contain allElementsOf colnamesAnimal )

		}

		it("has field types"){

			/*val types1: Array[DataType] = animalDf.schema.fields.map(_.dataType)
			//val types2: Array[String] = animalDf.schema.typeName
			val types3: List[DataType] = coltypesAnimal
			val types4: Array[DataType] = animalSchema.fields.map(_.dataType)*/
			val types1: Seq[DataType] = animalDf.schema.fields.map(_.dataType).toList
			//val types2: Array[String] = animalDf.schema.typeName
			val types3: Seq[DataType] = coltypesAnimal.toList
			val types4: Seq[DataType] = animalSchema.fields.map(_.dataType).toList

			val theTypes: Seq[Seq[DataType]] = List(types1, types3, types4).map(_.asInstanceOf[Seq[DataType]])

			theTypes.map(nlst => nlst shouldBe a[Seq[DataType]])

			theTypes.map(nlst => nlst should contain allElementsOf coltypesAnimal)
		}
	}

}
