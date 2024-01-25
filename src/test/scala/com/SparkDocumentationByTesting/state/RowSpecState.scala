package com.SparkDocumentationByTesting.state

import com.data.util.EnumHub._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}

import utilities.SparkSessionWrapper

/**
 *
 */
object RowSpecState extends SparkSessionWrapper {


	val seaSchema: StructType = StructType(Seq(
		StructField(Animal.SeaCreature.toString, StringType),
		StructField("YearsOld", IntegerType),
		StructField(WaterType.toString, StringType),
		StructField("IsLiving", BooleanType)
	))

	val pearlTuple: (String, Int, String, Boolean) = (Animal.SeaCreature.Pearl.toString, 1031, WaterType.Saltwater.toString, false)
	val anemoneTuple: (String, Int, String, Boolean) = (Animal.SeaCreature.Anemone.toString, 190, WaterType.Saltwater.toString, false)
	val seahorseTuple: (String, Int, String, Boolean) = (Animal.SeaCreature.Seahorse.toString, 2, WaterType.Saltwater.toString, true)
	val shrimpTuple: (String, Int, String, Boolean) = (Animal.SeaCreature.Shrimp.toString, 1, WaterType.Freshwater.toString, true)

	val pearlGSRow: Row = new GenericRowWithSchema(pearlTuple.productIterator.toArray, seaSchema)
	val seahorseGSRow: Row = new GenericRowWithSchema(seahorseTuple.productIterator.toArray, seaSchema)
	val anemoneGSRow: Row = new GenericRowWithSchema(anemoneTuple.productIterator.toArray, seaSchema)
	val shrimpGSRow: Row = new GenericRowWithSchema(shrimpTuple.productIterator.toArray, seaSchema)

	val pearlGNRow: Row = new GenericRow(pearlTuple.productIterator.toArray)
	val seahorseGNRow: Row = new GenericRow(seahorseTuple.productIterator.toArray)
	val anemoneGNRow: Row = new GenericRow(anemoneTuple.productIterator.toArray)
	val shrimpGNRow: Row = new GenericRow(shrimpTuple.productIterator.toArray)

	val pearlRow: Row = Row(pearlTuple.productIterator.toList:_*)
	val seahorseRow: Row = Row(seahorseTuple.productIterator.toList:_*)
	val anemoneRow: Row = Row(anemoneTuple.productIterator.toSeq:_*)
	val shrimpRow: Row = Row(shrimpTuple.productIterator.toSeq)

	// ---------------------------------------------------------------------------

	import sparkSessionWrapper.implicits._
	import scala.jdk.CollectionConverters._

	val dfGenericRowWithSchema: DataFrame = Seq(pearlTuple, seahorseTuple, anemoneTuple, shrimpTuple).toDF(seaSchema.names:_*)
	/*sparkSessionWrapper.createDataFrame(Seq(
		pearlGSRow, seahorseGSRow, anemoneGSRow, shrimpGSRow
	).asJava, seaSchema)*/

	val dfGenericRow = sparkSessionWrapper.createDataFrame(Seq(
		pearlGNRow, seahorseGNRow, anemoneGNRow, shrimpGNRow
	).asJava, seaSchema )

	val dfRow = sparkSessionWrapper.createDataFrame(Seq(
		pearlRow, seahorseRow, anemoneRow, shrimpRow
	).asJava, seaSchema )


	// ---------------------------------------------------------------------------
}
