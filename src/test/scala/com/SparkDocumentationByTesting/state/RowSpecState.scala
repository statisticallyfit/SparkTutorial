package com.SparkDocumentationByTesting.state


import utilities.EnumHub._
import utilities.EnumUtils.implicits._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}

import utilities.SparkSessionWrapper

/**
 *
 */
object RowSpecState extends SparkSessionWrapper {


	val seaSchema: StructType = StructType(Seq(
		StructField(Animal.SeaCreature.enumName, StringType),
		StructField("YearsOld", IntegerType),
		StructField(Biome.Marine.enumName, StringType),
		StructField("IsLiving", BooleanType)
	))

	val pearlTuple: (String, Int, String, Boolean) = (Animal.SeaCreature.Pearl.enumName, 1031, Biome.Marine.Saltwater.Seashore.enumName, false)
	val anemoneTuple: (String, Int, String, Boolean) = (Animal.SeaCreature.Anemone.enumName, 190, Biome.Marine.Saltwater.Ocean.enumName, false)
	val seahorseTuple: (String, Int, String, Boolean) = (Animal.SeaCreature.Seahorse.enumName, 2, Biome.Marine.Saltwater.enumName, true)
	val shrimpTuple: (String, Int, String, Boolean) = (Animal.SeaCreature.Shrimp.enumName, 1, Biome.Marine.Freshwater.Pond.enumName, true)

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
	val shrimpRow: Row = Row(shrimpTuple.productIterator.toSeq:_*) //NOTE cannot accept simple list?

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
