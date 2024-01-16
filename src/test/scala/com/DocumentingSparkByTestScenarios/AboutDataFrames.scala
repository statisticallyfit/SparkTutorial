package com.DocumentingSparkByTestScenarios


import org.apache.spark.sql.{Column, ColumnName, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._ // .{StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD


import scala.reflect.runtime.universe._

import com.SparkSessionForTests
import com.DocumentingSparkByTestScenarios.CustomMatchers

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import org.scalatest.Assertions._
import org.scalatest.TestSuite

import com.github.mrpowers.spark.fast.tests.DataFrameComparer


/**
 *
 */
class AboutDataFrames extends AnyFunSpec with Matchers with TestSuite
	/*with CustomMatchers*/ // use object import custom matchers
	with SparkSessionForTests
	/*with DataFrameComparer*/ {

	import sparkTestsSession.implicits._


	import CreatingDataFrames._
	import CustomMatchers._


	import com.data.util.DataHub.ManualDataFrames.fromEnums.{raw, dfs}
	//import com.data.util.DataHub.ManualDataFrames.fromAlvinHenrickBlog._


	describe("Creating data frames") {

		describe("using sparkSession's `createDataFrame()`"){

			it("on a scala Seq") {
				val resultDf: DataFrame = usingSessionCreateDataFrameOnSequence(sparkTestsSession, raw.businessStrSeq, raw.colnamesBusiness)

				//assertSmallDataFrameEquality(actualDF = resultDf, expectedDF = Dfs.businessDf)
				resultDf should equalDataFrame (dfs.businessDf)

			}
			it("on a scala Seq of Rows, with Schema") {
				val resultDf: DataFrame = usingSessionCreateDataFrameOnSequenceOfRowsWithSchema(sparkTestsSession, raw.businessRowSeq, raw.colnamesBusiness, raw.coltypesBusiness)

				resultDf should equalDataFrame(dfs.businessDf)
			}

			it("on RDD") {
				val resultDf: DataFrame = usingSessionCreateDataFrameOnRDD(sparkTestsSession, raw.businessStrRDD, raw.colnamesBusiness)

				resultDf should equalDataFrame(dfs.businessDf)
			}

			it("on RDD of Rows, with schema") {
				val resultDf: DataFrame = usingSessionCreateDataFrameOnRowRDDAndSchema(sparkTestsSession, raw.businessRowRDD, raw.colnamesBusiness, raw.coltypesBusiness)

				resultDf should equalDataFrame(dfs.businessDf)
			}
		}

		describe("using `toDF()`"){

			it("should use `toDF()` on RDD") {
				val resultDf: DataFrame = usingToDFOnRDD(sparkTestsSession, raw.businessStrRDD, raw.colnamesBusiness)

				resultDf should equalDataFrame(dfs.businessDf)
			}
			it("should use `toDF()` on Seq") {
				val resultDf: DataFrame = usingToDFOnSeq(sparkTestsSession, raw.businessStrSeq, raw.colnamesBusiness)._2

				resultDf should equalDataFrame(dfs.businessDf)
			}
		}
	}

	describe("Creating data frames (using input sources)"){

		import com.data.util.DataHub.ImportedDataFrames._

		// sparkMainSession.read.format(FORMAT_JSON).load(s"$PATH/$folderBillChambers/flight-data/json/2015-summary.json")
		val filepathFlightData: String = s"$PATH/$folderBillChambers/flight-data/json/2015-summary.json"


		describe("by reading CSV file") {
			val resultDf: DataFrame = usingReadFileByCSV(sparkTestsSession,  filepathFlightData)._2

			resultDf should equalDataFrame(dfs.businessDf)
		}

		describe("by reading TXT file") {
			val resultDf: DataFrame = usingReadFileByCSVWithSchema(sparkTestsSession, filepathFlightData, raw.colnamesBusiness, raw.coltypesBusiness)

			resultDf should equalDataFrame(dfs.businessDf)
		}
		describe("by reading JSON file") {
			val resultDf: DataFrame = usingReadJSONFile(sparkTestsSession, filepathFlightData)

			resultDf should equalDataFrame(dfs.businessDf)
		}
	}

}
/*
import com.SparkSessionForTests
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import org.scalatest.Assertions._ // intercept*/

//object tester extends AnyFunSpec with Matchers with com.DocumentingSparkByTestScenarios.CustomMatchers with SparkSessionForTests with App {
//
//
//	import sparkTestsSession.implicits._
//	val cs = List("a", "b", "c", "d")
//	val df1 = Seq((1, 3, "hi", 4), (1, 9, "string", 90)).toDF(cs:_*)
//	val df2 = Seq((1, 3, "hi", 4), (1, 9, "string", 90)).toDF(cs:_*)
//
//	def doIt = df1 should equalDataFrame (df2)
//
//	println(doIt)
//}


object CreatingDataFrames {

	def usingSessionCreateDataFrameOnSequence[T <: Product : TypeTag](spark: SparkSession, seq: Seq[T], colnames: Seq[String]): DataFrame	= {
		//import spark.implicits._
		val df: DataFrame = spark.createDataFrame(seq).toDF(colnames:_*)

		df.printSchema()
		df.show()

		df
	}

	/**
	 * `createDataFrame()` has another signature in spark which takes the util.List of Row type and schema for
	 * ccolumn names as arguments.
	 */
	def usingSessionCreateDataFrameOnSequenceOfRowsWithSchema(spark: SparkSession, seqOfRows: Seq[Row], colnames: Seq[String], coltypes: Seq[DataType]): DataFrame = {

		// NOTE: need to use "JavaConversions" not "JavaConverters" so that the createDataFrame from sequence of rows will work.
		// Sinec scala 2.13 need to use this other import instead: https://stackoverflow.com/a/6357299
		//import scala.collection.JavaConverters._
		import scala.jdk.CollectionConverters._
		//import scala.collection.JavaConversions._

		val schema: StructType = StructType(
			colnames.zip(coltypes).map{case (n, t) => StructField(n, t)}
		)
		val df: DataFrame = spark.createDataFrame(rows = seqOfRows.asJava, schema = schema)

		df.printSchema()
		df.show()
		df
	}


	def usingSessionCreateDataFrameOnRDD[T <: Product : TypeTag](spark: SparkSession, rdd: RDD[T], colnames: Seq[String]): DataFrame = {
		//import spark.implicits._

		val df: DataFrame = spark.createDataFrame(rdd).toDF(colnames: _*)

		df.printSchema()
		df.show()

		df
	}

	/**
	 * `createDataFrame()` has another signature which takes RDD[Row] and a schema for colnames as arguments.
	 * To use, must first
	 * 	1. convert rdd object from RDD[T] to RDD[Row], and
	 * 	2. define a schema using `StructType` and `StructField`
	 */
	def usingSessionCreateDataFrameOnRowRDDAndSchema(spark: SparkSession, rowRDD: RDD[Row], colnames: Seq[String], coltypes: Seq[DataType]): DataFrame = {

		val schema: StructType = StructType(
			colnames.zip(coltypes).map { case (n, t) => StructField(n, t, nullable = true) }
		)
		val df: DataFrame = spark.createDataFrame(rowRDD = rowRDD, schema = schema)

		df.printSchema()
		df.show()

		df
	}

	// TODO - why cannot make RDD[(A, B)] ? instead of string, string?
	def usingToDFOnRDD[T <: Product : TypeTag](spark: SparkSession, rdd: RDD[T], colnames: Seq[String]): DataFrame = {

		import spark.implicits._

		val df_noname: DataFrame = rdd.toDF() // default colnames are _1, _2
		df_noname.printSchema()
		df_noname.show() // show all the rows box format
		assert(df_noname.columns.toList == List("_1", "_2")) // TODO false if comparing arrays??

		val df: DataFrame = rdd.toDF(colnames:_*) // assigning colnames
		df.printSchema()
		df.show()
		assert(df.columns.toList == colnames)

		df
	}


	def usingToDFOnSeq[T <: Product : TypeTag](spark: SparkSession, seq: Seq[T]/*[(String, String)]*/, colnames: Seq[String]): (DataFrame, DataFrame) = {
		import spark.implicits._

		val df_noname: DataFrame = seq.toDF()
		val df: DataFrame = seq.toDF(colnames: _*)

		df.printSchema()
		df.show()

		(df_noname, df)
	}



	// NOTE: to read in multiple csv files, separate their file names with comma = https://hyp.is/ceetdpWBEey3Rnd9naElZQ/sparkbyexamples.com/spark/spark-read-csv-file-into-dataframe/
	// NOTE to read in all csv files from a folder, must pass in the entire directory name = https://hyp.is/kh1dZpWBEeyggz93IvgE_w/sparkbyexamples.com/spark/spark-read-csv-file-into-dataframe/
	type Df = DataFrame
	def usingReadFileByCSV(spark: SparkSession, filepath: String): Tuple4[Df, Df, Df, Df] = {
		val df_noheader: DataFrame = spark.read.csv(filepath)

		val df_header: DataFrame = spark
			.read
			.option(key = "header", value = true)
			.csv(path = filepath)

		val df_delim: DataFrame = spark
			.read
			.options(Map("delimiter" -> ","))
			.option(key = "header", value = true) // can still get colnames
			.csv(path = filepath)

		// setting this inferSchema = true infers the column types based on the data
		val df_inferSchema: DataFrame = spark
			.read
			.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
			.csv(filepath)

		df_header.printSchema()
		df_header.show()

		(df_noheader, df_header, df_delim, df_inferSchema)
	}

	// TODO read with quotes / nullvalues / dateformat = https://hyp.is/Gpl27Jh2Eey9h_sXVK2vZA/sparkbyexamples.com/spark/spark-read-csv-file-into-dataframe/


	/**
	 * Use if you know the schema of the file ahead of time and do not want to use the `inferSchema` option
	 * for column names and types. Can use a user-defined custom schema.
	 *
	 * @param spark
	 * @param filepath
	 * @return
	 */
	//import org.apache.spark.sql.types.AtomicType

	// Pass in the schema types (stringtype, integertype, ... in order of how they should correspond to column
	// names, then pair those up with teh column names to make the structtype manually here (using fold)

	def usingReadFileByCSVWithSchema(spark: SparkSession,
							   filepath: String,
							   colnames: Seq[String], coltypes: Seq[DataType]): DataFrame = {

		val emptyStruct: StructType = new StructType()

		val schema: StructType = colnames.zip(coltypes).foldLeft(emptyStruct){
			case (accStruct, (name, tpe)) => accStruct.add(name = name, dataType = tpe, nullable = true)
		}

		val df_schema: DataFrame = spark.read.format("csv")
			.option("header", "true")
			.schema(schema)
			.load(filepath)

		df_schema.printSchema()
		df_schema.show()

		df_schema
	}


	def usingReadTXTFile(spark: SparkSession, filepath: String): DataFrame = {

		val df = spark.read.text(filepath)
		df.printSchema()
		df.show()
		df
	}

	def usingReadJSONFile(spark: SparkSession, filepath: String): DataFrame = {

		val df = spark.read.json(filepath)
		df.printSchema()
		df.show()
		df
	}

	// TODO more xml detail here = https://sparkbyexamples.com/spark/spark-read-write-xml/
	/*def usingReadXMLFile(spark: SparkSession, filepath: String): DataFrame = {

		import spark.implicits._

		val df = spark.read
			.format("com.databricks.spark.xml")
			.option(key = "rowTag", value = "person")
			.xml(filepath)

		df.printSchema()
		df.show()
		df
	}*/
}

