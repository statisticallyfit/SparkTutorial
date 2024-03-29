package com.SparkDocumentationByTesting.specs.AboutDataFrames


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, Dataset, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize }
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

import utilities.DFUtils; import DFUtils._ ; import DFUtils.TypeAbstractions._; import DFUtils.implicits._
import utilities.GeneralMainUtils._
import utilities.GeneralMainUtils.implicits._
import utilities.DataHub.ManualDataFrames.fromEnums._
import ArtistDf._
import TradeDf._
import AnimalDf._

import utilities.EnumUtils.implicits._
import utilities.EnumHub._
import Human._
import ArtPeriod._
import Artist._
import Scientist._ ; import NaturalScientist._ ; import Mathematician._;  import Engineer._
import Craft._;
import Art._; import Literature._; import PublicationMedium._;  import Genre._
import Science._; import NaturalScience._ ; import Mathematics._ ; import Engineering._ ;


//import com.SparkSessionForTests
import com.SparkDocumentationByTesting.CustomMatchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import org.scalatest.Assertions._
import utilities.SparkSessionWrapper

import scala.reflect.runtime.universe._

// TODO add - AboutDataset (compare/contrast typed with Dataframe)
// TODO add - sparkdaria create dataframe method - explore features - use the Dataframe tabs tutorials (bookmarks)


/**
 *
 */

class CreateDataFrameSpecs extends AnyFunSpec with Matchers //with TestSuite
	with CustomMatchers // use object import custom matchers
	with SparkSessionWrapper //with BeforeAndAfterAll
	//with DataFrameComparer
	{

	import WaysToCreateDFs._
	//import CustomMatchers._ // TODO why doesn't this allow seeing it?

	// TODO can create a df from a schema, use this spark-test-repo source code = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/DataFrameToSchemaSuite.scala

	describe("Creating data frames") {

		describe("using sparkSession's `createDataFrame()`") {
			/**
			 * SOURCE:
			 * 	- sparkbyexamples
			 */
			it("on a scala Seq") {
				//val resultDf: DataFrame = usingSessionCreateDataFrameOnSequence(sparkSessionWrapper, tradeStrSeq, colnamesTrade)

				val resultDf: DataFrame = usingSessionCreateDataFrameOnSequence(sparkSessionWrapper, tradeStrSeq, colnamesTrade)

				tradeSchema shouldBe a[StructType]
				tradeSchema.fieldNames should contain allElementsOf (colnamesTrade)
				tradeSchema.fields.map(_.dataType) should contain allElementsOf (coltypesTrade)

				resultDf shouldBe a[DataFrame]

				// NOTE: not equal dfs because the schema is different:
				resultDf shouldNot equalDataFrame(tradeDf)
				resultDf.schema("DateOfTransaction").dataType shouldEqual StringType
				tradeDf.schema("DateOfTransaction").dataType shouldEqual DateType

			}

			/**
			 * SOURCE:
			 * 	- sparkbyexamples
			 */
			it("on a scala Seq of Rows, with Schema") {

				// TODO throws error "IllegalArgumentException: The value (1921-12-21) of the type (java.lang.String) cannot be converted to the DATE type" when using tradeDf (because of Date) so switching the df now:

				val resultDf: DataFrame = usingSessionCreateDataFrameOnSequenceOfRowsWithSchema(sparkSessionWrapper, animalRowSeq, colnamesAnimal, coltypesAnimal)

				animalSchema shouldBe a[StructType]
				animalSchema.fieldNames should contain allElementsOf (colnamesAnimal)
				animalSchema.fields.map(_.dataType) should contain allElementsOf (coltypesAnimal)

				resultDf shouldBe a[DataFrame]
				resultDf should equalDataFrame(animalDf)

			}

			/**
			 * SOURCE:
			 * 	- sparkbyexamples
			 */
			it("on RDD") {

				// WARNING date col issue
				val resultDf: DataFrame = usingSessionCreateDataFrameOnRDD(sparkSessionWrapper, animalStrRDD, colnamesAnimal)

				animalSchema shouldBe a[StructType]
				animalSchema.fieldNames should contain allElementsOf (colnamesAnimal)
				animalSchema.fields.map(_.dataType) should contain allElementsOf (coltypesAnimal)

				resultDf shouldBe a[DataFrame]
				resultDf should equalDataFrame(animalDf)

			}

			/**
			 * SOURCE:
			 * 	- sparkbyexamples
			 */
			it("on RDD of Rows, with schema") {


				val resultDf: DataFrame = usingSessionCreateDataFrameOnRowRDDAndSchema(sparkSessionWrapper, animalRowRDD, colnamesAnimal, coltypesAnimal)

				animalSchema shouldBe a[StructType]
				animalSchema.fieldNames should contain allElementsOf (colnamesAnimal)
				animalSchema.fields.map(_.dataType) should contain allElementsOf (coltypesAnimal)

				resultDf shouldBe a[DataFrame]
				resultDf should equalDataFrame(animalDf)

			}
		}

		describe("using `toDF()`"){

			/**
			 * SOURCE:
			 * 	- sparkbyexamples
			 */
			// NOTE problem with date column (schemas don't end up the same date col still is string) so using a non-date df:)
			it("should use `toDF()` on RDD") {
				val resultDf: DataFrame = usingToDFOnRDD(sparkSessionWrapper, animalStrRDD, colnamesAnimal)

				resultDf should equalDataFrame(animalDf)
			}

			/**
			 * SOURCE:
			 * 	- sparkbyexamples
			 */
			it("should use `toDF()` on Seq") {
				// WARNING date col schema issue so using non-date df

				val resultDf: DataFrame = usingToDFOnSeq(sparkSessionWrapper, animalStrSeq, colnamesAnimal)._2

				resultDf should equalDataFrame(animalDf)
			}
		}
	}


		// TODO pg 92 Bill Chambers - make example such that types in dataframe do not match the schema, therefore intecept an exception

	describe("Creating data frames (using input sources)") {

		import utilities.DataHub.ImportedDataFrames._

		// sparkMainSession.read.format(FORMAT_JSON).load(s"$PATH/$folderBillChambers/flight-data/json/2015-summary.json")
		val filepathJsonFlightData: String = s"$DATA_PATH/$folderBillChambers/$folderInputData/flight-data/json/2015-summary.json"
		val filepathCSVFlightData: String = s"$DATA_PATH/$folderBillChambers/$folderInputData/flight-data/csv/2015-summary.csv"


		/**
		 * SOURCE:
		 * 	- sparkbyexamples
		 */
		// TODO why are the csv ones not succeeding?
		/*it("by reading CSV file") {
			val resultDf: DataFrame = usingReadFileByCSV(sparkSessionWrapper, filepathCSVFlightData)._2

			resultDf should equalDataFrame(fromBillChambersBook.flightDf)
		}*/

		/**
		 * SOURCE:
		 * 	- sparkbyexamples
		 */
		/*it("by reading CSV file with schema") {
			val ns: List[String] = List("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME", "count")
			val ts: List[DataType] = List(StringType, StringType, LongType)

			val resultDf: DataFrame = usingReadFileByCSVWithSchema(sparkSessionWrapper, filepathCSVFlightData, ns, ts)

			resultDf should equalDataFrame(fromBillChambersBook.flightDf)
		}*/

		/**
		 * SOURCE:
		 * 	- sparkbyexamples
		 */
		it("by reading JSON file") {
			val resultDf: DataFrame = usingReadJSONFile(sparkSessionWrapper, filepathJsonFlightData)

			resultDf should equalDataFrame(fromBillChambersBook.flightDf)
		}
	}

}


object WaysToCreateDFs {

	def usingSessionCreateDataFrameOnSequence[T <: Product : TypeTag](spark: SparkSession, seq: Seq[T], colnames: Seq[String]): DataFrame = {
		//import spark.implicits._
		val df: DataFrame = spark.createDataFrame(seq).toDF(colnames: _*)

		//df.printSchema()
		//df.show()

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

		val schema: StructType = DFUtils.createSchema(colnames, coltypes)
		val df: DataFrame = spark.createDataFrame(rows = seqOfRows.asJava, schema = schema)

		/*df.printSchema()
		df.show()*/
		df
	}


	def usingSessionCreateDataFrameOnRDD[T <: Product : TypeTag](spark: SparkSession, rdd: RDD[T], colnames: Seq[String]): DataFrame = {
		//import spark.implicits._

		val df: DataFrame = spark.createDataFrame(rdd).toDF(colnames: _*)

		/*df.printSchema()
				df.show()*/

		df
	}

	/**
	 * `createDataFrame()` has another signature which takes RDD[Row] and a schema for colnames as arguments.
	 * To use, must first
	 * 	1. convert rdd object from RDD[T] to RDD[Row], and
	 *        2. define a schema using `StructType` and `StructField`
	 */
	def usingSessionCreateDataFrameOnRowRDDAndSchema(spark: SparkSession, rowRDD: RDD[Row], colnames: Seq[String], coltypes: Seq[DataType]): DataFrame = {

		val schema: StructType = StructType(
			colnames.zip(coltypes).map { case (n, t) => StructField(n, t, nullable = true) }
		)
		val df: DataFrame = spark.createDataFrame(rowRDD = rowRDD, schema = schema)

		/*df.printSchema()
		df.show()*/

		df
	}

	// TODO - why cannot make RDD[(A, B)] ? instead of string, string?
	def usingToDFOnRDD[T <: Product : TypeTag](spark: SparkSession, rdd: RDD[T], colnames: Seq[String]): DataFrame = {

		import spark.implicits._

		val df_noname: DataFrame = rdd.toDF() // default colnames are _1, _2

		/*df.printSchema()
		df.show()*/ // show all the rows box format
		//assert(df_noname.columns.toList == List("_1", "_2")) // TODO false if comparing arrays??

		val df: DataFrame = rdd.toDF(colnames: _*) // assigning colnames

		/*df.printSchema()
		df.show()*/
		assert(df.columns.toList == colnames)

		df
	}


	def usingToDFOnSeq[T <: Product : TypeTag](spark: SparkSession, seq: Seq[T] /*[(String, String)]*/ , colnames: Seq[String]): (DataFrame, DataFrame) = {
		import spark.implicits._

		val df_noname: DataFrame = seq.toDF()
		val df: DataFrame = seq.toDF(colnames: _*)


		/*df.printSchema()
		df.show()*/

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


		/*df.printSchema()
		df.show()*/

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

		val schema: StructType = colnames.zip(coltypes).foldLeft(emptyStruct) {
			case (accStruct, (name, tpe)) => accStruct.add(name = name, dataType = tpe, nullable = true)
		}

		val df_schema: DataFrame = spark.read.format("csv")
			.option("header", "true")
			.schema(schema)
			.load(filepath)

		/*df.printSchema()
		df.show()*/

		df_schema
	}


	def usingReadTXTFile(spark: SparkSession, filepath: String): DataFrame = {

		val df = spark.read.text(filepath)
		/*df.printSchema()
		df.show()*/
		df
	}

	def usingReadJSONFile(spark: SparkSession, filepath: String): DataFrame = {

		val df = spark.read.json(filepath)
		/*df.printSchema()
		df.show()*/
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

