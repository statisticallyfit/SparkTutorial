package com.SparkDocumentationByTesting.specs.AboutDataFrames

import com.SparkDocumentationByTesting.CustomMatchers
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.runtime.universe._

//import com.SparkSessionForTests
import org.scalatest.Assertions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper


// TODO add - AboutDataset (compare/contrast typed with Dataframe)
// TODO add - sparkdaria create dataframe method - explore features - use the Dataframe tabs tutorials (bookmarks)


/**
 *
 */
/*
object StateHere extends SparkSessionWrapper {

	import sparkSessionWrapper.implicits._

	import com.data.util.EnumHub.{Company, Country, Instrument, Transaction}
	import com.data.util.DataHub.ManualDataFrames.fromEnums.{Amount, EnumString, tradeSeq}

	lazy val colnamesTrade: List[String] = List("Company", "FinancialInstrument", "Amount", "BuyOrSell", "Country")
	lazy val coltypesTrade: List[DataType] = List(StringType, StringType, IntegerType, StringType, StringType)
	lazy val tradeSeq: Seq[(Company, Instrument, Amount, Transaction, Country)] = Seq(
		(Company.JPMorgan, Instrument.Financial.Stock, 2, Transaction.Buy, Country.China),
		(Company.Google, Instrument.Financial.Swap, 4, Transaction.Sell, Country.America),
		(Company.GoldmanSachs, Instrument.Financial.Equity, 3, Transaction.Sell, Country.America),
		(Company.Disney, Instrument.Financial.Bond, 10, Transaction.Buy, Country.Spain),
		(Company.Amazon, Instrument.Financial.Commodity.PreciousMetal.Gold, 12, Transaction.Buy, Country.CostaRica),
		(Company.Amazon, Instrument.Financial.Commodity.PreciousMetal.Silver, 12, Transaction.Buy, Country.CostaRica),
		(Company.Amazon, Instrument.Financial.Commodity.Gemstone.Ruby, 12, Transaction.Buy, Country.CostaRica),
		(Company.Amazon, Instrument.Financial.Commodity, 5, Transaction.Buy, Country.CostaRica),
		(Company.Google, Instrument.Financial.Derivative, 10, Transaction.Sell, Country.Arabia),
		(Company.Ford, Instrument.Financial.Derivative, 2, Transaction.Sell, Country.Argentina),
		(Company.Apple, Instrument.Financial.Stock, 1, Transaction.Buy, Country.Canada),
		(Company.IBM, Instrument.Financial.Share, 110, Transaction.Buy, Country.Brazil),
		(Company.Samsung, Instrument.Financial.Share, 2, Transaction.Sell, Country.China),
		(Company.Tesla, Instrument.Financial.Commodity.CrudeOil, 5, Transaction.Sell, Country.Estonia),
		(Company.Deloitte, Instrument.Financial.Cash, 9, Transaction.Sell, Country.Ireland)
	)
	// TODO if exception again but tradeSeq here too and test if working
	lazy val tradeStrSeq: Seq[(EnumString, EnumString, Amount, EnumString, EnumString)] = tradeSeq.map(_.tupleToHList.enumsToString.hlistToTuple /*.tupleToSparkRow*/)
	lazy val tradeDf: DataFrame = tradeStrSeq.toDF(colnamesTrade: _*)
}*/

class AboutCreatingDataFrames extends AnyFunSpec with Matchers //with TestSuite
	with CustomMatchers // use object import custom matchers
	with SparkSessionWrapper with BeforeAndAfterAll
	//with DataFrameComparer
	{

	import WaysToCreateDFs._
	//import CustomMatchers._ // TODO why doesn't this allow seeing it?


	import com.data.util.DataHub.ManualDataFrames.fromEnums._


	/*override def beforeAll = {
		println(s"trade seq  = ${tradeSeq}")
		println(s"trade str seq = ${tradeStrSeq}")
		println(s"trade str rdd = ${tradeStrRDD}")
		println(s"trade row rdd = ${tradeRowRDD}")
		println(s"trade df = \n")
		tradeDf.show
	}*/


	describe("Creating data frames") {

		describe("using sparkSession's `createDataFrame()`") {

			it("on a scala Seq") {
				//val resultDf: DataFrame = usingSessionCreateDataFrameOnSequence(sparkSessionWrapper, tradeStrSeq, colnamesTrade)

				val resultDf: DataFrame = usingSessionCreateDataFrameOnSequence(sparkSessionWrapper, tradeStrSeq, colnamesTrade)

				tradeSchema shouldBe a[StructType]
				tradeSchema.fieldNames should contain allElementsOf (colnamesTrade)
				tradeSchema.fields.map(_.dataType) should contain allElementsOf (coltypesTrade)

				resultDf shouldBe a[DataFrame]
				resultDf should equalDataFrame(tradeDf)


			}
			it("on a scala Seq of Rows, with Schema") {


				val resultDf: DataFrame = usingSessionCreateDataFrameOnSequenceOfRowsWithSchema(sparkSessionWrapper, tradeRowSeq, colnamesTrade, coltypesTrade)

				tradeSchema shouldBe a[StructType]
				tradeSchema.fieldNames should contain allElementsOf (colnamesTrade)
				tradeSchema.fields.map(_.dataType) should contain allElementsOf (coltypesTrade)

				resultDf shouldBe a[DataFrame]
				resultDf should equalDataFrame(tradeDf)

			}

			it("on RDD") {

				val resultDf: DataFrame = usingSessionCreateDataFrameOnRDD(sparkSessionWrapper, tradeStrRDD, colnamesTrade)

				tradeSchema shouldBe a[StructType]
				tradeSchema.fieldNames should contain allElementsOf (colnamesTrade)
				tradeSchema.fields.map(_.dataType) should contain allElementsOf (coltypesTrade)

				resultDf shouldBe a[DataFrame]
				resultDf should equalDataFrame(tradeDf)

			}

			it("on RDD of Rows, with schema") {


				val resultDf: DataFrame = usingSessionCreateDataFrameOnRowRDDAndSchema(sparkSessionWrapper, tradeRowRDD, colnamesTrade, coltypesTrade)

				tradeSchema shouldBe a[StructType]
				tradeSchema.fieldNames should contain allElementsOf (colnamesTrade)
				tradeSchema.fields.map(_.dataType) should contain allElementsOf (coltypesTrade)

				resultDf shouldBe a[DataFrame]
				resultDf should equalDataFrame(tradeDf)

			}
		}

		describe("using `toDF()`"){

			it("should use `toDF()` on RDD") {
				val resultDf: DataFrame = usingToDFOnRDD(sparkSessionWrapper, tradeStrRDD, colnamesTrade)

				resultDf should equalDataFrame(tradeDf)
			}
			it("should use `toDF()` on Seq") {
				val resultDf: DataFrame = usingToDFOnSeq(sparkSessionWrapper, tradeStrSeq, colnamesTrade)._2

				resultDf should equalDataFrame(tradeDf)
			}
		}
	}

	describe("Creating data frames (using input sources)") {

		import com.data.util.DataHub.ImportedDataFrames._

		// sparkMainSession.read.format(FORMAT_JSON).load(s"$PATH/$folderBillChambers/flight-data/json/2015-summary.json")
		val filepathJsonFlightData: String = s"$PATH/$folderBillChambers/flight-data/json/2015-summary.json"
		val filepathCSVFlightData: String = s"$PATH/$folderBillChambers/flight-data/csv/2015-summary.csv"


		// TODO why are the csv ones not succeeding?
		/*it("by reading CSV file") {
			val resultDf: DataFrame = usingReadFileByCSV(sparkSessionWrapper, filepathCSVFlightData)._2

			resultDf should equalDataFrame(fromBillChambersBook.flightDf)
		}*/

		/*it("by reading CSV file with schema") {
			val ns: List[String] = List("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME", "count")
			val ts: List[DataType] = List(StringType, StringType, LongType)

			val resultDf: DataFrame = usingReadFileByCSVWithSchema(sparkSessionWrapper, filepathCSVFlightData, ns, ts)

			resultDf should equalDataFrame(fromBillChambersBook.flightDf)
		}*/
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
			colnames.zip(coltypes).map { case (n, t) => StructField(n, t) }
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
	 *        2. define a schema using `StructType` and `StructField`
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
		//assert(df_noname.columns.toList == List("_1", "_2")) // TODO false if comparing arrays??

		val df: DataFrame = rdd.toDF(colnames: _*) // assigning colnames
		df.printSchema()
		df.show()
		assert(df.columns.toList == colnames)

		df
	}


	def usingToDFOnSeq[T <: Product : TypeTag](spark: SparkSession, seq: Seq[T] /*[(String, String)]*/ , colnames: Seq[String]): (DataFrame, DataFrame) = {
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

		val schema: StructType = colnames.zip(coltypes).foldLeft(emptyStruct) {
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

