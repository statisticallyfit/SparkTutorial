package com.DocumentingSparkByTestScenarios


import com.DocumentingSparkByTestScenarios.CreatingDataFrames.{Art, Literature, Musician, Painter}
import org.apache.spark.sql.{Column, ColumnName, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.{DocumentingSparkByTestScenarios, SparkSessionForTests}
import org.scalatest.TestSuite

import scala.reflect.runtime.universe._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.rdd.RDD
import org.scalatest.Assertions._ // intercept





object StateForDataFrameCreation extends SparkSessionForTests {

	val columnNames: Seq[String] = Seq("language", "users_count")

	val seq: Seq[(String, String)] = Seq(("Java", "20000"),
		("Python", "100000"),
		("Scala", "3000")
	)
}
import StateForDataFrameCreation._

/**
 *
 */
class AboutDataFrames extends AnyFunSpec with Matchers //with TestSuite
	with CustomMatchers
	with SparkSessionForTests
	with DataFrameComparer {

	import sparkTestsSession.implicits._


	import CreatingDataFrames._


	//import com.data.util.DataHub.ManualDataFrames.fromAlvinHenrickBlog._


	describe("Creating data frames") {

		describe("using sparkSession's `createDataFrame()`"){

			it("on a scala Seq") {
				usingSessionCreateDataFrameOnSequence(spark: SparkSession,
					seq: Seq[(String, String)],
					colnames: Seq[String])
			}
			it("on a scala Seq of Rows, with Schema") {
				usingSessionCreateFataFrameOnSequenceOfRowsWithSchema(spark: SparkSession,
					seq: Seq[(String, String)],
					colnames: Seq[String])
				usingSessionCreateFataFrameOnSequenceOfRowsWithSchema(spark: SparkSession,
					seqOfRows: Seq[Row],
					colnames: Seq[String])
			}

			it("on RDD") {
				usingSessionCreateDataFrameOnRDD(spark: SparkSession,
					rdd: RDD[(String, String)],
					colnames: Seq[String])
			}

			it("on RDD of Rows, with schema") {
				usingSessionCreateDataFrameOnRowRDDAndSchema(spark: SparkSession,
					rdd: RDD[(String, String)],
					colnames: Seq[String])

				usingSessionCreateDataFrameOnRowRDDAndSchema(spark: SparkSession,
					rowRDD: RDD[Row],
					colnames: Seq[String])
			}
		}

		describe("using `toDF()`"){

			it("should use `toDF()` on RDD") {
				usingToDFOnRDD(spark: SparkSession,
					rdd: RDD[(String, String)],
					colnames: Seq[String])
			}
			it("should use `toDF()` on Seq") {
				usingToDFOnSeq(spark: SparkSession, seq: Seq[(String, String)], colnames: Seq[String])
			}
		}


	}





	describe("Creating data frames (using input sources)"){

		describe("by reading CSV file") {
			usingReadFileByCSV(spark: SparkSession, filepath: String)
		}
		describe("by reading TXT file") {
			usingReadTXTFile(spark: SparkSession, filepath: String)
		}
		describe("by reading JSON file") {
			usingReadJSONFile(spark: SparkSession, filepath: String)
		}
	}

}




object CreatingDataFrames {

	import enumeratum._
	import enumeratum.values._

	def usingSessionCreateDataFrameOnSequence(spark: SparkSession,
									  seq: Seq[(String, String)],
									  colnames: Seq[String]): DataFrame	= {
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
	def usingSessionCreateFataFrameOnSequenceOfRowsWithSchema(spark: SparkSession,
												   seq: Seq[(String, String)],
												   colnames: Seq[String]): DataFrame = {

		val seqOfRows: Seq[Row] = seq.map { case (name1, name2) => Row(name1, name2) }
		usingSessionCreateFataFrameOnSequenceOfRowsWithSchema(spark, seqOfRows, colnames)

	}
	def usingSessionCreateFataFrameOnSequenceOfRowsWithSchema(spark: SparkSession,
												   seqOfRows: Seq[Row],
												   colnames: Seq[String]): DataFrame = {
		import org.apache.spark.sql.Row

		// NOTE: need to use "JavaConversions" not "JavaConverters" so that the createDataFrame from sequence of rows will work.
		//import scala.collection.JavaConversions._
		//import scala.collection.JavaConverters._

		// Sinec scala 2.13 need to use this other import instead: https://stackoverflow.com/a/6357299
		import scala.collection.JavaConverters._
		//import scala.jdk.CollectionConverters._
		//import scala.collection.JavaConversions._


		import org.apache.spark.sql.types.{StringType, StructField, StructType}
		val schema: StructType = StructType(
			colnames.map(n => StructField(name = n, dataType = StringType, nullable = true))
		)


		val df: DataFrame = spark.createDataFrame(rows = seqOfRows.asJava, schema = schema)

		df.printSchema()
		df.show()
		df
	}


	def usingSessionCreateDataFrameOnRDD(spark: SparkSession,
								  rdd: RDD[(String, String)],
								  colnames: Seq[String]): DataFrame = {
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
	def usingSessionCreateDataFrameOnRowRDDAndSchema(spark: SparkSession,
										    rdd: RDD[RowType],
										    colnames: Seq[String]): DataFrame = {
		val rowRDD: RDD[Row] = rdd.map{ case (a1, a2) => Row(a1, a2)}
		usingSessionCreateDataFrameOnRowRDDAndSchema(spark, rowRDD, colnames)
	}
	def usingSessionCreateDataFrameOnRowRDDAndSchema(spark: SparkSession,
										    rowRDD: RDD[Row],
										    colnames: Seq[String]): DataFrame = {

		import org.apache.spark.sql.Row
		import org.apache.spark.sql.types.{StringType, StructField, StructType}

		/*val schema = StructType(Array(
			StructField(name = "language", dataType = StringType, nullable = true),
			StructField(name = "users_count", dataType = StringType, nullable = true)
		))*/
		val schema: StructType = StructType(
			colnames.map(n => StructField(name = n, dataType = StringType, nullable = true))
		)
		val df = spark.createDataFrame(rowRDD = rowRDD, schema = schema)

		df.printSchema()
		df.show()

		df
	}

	// TODO - why cannot make RDD[(A, B)] ? instead of string, string?
	def usingToDFOnRDD(spark: SparkSession,
				    rdd: RDD[(String, String)],
				    colnames: Seq[String]): DataFrame = {

		// NOTE: need implicits to call rdd.toDF()
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


	def usingToDFOnSeq(spark: SparkSession, seq: Seq[(String, String)], colnames: Seq[String]): (DataFrame, DataFrame) = {
		import spark.implicits._

		val df_noname: DataFrame = seq.toDF()
		val df: DataFrame = seq.toDF(colnames: _*)

		df.printSchema()
		df.show()

		(df_noname, df)
	}






	// NOTE: to read in multiple csv files, separate their file names with comma = https://hyp.is/ceetdpWBEey3Rnd9naElZQ/sparkbyexamples.com/spark/spark-read-csv-file-into-dataframe/
	// NOTE to read in all csv files from a folder, must pass in the entire directory name = https://hyp.is/kh1dZpWBEeyggz93IvgE_w/sparkbyexamples.com/spark/spark-read-csv-file-into-dataframe/
	def usingReadFileByCSV(spark: SparkSession, filepath: String): List[DataFrame] = {
		val df_noheader = spark.read.csv(filepath)

		val df: DataFrame = spark
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

		df.printSchema()
		df.show()

		List(df_noheader, df, df_delim, df_inferSchema)
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

	def usingReadFileByCSVWithCustomSchema(spark: SparkSession,
								    schemaNameTypePairs: Seq[(String, DataType)],
								    /*schema: StructType,*/
								    filepath: String): DataFrame = {

		val emptyStruct: StructType = new StructType()

		val userSchema: StructType = schemaNameTypePairs.foldLeft(emptyStruct){
			case (accStruct, (name, tpe)) => accStruct.add(name = name, dataType = tpe, nullable = true)
		}

		val dfWithSchema: DataFrame = spark.read.format("csv")
			.option("header", "true")
			.schema(userSchema)
			.load(filepath)

		dfWithSchema.printSchema()
		dfWithSchema.show()

		dfWithSchema
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

