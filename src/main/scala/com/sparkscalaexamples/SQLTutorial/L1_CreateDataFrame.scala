package com.sparkscalaexamples.SQLTutorial


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.{DataType, StringType, IntegerType, BooleanType, DoubleType, StructField, StructType}

/**
 * WEB PAGE SOURCE = https://sparkbyexamples.com/spark/different-ways-to-create-a-spark-dataframe/
 * CODE SOURCE = https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/CreateDataFrame.scala
 */



object ConvertRDDToDataFrame {

	// TODO - why cannot make RDD[(A, B)] ? instead of string, string?
	def usingToDF(spark: SparkSession,
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

	def usingCreateDataFrameFromSparkSessionViaRDD(spark: SparkSession,
										  rdd: RDD[(String, String)],
										  colnames: Seq[String]): DataFrame	= {
		//import spark.implicits._

		val df: DataFrame = spark.createDataFrame(rdd).toDF(colnames:_*)

		df.printSchema()
		df.show()

		df
	}

	def usingCreateDataFrameFromSparkSessionViaSequence(spark: SparkSession,
											  data: Seq[(String, String)],
										  colnames: Seq[String]): DataFrame	= {
		//import spark.implicits._
		val df: DataFrame = spark.createDataFrame(data).toDF(colnames:_*)

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
	def usingCreateDataFrameWithRowAndSchema(spark: SparkSession,
									 rdd: RDD[(String, String)],
									 colnames: Seq[String]): DataFrame = {

		import org.apache.spark.sql.types.{StringType, StructField, StructType}
		import org.apache.spark.sql.Row

		/*val schema = StructType(Array(
			StructField(name = "language", dataType = StringType, nullable = true),
			StructField(name = "users_count", dataType = StringType, nullable = true)
		))*/
		val schema: StructType = StructType(
			colnames.map(n => StructField(name = n, dataType = StringType, nullable = true))
		)

		val rowRDD: RDD[Row] = rdd.map{ case (a1, a2) => Row(a1, a2)}
		val df = spark.createDataFrame(rowRDD = rowRDD, schema = schema)

		df.printSchema()
		df.show()

		df
	}

	def usingSeqToDF(spark: SparkSession, data: Seq[(String, String)], colnames: Seq[String]): (DataFrame, DataFrame) = {
		import spark.implicits._

		val df_noname: DataFrame = data.toDF()
		val df: DataFrame = data.toDF(colnames:_*)

		df.printSchema()
		df.show()

		(df_noname, df)
	}

	/**
	 * `createDataFrame()` has another signature in spark which takes the util.List of Row type and schema for
	 * ccolumn names as arguments.
	 */
	def usingCreateFataFrameFromListOfRowsAndSchema(spark: SparkSession,
										   data: Seq[(String, String)],
										   colnames: Seq[String]): DataFrame = {
		import org.apache.spark.sql.Row
		import scala.collection.JavaConversions._
		//import scala.collection.JavaConverters._
		import org.apache.spark.sql.types.{StringType, StructField, StructType}

		/*val seqOfRows = Seq(Row("Java", "20000"),
			Row("Python", "100000"),
			Row("Scala", "3000")
		)*/
		val seqOfRows: Seq[Row] = data.map { case (name1, name2) => Row(name1, name2)}

		/*val schema = StructType(Array(
			StructField(name = "language", dataType = StringType, nullable = true),
			StructField(name = "users_count", dataType = StringType, nullable = true)
		))*/
		val schema: StructType = StructType(
			colnames.map(n => StructField(name = n, dataType = StringType, nullable = true))
		)

		val df: DataFrame = spark.createDataFrame(rows = seqOfRows, schema = schema)

		df.printSchema()
		df.show()
		df
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





object L1_CreateDataFrame extends App {


	val spark: SparkSession = SparkSession.builder()
		.master("local[1]")
		.appName("SparkByExamples.com")
		.getOrCreate()


	val columnNames: Seq[String] = Seq("language", "users_count")
	val data: Seq[(String, String)] = Seq(("Java", "20000"),
		("Python", "100000"),
		("Scala", "3000")
	)
	// Create an RDD from a Seq collection by calling parallelize
	val rdd: RDD[(String, String)] = spark.sparkContext.parallelize(data)

	// Convert RDD to dataframe (using toDF())
	val df1 = ConvertRDDToDataFrame.usingToDF(spark, rdd, columnNames)
	val df2 = ConvertRDDToDataFrame.usingCreateDataFrameFromSparkSessionViaRDD(spark, rdd, columnNames)
	val df3 = ConvertRDDToDataFrame.usingCreateDataFrameWithRowAndSchema(spark, rdd, columnNames)
	val (df4_, df4) = ConvertRDDToDataFrame.usingSeqToDF(spark, data, columnNames)
	val df5 = ConvertRDDToDataFrame.usingCreateDataFrameFromSparkSessionViaSequence(spark, data, columnNames)
	val df6 = ConvertRDDToDataFrame.usingCreateFataFrameFromListOfRowsAndSchema(spark, data, columnNames)


	val List(df7_, df7, df7_delim, df7_inferSchema) = ConvertRDDToDataFrame.usingReadFileByCSV(spark, filepath =
		ZIPCODES_FILE_CSV)
	//TODO left off here to check what inferschema does

	assert(List(df1, df2, df3, df4, df5, df6).combinations(2)
		.forall{ case List(dfA, dfB) => dfA.collectAsList() == dfB.collectAsList()},
		"Test: all dfs must have same row contents"
	)
	assert(List(df1, df2, df3, df4, df5, df6).combinations(2).forall{ case List(dfA, dfB) => dfA.schema == dfB.schema},
		"Test: all dfs must have same schemas"
	)
	assert(df4_.columns.toList == List("_1", "_2")) // sequence itself doesn't have the colnames
	assert(df4_.schema != df1.schema)


	// CSV part -----------------------------------------------------------------------------------------------------------
	//import org.apache.spark.sql.types.{StringType, IntegerType, BooleanType, DoubleType, StructField, StructType}

	val ZIPCODES_FILE_CSV = "src/main/resources/zipcodes.csv"
	val ZIPCODES_FILE_TXT = "src/main/resources/simple_zipcodes.txt"
	val ZIPCODES_FILE_JSON = "src/main/resources/simple_zipcodes.json"

	val ZIPCODES_COLNAMES = List("RecordNumber", "Zipcode", "ZipCodeType", "City", "State", "LocationType", "Lat", "Long",
		"Xaxis", "Yaxis", "Zaxis", "WorldRegion", "Country","LocationText", "Location", "Decommisioned", "TaxReturnsFiled",
		"EstimatedPopulation", "TotalWages", "Notes")


	assert(df7.columns.toList == ZIPCODES_COLNAMES &&
		df7.columns.toList == df7_delim.columns.toList &&
		df7.columns.toList == df7.schema.fields.map(structField => structField.name).toList,

		"Test: df7 csv column names"
	)

	assert(df7.schema.fields.map(structfield => structfield.dataType).forall(_ == StringType))
	assert(df7.schema == df7_delim.schema)



	// Reading CSV Files with user-specified custom schema
	val schemaTypes: Seq[DataType] = List(IntegerType, IntegerType, StringType, StringType, StringType, StringType,
		DoubleType, DoubleType, IntegerType, DoubleType, DoubleType, StringType, StringType, StringType, StringType,
		BooleanType, StringType, IntegerType, IntegerType, StringType)
	val schemaNameTypePairs: Seq[(String, DataType)] = ZIPCODES_COLNAMES.zip(schemaTypes)

	val df8: DataFrame = ConvertRDDToDataFrame.usingReadFileByCSVWithCustomSchema(spark, schemaNameTypePairs, ZIPCODES_FILE_CSV)
	val df9: DataFrame = ConvertRDDToDataFrame.usingReadTXTFile(spark, ZIPCODES_FILE_TXT)
	val df10: DataFrame = ConvertRDDToDataFrame.usingReadJSONFile(spark, ZIPCODES_FILE_JSON)
	//TODO xml thing error


}
