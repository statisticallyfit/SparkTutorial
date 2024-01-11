//package com.DocumentingSparkByTestScenarios
//
//
//import com.DocumentingSparkByTestScenarios.CreatingDataFrames.{Art, Literature, Musician, Painter}
//import org.apache.spark.sql.{Column, ColumnName, DataFrame, Dataset, Row, SparkSession}
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.functions._
//import com.{DocumentingSparkByTestScenarios, SparkSessionForTests}
//import org.scalatest.TestSuite
//
//import scala.reflect.runtime.universe._
//import org.scalatest.funspec.AnyFunSpec
//import org.scalatest.matchers.should._
//import com.github.mrpowers.spark.fast.tests.DataFrameComparer
//import org.apache.spark.rdd.RDD
//import org.scalatest.Assertions._ // intercept
//
//
//
//
//
//object StateForDataFrameCreation extends SparkSessionForTests {
//
//	val columnNames: Seq[String] = Seq("language", "users_count")
//
//	val seq: Seq[(String, String)] = Seq(("Java", "20000"),
//		("Python", "100000"),
//		("Scala", "3000")
//	)
//}
//import StateForDataFrameCreation._
//
///**
// *
// */
//class AboutDataFrames extends AnyFunSpec with Matchers //with TestSuite
//	with CustomMatchers
//	with SparkSessionForTests
//	with DataFrameComparer {
//
//	import sparkTestsSession.implicits._
//
//
//	//import com.data.util.DataHub.ManualDataFrames.fromAlvinHenrickBlog._
//
//
//	describe("Creating data frames") {
//
//		it("should use sparkSession's `createDataFrame()` on a scala Seq"){
//			usingSessionCreateDataFrameOnSequence(spark: SparkSession,
//									  seq: Seq[(String, String)],
//									  colnames: Seq[String])
//		}
//		it("should use sparkSession's `createDataFrame()` on a scala Seq of Rows, with Schema"){
//			usingSessionCreateFataFrameOnSequenceOfRowsWithSchema(spark: SparkSession,
//												   seq: Seq[(String, String)],
//												   colnames: Seq[String])
//												   usingSessionCreateFataFrameOnSequenceOfRowsWithSchema(spark: SparkSession,
//												   seqOfRows: Seq[Row],
//												   colnames: Seq[String])
//		}
//
//		it("should use sparkSession's `createDataFrame()` on RDD") {
//			usingSessionCreateDataFrameOnRDD(spark: SparkSession,
//								  rdd: RDD[(String, String)],
//								  colnames: Seq[String])
//		}
//
//		it("should use sparkSession's `createDataFrame()` on RDD of Rows, with schema"){
//			usingSessionCreateDataFrameOnRowRDDAndSchema(spark: SparkSession,
//										    rdd: RDD[(String, String)],
//										    colnames: Seq[String])
//
//			usingSessionCreateDataFrameOnRowRDDAndSchema(spark: SparkSession,
//										    rowRDD: RDD[Row],
//										    colnames: Seq[String])
//		}
//		it("should use `toDF()` on RDD"){
//			usingToDFOnRDD(spark: SparkSession,
//				    rdd: RDD[(String, String)],
//				    colnames: Seq[String])
//		}
//		it("should use `toDF()` on Seq"){
//			usingToDFOnSeq(spark: SparkSession, seq: Seq[(String, String)], colnames: Seq[String])
//		}
//	}
//
//
//
//
//
//	describe("Creating data frames (using input sources)"){
//
//		describe("by reading CSV file") {
//			usingReadFileByCSV(spark: SparkSession, filepath: String)
//		}
//		describe("by reading TXT file") {
//			usingReadTXTFile(spark: SparkSession, filepath: String)
//		}
//		describe("by reading JSON file") {
//			usingReadJSONFile(spark: SparkSession, filepath: String)
//		}
//	}
//
//}
//
//
//
//
//object CreatingDataFrames {
//
//	// FinancialInstrument=  stocks, bonds, options, derivatives
//	// anmials = gi, hippo, crocodile, zebra
//	// mineral = graphite, diamond, ruby, pearl
//	// preciousmetal = gold, copper, bronze, silver, obsidian
//	// weekday seen on = m, t, w ....
//	// Gender = m, f
//
//	object Vessel extends Enumeration {
//		type Vessel = Value
//		val Ship, Submarine, Airplane, Helicopter, Car, Truck, Train, Canoe, Kayak = Value
//	}
//
//	/*object Instrument extends Enumeration {
//		type Instrument = Value
//		val Musical, Financial, Navigational = Value
//	}*/
//	trait Instrument[T <: Enumeration]
//	// Source = https://www.martek-marine.com/ecdis/pre-ecdis-navigation/
//
//	object Navigational extends Enumeration with Instrument[Navigational.type] {
//		type Instrument = Value
//		val Compass, Sextant, LandLine, Astrolabe, Pelorus, SandGlass, Quadrant, Nocturnal, Backstaff = Value
//	}
//	object Musical extends Enumeration with Instrument[Musical.type] {
//		type Instrument = Value
//		val Flute, Oboe, Clarinet, Trombone, Tuba, FrenchHorn, Trumpet, Saxophone, Harmonica, Xylophone, Piano, Violin, Harp, Guitar, Cello, Voice = Value
//	}
//
//	object Financial extends Enumeration with Instrument[Financial.type] {
//		type Instrument = Value
//		val Stock, Bond, Option, Derivative, Future, Swap, Equity, Share, Commodity, Cash = Value
//	}
//
//
//	object Transaction extends Enumeration {
//		type Type = Value
//		val Buy, Sell = Value
//	}
//	object Company extends Enumeration {
//		type Firm = Value
//		val GoldmanSachs, JPMorgan, Samsung, Ford, Disney, Apple, Google, Microsoft, Tesla, Amazon, Walmart, Nike, Twitter, Facebook, Starbucks, IBM = Value
//	}
//
//	sealed abstract case class Instrument(kind: Enumeration)
//	case object Fin extends Instrument(Financial.Instrument)
//	case object Musical extends Instrument("Musical")
//
//
//	trait Art[A]
//	trait Artist[T]
//	trait Artst
//
//	object Art extends Enumeration with Art[Art.Kind] {
//		type Kind = Value
//		val Painting, Music, Literature, Sculpture, Architecture, Theatre, Cinema = Value
//	}/*
//	case class Painter() extends Artist[Art.Painting]
//	case class Musician() extends Artist[Musician]
//	case class Writer() extends Artist[Writer]
//	case class Sculptor() extends Artist[Sculptor]
//	case class Architect() extends Artist[Architect]
//	case class Actor() extends Artist[Actor]*/
//	object Artist extends Enumeration with Artist[Artist.ValueSet] {
//		type Kind = Value
//		val Painter, Musician, Writer, Sculptor, Architect, Actor = Value
//	}
//
//	object Painter extends Enumeration with Artist[Painter.Person/*Art.Painting.type*/] {
//		type Person = Value
//		val VanGogh, LeonardoDaVinci, ClaudeMonet, PabloPicasso, Rembrandt, Michelangelo, ElGreco = Value
//	}
//	Painter.toString()
//
//	object Musician extends Enumeration with Artst with Artist[Musician.ValueSet /*Art.Value*/ ] {
//		type Person = Value
//		val CelticWomen, Enya, HayleyWestenra, SarahBrightman, PhilCollins, RodStewart, Beyonce, AlanisMorrisette, Adele, // Voice
//		JohnColtrane, PaulDesmond, SonnyStitt, // Saxophone
//		JackTeagarden, FredWesley, // Trombone
//		MinLeibrook, WalterEnglish, SquireGersh, //Tuba
//		HerbieMann, TheobaldBoehm, YusefLateef, // Flute
//		LouisArmstrong, // Trumpet, Voice
//		JellyRollMorton, // Piano, Voice
//		NiccoloPaganini, ViktoriaMullova, GeorgeEnescu, // Violin
//		= Value
//	}
//	object Writer extends Enumeration with Artst with Artist[Writer.ValueSet /*Art.Value*/ ] {
//		type Person = Value
//		val AlfredLordTennyson, LordByron, EdgarAllanPoe, CharlesDickens, EmilyDickinson, JulesVerne, JaneAusten, TSEliot, HansChristianAnderson, MarkTwain, JamesJoyce, LeoTolstoy, FScottFitzgerald, GeorgeOrwell, HermanMelville, RoaldDahl, AgathaChristie, WilliamShakespeare, AlexandreDumas, ErnestHemingway, HermanHesse, AntonChekhov, AlexanderPushkin, GeoffreyChaucer = Value
//	}
//
//
//	//val domainToAuthor: Map[Art.Type, Artist[Art.type]] = Map(Art.Painting -> Painter.VanGogh)
//	val domainToAuthor = Map(
//		Art.Painting -> Musician.Enya,
//		Art.Literature -> Writer.LordByron
//	)
//	object Animal extends Enumeration {
//		type Animal = Value
//		val Giraffe, Crocodile, Hippo, Elephant, Pelican, Flamingo, Albatross, Zebra, Lion, Heyna, Panda, Koala, Cougar, Panther, Tiger, Leopard, Gorilla, Snake, Termite, Penguin, Bear, Reindeer, Squirrel, Lynx, Marmot, Weasel, Rabbit, Fox = Value
//	}
//	object Climate extends Enumeration {
//		type Climate = Value
//		val Temperate, Arid, Tundra, Polar, Tropical, Rainforest, Dry, Desert, Mediterranean, Continental = Value
//	}
//	object Country extends Enumeration {
//		type Country = Value
//		val Africa, China, Russia, Spain, France, Arabia, Scotland, Ireland, England, Romania, Estonia, America, Canada, Brazil, CostaRica, Argentina, Australia = Value
//	}
//	type AnimalDescr = (Animal.Animal, Climate.Climate, Country.Country, Amount)
//	object Astronomical extends Enumeration {
//		type Object = Value
//		val Nebula, Galaxy, Star, Sun, Moon, Asteroid, Meteor, BlackHole, Planet = Value
//	}
//	object Fruit extends Enumeration {
//		type Fruit = Value
//		val Banana, Strawberry, Blackberry, Blueberry, Raspberry, Kiwi, Pineapple, Mango, Pear, Apple, Persimmon, Starfruit, Melon, Watermelon, Tangerine, Mandarin, Orange, Lemon, Lime = Value
//	}
//	object Vegetable extends Enumeration {
//		type Vegetable = Value
//		val Avocado, Tomato, Cucumber, Lettuce, Celery, Cabbage, Carrot, Potato, Turnip, Pumpkin, Eggplant, Zucchini, Pepper, Cauliflower, Broccoli = Value
//	}
//	object Taste extends Enumeration {
//		type Taste = Value
//		val Sweet, Sour, Salty, Bitter, Juicy, Oily = Value
//	}
//	object Food extends Enumeration {
//		type Type = Value
//		val Fruit, Vegetable, Spice, Protein, Carbohydrate = Value
//	}
//
//	type Amount = Int
//	type Price = Double
//
//
//
//	av ls = Seq(("Banana", 10, "Fruit", "Low"))
//
//	def usingSessionCreateDataFrameOnSequence(spark: SparkSession,
//									  seq: Seq[(String, String)],
//									  colnames: Seq[String]): DataFrame	= {
//		//import spark.implicits._
//		val df: DataFrame = spark.createDataFrame(seq).toDF(colnames:_*)
//
//		df.printSchema()
//		df.show()
//
//		df
//	}
//
//	/**
//	 * `createDataFrame()` has another signature in spark which takes the util.List of Row type and schema for
//	 * ccolumn names as arguments.
//	 */
//	def usingSessionCreateFataFrameOnSequenceOfRowsWithSchema(spark: SparkSession,
//												   seq: Seq[(String, String)],
//												   colnames: Seq[String]): DataFrame = {
//
//		val seqOfRows: Seq[Row] = seq.map { case (name1, name2) => Row(name1, name2) }
//		usingSessionCreateFataFrameOnSequenceOfRowsWithSchema(spark, seqOfRows, colnames)
//
//	}
//	def usingSessionCreateFataFrameOnSequenceOfRowsWithSchema(spark: SparkSession,
//												   seqOfRows: Seq[Row],
//												   colnames: Seq[String]): DataFrame = {
//		import org.apache.spark.sql.Row
//
//		// NOTE: need to use "JavaConversions" not "JavaConverters" so that the createDataFrame from sequence of rows will work.
//		//import scala.collection.JavaConversions._
//		//import scala.collection.JavaConverters._
//
//		// Sinec scala 2.13 need to use this other import instead: https://stackoverflow.com/a/6357299
//		import scala.collection.JavaConverters._
//		//import scala.jdk.CollectionConverters._
//		//import scala.collection.JavaConversions._
//
//
//		import org.apache.spark.sql.types.{StringType, StructField, StructType}
//		val schema: StructType = StructType(
//			colnames.map(n => StructField(name = n, dataType = StringType, nullable = true))
//		)
//
//
//		val df: DataFrame = spark.createDataFrame(rows = seqOfRows.asJava, schema = schema)
//
//		df.printSchema()
//		df.show()
//		df
//	}
//
//
//	def usingSessionCreateDataFrameOnRDD(spark: SparkSession,
//								  rdd: RDD[(String, String)],
//								  colnames: Seq[String]): DataFrame = {
//		//import spark.implicits._
//
//		val df: DataFrame = spark.createDataFrame(rdd).toDF(colnames: _*)
//
//		df.printSchema()
//		df.show()
//
//		df
//	}
//
//	/**
//	 * `createDataFrame()` has another signature which takes RDD[Row] and a schema for colnames as arguments.
//	 * To use, must first
//	 * 	1. convert rdd object from RDD[T] to RDD[Row], and
//	 * 	2. define a schema using `StructType` and `StructField`
//	 */
//	def usingSessionCreateDataFrameOnRowRDDAndSchema(spark: SparkSession,
//										    rdd: RDD[RowType],
//										    colnames: Seq[String]): DataFrame = {
//		val rowRDD: RDD[Row] = rdd.map{ case (a1, a2) => Row(a1, a2)}
//		usingSessionCreateDataFrameOnRowRDDAndSchema(spark, rowRDD, colnames)
//	}
//	def usingSessionCreateDataFrameOnRowRDDAndSchema(spark: SparkSession,
//										    rowRDD: RDD[Row],
//										    colnames: Seq[String]): DataFrame = {
//
//		import org.apache.spark.sql.Row
//		import org.apache.spark.sql.types.{StringType, StructField, StructType}
//
//		/*val schema = StructType(Array(
//			StructField(name = "language", dataType = StringType, nullable = true),
//			StructField(name = "users_count", dataType = StringType, nullable = true)
//		))*/
//		val schema: StructType = StructType(
//			colnames.map(n => StructField(name = n, dataType = StringType, nullable = true))
//		)
//		val df = spark.createDataFrame(rowRDD = rowRDD, schema = schema)
//
//		df.printSchema()
//		df.show()
//
//		df
//	}
//
//	// TODO - why cannot make RDD[(A, B)] ? instead of string, string?
//	def usingToDFOnRDD(spark: SparkSession,
//				    rdd: RDD[(String, String)],
//				    colnames: Seq[String]): DataFrame = {
//
//		// NOTE: need implicits to call rdd.toDF()
//		import spark.implicits._
//
//		val df_noname: DataFrame = rdd.toDF() // default colnames are _1, _2
//		df_noname.printSchema()
//		df_noname.show() // show all the rows box format
//		assert(df_noname.columns.toList == List("_1", "_2")) // TODO false if comparing arrays??
//
//		val df: DataFrame = rdd.toDF(colnames:_*) // assigning colnames
//		df.printSchema()
//		df.show()
//		assert(df.columns.toList == colnames)
//
//		df
//	}
//
//
//	def usingToDFOnSeq(spark: SparkSession, seq: Seq[(String, String)], colnames: Seq[String]): (DataFrame, DataFrame) = {
//		import spark.implicits._
//
//		val df_noname: DataFrame = seq.toDF()
//		val df: DataFrame = seq.toDF(colnames: _*)
//
//		df.printSchema()
//		df.show()
//
//		(df_noname, df)
//	}
//
//
//
//
//
//
//	// NOTE: to read in multiple csv files, separate their file names with comma = https://hyp.is/ceetdpWBEey3Rnd9naElZQ/sparkbyexamples.com/spark/spark-read-csv-file-into-dataframe/
//	// NOTE to read in all csv files from a folder, must pass in the entire directory name = https://hyp.is/kh1dZpWBEeyggz93IvgE_w/sparkbyexamples.com/spark/spark-read-csv-file-into-dataframe/
//	def usingReadFileByCSV(spark: SparkSession, filepath: String): List[DataFrame] = {
//		val df_noheader = spark.read.csv(filepath)
//
//		val df: DataFrame = spark
//			.read
//			.option(key = "header", value = true)
//			.csv(path = filepath)
//
//		val df_delim: DataFrame = spark
//			.read
//			.options(Map("delimiter" -> ","))
//			.option(key = "header", value = true) // can still get colnames
//			.csv(path = filepath)
//
//		// setting this inferSchema = true infers the column types based on the data
//		val df_inferSchema: DataFrame = spark
//			.read
//			.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
//			.csv(filepath)
//
//		df.printSchema()
//		df.show()
//
//		List(df_noheader, df, df_delim, df_inferSchema)
//	}
//
//	// TODO read with quotes / nullvalues / dateformat = https://hyp.is/Gpl27Jh2Eey9h_sXVK2vZA/sparkbyexamples.com/spark/spark-read-csv-file-into-dataframe/
//
//
//	/**
//	 * Use if you know the schema of the file ahead of time and do not want to use the `inferSchema` option
//	 * for column names and types. Can use a user-defined custom schema.
//	 *
//	 * @param spark
//	 * @param filepath
//	 * @return
//	 */
//	//import org.apache.spark.sql.types.AtomicType
//
//	// Pass in the schema types (stringtype, integertype, ... in order of how they should correspond to column
//	// names, then pair those up with teh column names to make the structtype manually here (using fold)
//
//	def usingReadFileByCSVWithCustomSchema(spark: SparkSession,
//								    schemaNameTypePairs: Seq[(String, DataType)],
//								    /*schema: StructType,*/
//								    filepath: String): DataFrame = {
//
//		val emptyStruct: StructType = new StructType()
//
//		val userSchema: StructType = schemaNameTypePairs.foldLeft(emptyStruct){
//			case (accStruct, (name, tpe)) => accStruct.add(name = name, dataType = tpe, nullable = true)
//		}
//
//		val dfWithSchema: DataFrame = spark.read.format("csv")
//			.option("header", "true")
//			.schema(userSchema)
//			.load(filepath)
//
//		dfWithSchema.printSchema()
//		dfWithSchema.show()
//
//		dfWithSchema
//	}
//
//
//	def usingReadTXTFile(spark: SparkSession, filepath: String): DataFrame = {
//
//		val df = spark.read.text(filepath)
//		df.printSchema()
//		df.show()
//		df
//	}
//
//	def usingReadJSONFile(spark: SparkSession, filepath: String): DataFrame = {
//
//		val df = spark.read.json(filepath)
//		df.printSchema()
//		df.show()
//		df
//	}
//
//	// TODO more xml detail here = https://sparkbyexamples.com/spark/spark-read-write-xml/
//	/*def usingReadXMLFile(spark: SparkSession, filepath: String): DataFrame = {
//
//		import spark.implicits._
//
//		val df = spark.read
//			.format("com.databricks.spark.xml")
//			.option(key = "rowTag", value = "person")
//			.xml(filepath)
//
//		df.printSchema()
//		df.show()
//		df
//	}*/
//}
//
