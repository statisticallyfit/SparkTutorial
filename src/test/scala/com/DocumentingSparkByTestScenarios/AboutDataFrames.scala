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

		it("should use sparkSession's `createDataFrame()` on a scala Seq"){
			usingSessionCreateDataFrameOnSequence(spark: SparkSession,
									  seq: Seq[(String, String)],
									  colnames: Seq[String])
		}
		it("should use sparkSession's `createDataFrame()` on a scala Seq of Rows, with Schema"){
			usingSessionCreateFataFrameOnSequenceOfRowsWithSchema(spark: SparkSession,
												   seq: Seq[(String, String)],
												   colnames: Seq[String])
												   usingSessionCreateFataFrameOnSequenceOfRowsWithSchema(spark: SparkSession,
												   seqOfRows: Seq[Row],
												   colnames: Seq[String])
		}

		it("should use sparkSession's `createDataFrame()` on RDD") {
			usingSessionCreateDataFrameOnRDD(spark: SparkSession,
								  rdd: RDD[(String, String)],
								  colnames: Seq[String])
		}

		it("should use sparkSession's `createDataFrame()` on RDD of Rows, with schema"){
			usingSessionCreateDataFrameOnRowRDDAndSchema(spark: SparkSession,
										    rdd: RDD[(String, String)],
										    colnames: Seq[String])

			usingSessionCreateDataFrameOnRowRDDAndSchema(spark: SparkSession,
										    rowRDD: RDD[Row],
										    colnames: Seq[String])
		}
		it("should use `toDF()` on RDD"){
			usingToDFOnRDD(spark: SparkSession,
				    rdd: RDD[(String, String)],
				    colnames: Seq[String])
		}
		it("should use `toDF()` on Seq"){
			usingToDFOnSeq(spark: SparkSession, seq: Seq[(String, String)], colnames: Seq[String])
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

	// FinancialInstrument=  stocks, bonds, options, derivatives
	// anmials = gi, hippo, crocodile, zebra
	// mineral = graphite, diamond, ruby, pearl
	// preciousmetal = gold, copper, bronze, silver, obsidian
	// weekday seen on = m, t, w ....
	// Gender = m, f

	/*object Vessel extends Enumeration {
		type Vessel = Value
		val Ship, Submarine, Airplane, Helicopter, Car, Truck, Train, Canoe, Kayak = Value
	}*/

	/*object Instrument extends Enumeration {
		type Instrument = Value
		val Musical, Financial, Navigational = Value
	}*/
	/*sealed trait Instrument extends EnumEntry
	// Source = https://www.martek-marine.com/ecdis/pre-ecdis-navigation/

	object Navigational extends Enum[Instrument] {
		val values = findValues
		case object Compass extends Instrument
		case object LandLine extends Instrument
		case object Astrolabe extends Instrument
		case object Pelorus extends Instrument
		case object SandGlass extends Instrument
		case object Quadrant extends Instrument
		case object Nocturnal extends Instrument
		case object Backstaff extends Instrument

	}
	object Musical extends Enum[Instrument] {
		type Instrument = Value
		val Flute, Oboe, Clarinet, Trombone, Tuba, FrenchHorn, Trumpet, Saxophone, Harmonica, Xylophone, Piano, Violin, Harp, Guitar, Cello, Voice = Value
	}

	object Financial extends Enumeration with Instrument[Financial.type] {
		type Instrument = Value
		val Stock, Bond, Option, Derivative, Future, Swap, Equity, Share, Commodity, Cash = Value
	}*/


	object Transaction extends Enumeration {
		type Type = Value
		val Buy, Sell = Value
	}
	object Company extends Enumeration {
		type Firm = Value
		val GoldmanSachs, JPMorgan, Samsung, Ford, Disney, Apple, Google, Microsoft, Tesla, Amazon, Walmart, Nike, Twitter, Facebook, Starbucks, IBM = Value
	}

	sealed abstract case class Instrument(kind: Enumeration)
	case object Fin extends Instrument(Financial.Instrument)
	case object Musical extends Instrument("Musical")



	sealed trait Art extends EnumEntry
	sealed trait Painting extends EnumEntry with Art
	sealed trait Literature extends EnumEntry with Art
	sealed trait LiteraryPeriod extends Literature
	sealed trait LiteratureType extends Literature

	object Art extends Enum[Art] with Art {
		val values: IndexedSeq[Art] = findValues

		case object Painting extends Art
		/*case object Painting extends Enum[Painting] with Painting {
			def values: IndexedSeq[Painting] = findValues
			case object Painter extends Painting with Artist
		}*/
		case object Music extends Art
		// TODO underneath - MusicalInstrument (Voice, Flute etc), MusicalPerson (Musician)
		// TODO set instruments - mix them in as traits into the People Musicians (below)
		case object Literature extends Enum[Literature] with Literature with Art {
			val values: IndexedSeq[Literature] = findValues
			case object LiteratureType extends Enum[LiteratureType] with LiteratureType {
				val values: IndexedSeq[LiteratureType] = findValues
				case object Poetry extends LiteratureType
				case object Essay extends LiteratureType
				case object Novel extends LiteratureType
				case object Prose extends LiteratureType
				case object Fable extends LiteratureType
				case object Drama extends LiteratureType
				case object Mystery extends LiteratureType
				case object Horror extends LiteratureType
				case object Satire extends LiteratureType
				case object Comedy extends LiteratureType
				case object HistoricalFiction extends LiteratureType
				case object Nonfiction extends LiteratureType
				case object Mythology extends LiteratureType
			}
			case object LiteraryPeriod extends Enum[LiteraryPeriod] with LiteraryPeriod {
				val values: IndexedSeq[LiteraryPeriod] = findValues
				case object RomanticismPeriod extends LiteraryPeriod
				case object BluesPeriod extends LiteraryPeriod
				case object OldEnglishPeriod extends LiteraryPeriod
				case object MiddleEnglishPeriod extends LiteraryPeriod
				case object EdwardianPeriod extends LiteraryPeriod
				case object VictorianPeriod extends LiteraryPeriod
				case object MedievalPeriod extends LiteraryPeriod
				case object ModernismPeriod extends LiteraryPeriod
				case object RenaissancePeriod  extends LiteraryPeriod
			}
		}
		// TODO list eras - Romanticisim, ... (Jane Austen)
		// TODO lsit types of literature - Poetry, autobiography
		case object Sculpture extends Art
		case object Architecture extends Art
		case object Theatre extends Art
		case object Cinema extends Art

	}
	//val a1: Artist = Art.Painting.Painter


	sealed trait Human extends EnumEntry
	sealed trait Artist extends /*EnumEntry with*/ Human
	sealed trait Painter extends /*EnumEntry with*/ Artist
	sealed trait Musician extends Artist
	sealed trait Writer extends Artist
	sealed trait Sculptor extends Artist
	sealed trait Architect extends Artist
	sealed trait Dancer extends Artist
	sealed trait Singer extends Artist

	object Human extends Enum[Human] with Human {
		val values: IndexedSeq[Human] = findValues

		case object VanGogh extends Human with Painter
		case object LeonardoDaVinci extends Human with Painter with Sculptor
		case object Michelangelo extends Human with Painter with Sculptor
		case object ClaudeMonet extends Human with Painter
		case object Rembrandt extends Human with Painter
		case object ElGreco extends Human with Painter
		//-------------------------------
		// Voice
		case object SarahBrightman extends Human with Musician with Singer
		case object Adele extends Human with Musician with Singer
		case object Beyonce extends Human with Musician with Singer
		case object PhilCollins extends Human with Musician with Singer
		case object RodStewart extends Human with Musician with Singer
		case object AlannisMorrisette extends Human with Musician with Singer
		// Saxophone
		case object JohnColtrane extends Human with Musician
		case object PaulDesmond extends Human with Musician
		case object SonnyStitt extends Human with Musician
		// Trombone
		case object JackTeagarden extends Human with Musician
		case object FredWesley extends Human with Musician
		// Tuba
		case object MinLeiBrook extends Human with Musician
		case object WalterEnglish extends Human with Musician
		case object SquireGersh extends Human with Musician
		// Flute
		case object HerbieMann extends Human with Musician
		case object TheobaldBoehm extends Human with Musician
		case object YusefLateef extends Human with Musician
		// Trumpet, voice
		case object LouisArmstrong extends Human with Musician
		// Piano, voice
		case object JellyRollMorton extends Human with Musician
		// Violin
		case object NiccoloPaganini extends Human with Musician
		case object ViktoriaMullova extends Human with Musician
		case object GeorgeEnescu extends Human with Musician
		// ---------------
		case object AlfredLordTennyson extends Human with Writer
		case object LordByron extends Human with Writer
		case object WilliamShakespeare extends Human with Writer
		case object CharlesDickens extends Human with Writer
		case object EmilyDickenson extends Human with Writer
		case object JaneAusten extends Human with Writer
		case object JulesVerne extends Human with Writer
		case object EdgarAllanPoe extends Human with Writer
		case object TSEliot extends Human with Writer
		case object HansChristianAnderson extends Human with Writer
		case object MarkTwain extends Human with Writer
		case object JamesJoyce extends Human with Writer
		case object LeoTolstoy extends Human with Writer
		case object FScottFitzgerald extends Human with Writer
		case object GeorgeOrwell extends Human with Writer
		case object HermanMelville extends Human with Writer
		case object RoaldDahl extends Human with Writer
		case object AlexandreDumas extends Human with Writer
		case object AgathaChristie extends Human with Writer
		case object ErnestHemingway extends Human with Writer
		case object HermanHesse extends Human with Writer
		case object AntonChekhov extends Human with Writer
		case object AlexanderPushkin extends Human with Writer
		case object GeoffreyChaucer extends Human with Writer

		// ------------------------------------------------
		case object ConstantinBracusi extends Human with Sculptor
		case object AugusteRodin extends Human with Sculptor
		// ------------------------------------------------
		case object AntoniGaudi extends Human with Architect
		case object WilliamPereira extends Human with Architect
		// ------------------------------------------------
		case object AnnaPavlova extends Human with Dancer
		case object RudolfNureyev extends Human with Dancer


		object Artist extends Enum[Artist] with Artist {
			val values: IndexedSeq[Artist] = findValues
			case object Painter extends Artist
			/*case object Painter extends Enum[Painter] with Artist {
				val values: IndexedSeq[Painter] = findValues
				case object VanGogh extends Painter
				case object LeonardoDaVinci extends Painter
			}*/
			case object Musician extends Artist
			case object Writer extends Artist
			case object Sculptor extends Artist
			case object Architect extends Artist
			case object Actor extends Artist // theatre, cinema
			case object Dancer extends Artist // theatre
			case object Singer extends Artist // theatre
		}
	}
//	val v1: Human  = Human.VanGogh
//	val v2: Painter = Human.VanGogh
//	val v2: Artist = Human.Artist.Painter.VanGogh
//	val v3: Painter = Human.Artist.Painter.VanGogh



	sealed trait Animal extends EnumEntry
	sealed trait Cat extends Animal with EnumEntry
	sealed trait Bird extends Animal with EnumEntry
	object Animal extends Enum[Animal] {
		val values: IndexedSeq[Animal] = findValues //AnimalEnum.values.toIndexedSeq.map(_.asInstanceOf[Animal])
		case object Giraffe extends Animal
		case object Crocodile extends Animal
		case object Hippo extends Animal
		case object Elephant extends Animal
		case object Zebra extends Animal
		case object Hyena extends Animal
		case object Panda extends Animal
		case object Koala extends Animal
		case object Gorilla extends Animal
		case object Snake extends Animal
		case object Termite extends Animal
		case object Bear extends Animal
		case object Reindeer extends Animal
		case object Squirrel extends Animal
		case object Marmot extends Animal
		case object Weasel extends Animal
		case object Rabbit extends Animal
		case object Fox extends Animal

		object Cat extends Enum[Cat] with Cat {
			val values: IndexedSeq[Cat] = findValues
			case object Cougar extends Cat
			case object Lion extends Cat
			case object Tiger extends Cat
			case object Lynx extends Cat
			case object Panther extends Cat
			case object Leopard extends Cat
			case object MountainLion extends Cat
			case object SabertoothedTiger extends Cat

			object HouseCat extends Enum[Cat] with Cat {
				val values: IndexedSeq[Cat] = findValues
				case object PersianCat extends Cat
				case object ShorthairedCat extends Cat
				case object SiameseCat extends Cat
			}
		}

		object Bird extends Enum[Bird] with Bird {
			val values: IndexedSeq[Bird] = findValues
			case object Pelican extends Bird
			case object Flamingo extends Bird
			case object Albatross extends Bird
			case object Vulture extends Bird
			case object Hawk extends Bird
			case object Canary extends Bird
			case object Parrot extends Bird
			case object Sparrow extends Bird
			case object Robin extends Bird
			case object Chickadee extends Bird
			case object Raven extends Bird
			case object Crow extends Bird
			case object Bluejay extends Bird
			case object Mockingbird extends Bird
			case object Penguin extends Bird
			case object Ostrich extends Bird
			object Eagle extends Enum[Bird] with Bird {
				val values: IndexedSeq[Bird] = findValues
				case object BaldEagle extends Bird
				case object GoldenEagle extends Bird
			}
		}
	}
	//val p1: Animal = Animal.cat.houseCat.PersianCat
	//Animal.Bird.Eagle.GoldenEagle
	//Animal.Bird.Canary







	sealed trait CelestialBody extends EnumEntry
	sealed trait Planet extends EnumEntry with CelestialBody
	object CelestialBody extends Enum[CelestialBody] with CelestialBody {
		val values: IndexedSeq[CelestialBody] = findValues

		case object BlackHole extends CelestialBody
		case object Star extends CelestialBody
		case object Sun extends CelestialBody
		case object Moon extends CelestialBody
		case class Nebula() extends CelestialBody
		case object Galaxy extends CelestialBody
		case class Asteroid() extends CelestialBody
		case class Meteor() extends CelestialBody
		//case object Planet extends CelestialBody

		object Planet extends Enum[Planet] with Planet {
			val values: IndexedSeq[Planet] = findValues
			case object Mercury extends Planet
			case object Mars extends Planet
			case object Jupiter extends Planet
			case object Venus extends Planet
			case object Pluto extends Planet
		}
	}
	/*val c1: CelestialBody = CelestialBody.Planet.Venus
	val c2: Planet = CelestialBody.Planet.Mars
	val c3: CelestialBody = CelestialBody.Galaxy
	val c4: CelestialBody = CelestialBody.Nebula()*/


	object Climate extends Enumeration {
		type Climate = Value
		val Temperate, Arid, Tundra, Polar, Tropical, Rainforest, Dry, Desert, Mediterranean, Continental = Value
	}

	object Country extends Enumeration {
		type Country = Value
		val Africa, China, Russia, Spain, France, Arabia, Scotland, Ireland, England, Romania, Estonia, America, Canada, Brazil, CostaRica, Argentina, Australia = Value
	}





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

