package utilities

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


import scala.reflect.runtime.universe._

import utilities.GeneralMainUtils._
import utilities.GeneralMainUtils.implicits._
import utilities.DFUtils
import DFUtils.implicits._
import DFUtils.TypeAbstractions._
import utilities.EnumHub._
import utilities.EnumUtils.implicits._

import World.Africa._
import World.Europe._
import World.NorthAmerica._
import World.SouthAmerica._
import World._
import World.Asia._
import World.Oceania._
import World.CentralAmerica._


/**
 * GOAL: create datasets here already preloaded so not have to load multiple times
 */
object DataHub /*extends SparkSessionWrapper*/ /*with App*/ {


	val sess: SparkSession = SparkSession.builder().master("local[1]").appName("datahub").getOrCreate()
	//import sparkMainSession.implicits._
	//withLocalSparkContext(sess => {
	//val sess = sparkSessionWrapper
	import sess.implicits._




	object ImportedDataFrames {

		//val PATH: String = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/data/BillChambers_SparkTheDefinitiveGuide"

		val DATA_PATH: String = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/data"
		val FORMAT_JSON: String = "json"
		val FORMAT_CSV: String = "csv"


		val folderBlogs: String  = "blogs"
		val folderBillChambers: String = "BillChambers_SparkTheDefinitiveGuide"
		val folderDamji: String = "Damji_LearningSpark"
		val folderHolden: String = "ZahariaHolden_LearningSpark"
		val folderJeanGeorgesPerrin: String = "JeanGeorgesPerrin_SparkInAction"

		val folderInputData: String = "inputData"
		val folderOutputData: String = "outputData"


		object fromBillChambersBook /*(sess: SparkSession)*/ {

			val flightDf: DataFrame = sess.read.format(FORMAT_JSON).load(s"$DATA_PATH/$folderBillChambers/$folderInputData/flight-data/json/2015-summary.json")
			val colnamesFlight: Seq[NameOfCol] = flightDf.columns.toSeq
			val coltypesFlight: Seq[TypenameOfCol] = flightDf.schema.fields.map(f => DFUtils.dataTypeToStrName(f.dataType)).toSeq


			val colIDSFlight: Map[NameOfCol, Int] = DFUtils.colnamesToIndices(flightDf)
			//val bikeDf: DataFrame = sparkSession.read.format(FORMAT_JSON).load( s"$PATH/$folderBillChambers/bike-data")
		}

		object fromDamjiBook {

			// Chapter 2:
			val fileMnM = s"$DATA_PATH/$folderDamji/$folderInputData/mnm_dataset.csv"

			val mnmDf: DataFrame = (sess.read.format("csv")
				.option(key = "header", value = true)
				.option(key = "inferSchema", value = true)
				.load(fileMnM))

		}

		object fromHoldenBook {


		}

		object fromJeanPerrinBook {

		}

	}

	object ManualDataFrames {

		object ArrayDf {
			val initialDf = Seq(
				("x", 4, 1),
				("x", 6, 2),
				("z", 7, 3),
				("a", 3, 4),
				("z", 5, 2),
				("x", 7, 3),
				("x", 9, 7),
				("z", 1, 8),
				("z", 4, 9),
				("z", 7, 4),
				("a", 8, 5),
				("a", 5, 2),
				("a", 3, 8),
				("x", 2, 7),
				("z", 1, 9)
			).toDF("col1", "col2", "col3")

			val arrayGroupDf: DataFrame = (initialDf.groupBy("col1")
				.agg(collect_list(col("col2")).as("ArrayCol2"), collect_list(col("col3")).as("ArrayCol3")))


			import scala.Double.NaN
			import utilities.GeneralMainUtils.Helpers._

			val initialNullDf = Seq(
				("x", 2, null),
				("x", null, 5),
				("z", null, 1),
				("z", null, null),
				("a", null, 9),
				("a", 4, null),
				("x", 1.3, NaN),
				("x", NaN, 2.4),
				("x", NaN, NaN),
				("a", NaN, NaN),
				("a", 2, NaN),
				("a", NaN, 5.3),
				("x", 4, 1),
				("x", 6, 2),
				("z", 7.5, 3),
				("a", 3, 4.5),
				("z", 5.8, 2.7),
				("x", 7.6, 3),
				("x", 9, 7.8),
				("x", null, 1),
				("x", 8, null),
				("z", 8.8, NaN),
				("z", NaN, 8),
				("z", NaN, NaN),
				("z", 1.1, 8),
				("z", 4, 9.7),
				("z", 0.3, null),
				("z", null, null),
				("z", 7, 4),
				("a", 8.1, 5),
				("a", 5, 2.6),
				("a", 3, 8.9),
				("a", 3.4, null),
				("a", null, null),
				("x", 1, null),
				("x", 2, 7.4),
				("z", 1.2, 9)
			).map { case (e1, e2, e3) => {
				(e1.toString, getSimpleString(e2), getSimpleString(e3)) // this function covers the null case

			}}.toDF("col1", "col2", "col3")

			val arrayNullGroupDf: DataFrame = (initialNullDf.groupBy("col1")
				.agg(collect_list(col("col2")).as("ArrayCol2"), collect_list(col("col3")).as("ArrayCol3")))

			// ------

			// SOURCE: https://towardsdatascience.com/the-definitive-way-to-sort-arrays-in-spark-1224f5529961
			case class Person(name: String, age: Int)

			val simplePeopleDf: DataFrame = Seq(
				Array(Person("William", 23), Person("Katarina", 14), Person("Jude", 18), Person("Meredith", 8)),
				Array(Person("Casper", 30), Person("Alexandra", 13), Person("George", 19), Person("Serafina", 28), Person("Zara", 3)),
				Array(Person("Berenice", 89), Person("Xenia", 38), Person("Delilah", 79), Person("Paige", 24))
			).toDF("people")


			// --------------
			// SOURCE: https://stackoverflow.com/questions/54954732/spark-scala-filter-array-of-structs-without-explode
			object personInfo {
				val Quan = "Quan"
				val Quinn = "Quinn"
				val Katerina = "Katerina"
				val Catherine = "Catherine"
				val Helen = "Helen"
				val Hannah = "Hannah"
				val Hazel = "Hazel"
				val Harriet = "Harriet"
				val Henry = "Henry"
				val Hector = "Hector"
				val Harry = "Harry"
				val Liliana = "Liliana"
				val Amber = "Amber"
				val Astrid = "Astrid"
				val Berenice = "Berenice"
				val Bella = "Bella"
				val Bridget = "Bridget"
				val Brianna = "Brianna"
				val Bethany = "Bethany"
				val Blake = "Blake"
				val Bonnie = "Bonnie"
				val Hugo = "Hugo"
				val Victor = "Victor"
				val Jasper = "Jasper"
				val Naza = "Naza"
				val Nesryn = "Nesryn"
				val Niki = "Niki"
				val Nicole = "Nicole"
				val Nanette = "Nanette"
				val Nina = "Nina"
				val Penelope = "Penelope"
				val Natalia = "Natalia"
				val Pauline = "Pauline"
				val Xenia = "Xenia"
				val Xavier = "Xavier"
				val Tijah = "Tijah"
				val Dmitry = "Dmitry"
				val Vesper = "Vesper"
				val Yigor = "Yigor"
				val Tyler = "Tyler"
				val Tatiana = "Tatiana"
				val Sascha = "Sascha"
				val Selene = "Selene"
				val Stacey = "Stacey"
				val Sigurd = "Sigurd"
				val Sarah = "Sarah"
				val Sabrina = "Sabrina"
				val Sabrielle = "Sabrielle"
				val Sophie = "Sophie"

				val aaa = "aaa"
				val bbb = "bbb"
				val ccc = "ccc"
				val ddd = "ddd"
				val eee = "eee"
				val fff = "fff"
				val ggg = "ggg"
				val hhh = "hhh"
				val iii = "iii"
				val jjj = "jjj"
				val kkk = "kkk"
				val lll = "lll"
				val mmm = "mmm"
				val nnn = "nnn"
				val ooo = "ooo"
				val ppp = "ppp"
				val qqq = "qqq"
				val rrr = "rrr"
				val sss = "sss"
				val ttt = "ttt"
				val uuu = "uuu"
				val vvv = "vvv"
				val www = "www"
				val xxx = "xxx"
				val yyy = "yyy"
				val zzz = "zzz"
			}

			import personInfo._

			val personDf = (Seq(
				("a", 7, Astrid, jjj, "555", 12),
				("a", 3, Quan, zzz, "345", 11),
				("a", 10, Quinn,zzz, "345", 11),
				("a", 3, Helen, ggg, "191", 30),
				("a", 8, Liliana, ddd, "332", 40),
				("a", 7, Amber, jjj, "443", 11),
				("a", 3, Hugo, xxx, "324", 30),
				("a", 2, Victor, yyy, "223", 45),
				("a", 1, Jasper, xxx, "1", 27),

				("b", 1, Penelope, ppp, "345", 52),
				("b", 5, Pauline, ppp, "111", 52),
				("b", 4, Xenia, eee, "13", 9),
				("b", 1, Natalia, nnn, "678", 15),
				("b", 9, Blake, bbb, "445", 19),
				("b", 9, Brianna, bbb, "442", 19),
				("b", 9, Bonnie, bbb, "441", 19),
				("b", 9, Berenice, bbb, "430", 19),
				("b", 9, Bridget, bbb, "412", 19),
				("b", 9, Bella, bbb, "417", 19),

				("c", 1, Yigor, vvv, "34", 11),
				("c", 3, Tyler, vvv, "111", 10),
				("c", 10, Tijah, vvv, "0", 10),
				("c", 4, Katerina, iii, "19", 19),
				("c", 17, Catherine, iii, "138", 90),
				("c", 34, Dmitry, kkk, "787", 23),
				("c", 9, Vesper, kkk, "348", 25),
				("c", 3, Tatiana, vvv, "123", 10),

				("n", 1, Naza, nnn, "131", 15),
				("n", 1, Nesryn, nnn, "128", 15),
				("n", 1, Niki, nnn, "155", 15),
				("n", 1, Nicole, nnn, "154", 15),
				("n", 1, Nanette, nnn, "152", 15),
				("n", 1, Nina, nnn, "140", 15),

				("h", 5, Hazel, hhh, "143", 5),
				("h", 5, Harriet, hhh, "142", 5),
				("h", 5, Henry, hhh, "111", 5),
				("h", 5, Harry, hhh, "992", 5),
				("h", 5, Hannah, hhh, "934", 5),

				("s", 20, Sascha, ooo, "112", 22),
				("s", 20, Selene, ooo, "134", 22),
				("s", 20, Sarah, ooo, "122", 22),
				("s", 20, Sophie, ooo, "156", 21),
				("s", 20, Stacey, ooo, "189", 14),
				("s", 14, Sabrielle, ooo, "444", 22),
				("s", 20, Sabrina, ooo, "433", 22),
				("s", 20, Sigurd, ooo, "332", 21)

			).toDF("groupingKey", "id", "name", "middleInitialThrice", "addressNumber", "age")
				.groupBy("groupingKey")
				.agg(collect_list(struct("id", "name", "middleInitialThrice", "addressNumber", "age")).as("yourArray")))

			val personUniqueMidDf = (Seq(
				("a", 3, Quan, zzz, "345", 11),
				("a", 10, Quinn, kkk, "345", 11),
				("a", 3, Helen, ggg, "191", 30),
				("a", 8, Liliana, ddd, "332", 40),
				("a", 7, Amber, jjj, "443", 11),
				("a", 7, Astrid, aaa, "555", 12),
				("a", 3, Hugo, lll, "324", 30),
				("a", 2, Victor, yyy, "223", 45),
				("a", 1, Jasper, xxx, "1", 27),
				("b", 1, Naza, nnn, "131", 15),
				("b", 1, Nesryn, rrr, "128", 15),
				("b", 1, Penelope, ppp, "345", 52),
				("b", 1, Natalia, eee, "678", 15),
				("b", 5, Pauline, ttt, "111", 52),
				("b", 4, Xenia, bbb, "13", 9),
				("c", 10, Tijah, vvv, "0", 10),
				("c", 4, Katerina, iii, "19", 19),
				("c", 17, Catherine, ccc, "138", 90),
				("c", 34, Dmitry, fff, "787", 23),
				("c", 9, Vesper, hhh, "348", 25),
				("c", 1, Yigor, "ooo", "34", 11),
				("c", 3, Tatiana, mmm, "123", 10),
				("c", 3, Tyler, qqq, "111", 10),
			).toDF("groupingKey", "id", "name", "middleInitialThrice", "addressNumber", "age")
				.groupBy("groupingKey")
				.agg(collect_list(struct("id", "name", "middleInitialThrice", "addressNumber", "age")).as("yourArray")))

			/**
			 * NOTE: reason for moving these classes to the datahub object is that dataset[obj] needs spark sql context implicits and these classes separately declare
			 * SOURCE: https://stackoverflow.com/a/44774366
 			 */
			case class RecordRaw(groupingKey: String, yourArray: Seq[(Int, String, String, String, Int)])

			case class PersonStruct(id: Int, name: String, middleInitialThrice: String, addressNumber: String, age: Int)
			case class Record(groupingKey: String, yourArray: Seq[PersonStruct])

			val personRecDs: Dataset[RecordRaw] = personDf.as[RecordRaw]
			val personDs: Dataset[Record] = personDf.as[Record]
			val personRDD: RDD[Record] = sess.sparkContext.parallelize(personDs.collect().toSeq)

			//case class YourPersonStruct(id: Int, someProperty: String, someOtherProperty: String, propertyToFilterOn: Int)
			case class PersonArray(yourArray: Seq[PersonStruct])

			val personArrayDs: Dataset[PersonArray] = personDf.as[PersonArray]
			val personArrayRDD: RDD[PersonArray] = sess.sparkContext.parallelize(personArrayDs.collect().toSeq)



			// NOTE: here constructing temporary classes to fit the data frames that are created during the tests of ArraySpecs (for array_sort)
			// Rule: the argument has to match the created df's column name.
			case class SortByMidStruct[P](groupingKey: String, sortedByMiddle: Seq[P])
			case class SortByNameStruct[P](groupingKey: String, sortedByName: Seq[P])
			case class SortByIDStruct[P](groupingKey: String, sortedByID: Seq[P])
			case class SortByAgeStruct[P](groupingKey: String, sortedByAge: Seq[P])
			case class SortByAddressStruct[P](groupingKey: String, sortedByAddress: Seq[P])
			case class SortStruct[P](groupingKey: String, sorted: Seq[P])


			// Can put these inside the dataset templates above, as P struct
			case class PersonMidFirstStruct(middleInitialThrice: String, id: Int, age: Int, addressNumber: String, name: String)
			case class PersonMidIdNameAgeStruct(middleInitialThrice: String, id: Int, name: String, age: Int)
			//case class DatasetofPersonRearrangedMidFirst(groupingKey: String, sortedByMiddle: Seq[PersonMidFirstStruct])
			case class PersonMidIDStruct(middleInitialThrice: String, id: Int)
			case class PersonNameIdStruct(name: String, id: Int)
			case class PersonMidNameIDStruct(middleInitialThrice: String, name: String, id: Int)
			case class PersonMidIDNameStruct(middleInitialThrice: String, id: Int, name: String)


			case class Score(id: Int, num: Int)
		}

		object XYRandDf {

			import scala.jdk.CollectionConverters._
			import scala.util.Random

			val n = 10

			val xs: Seq[Int] = Seq.fill(n)(Random.between(0, 20))
			val ys: Seq[Int] = Seq.fill(n)(Random.between(0, 20))
			val zs: Seq[Int] = xs.zip(ys).map { case (x, y) => x + y }

			val sch: StructType = DFUtils.createSchema(names = Seq("x", "y", "z"), tpes = Seq(IntegerType, IntegerType, IntegerType))

			val seqOfRows: Seq[Row] = Seq(xs, ys, zs).transpose.map(Row(_: _*))
			val df: DataFrame = sess.createDataFrame(seqOfRows.asJava, sch)
		}

		object XYNumDf {
			import scala.jdk.CollectionConverters._

			val xs: Seq[Int] = List(1, 2, 1, 1, 3, 4, 5, 8, -9, 10, -11, 1, 3, 5, 1, 7, 8, 1, 4, 10)
			val ys: Seq[Int] = List(4, 3, 1, 1, 4, 1, 7, 8, 1, 2, 1, 14, 5, 1, 7, 8, 9, 1, 2, 7)
			//val zs: Seq[Int] = xs.zip(ys).map { case (x, y) => x + y }
			val bs: Seq[Int] = List(5, 5, 0, 9, 1, 5, 7, 8, -7, 1, 0, 12, 5, 3, 1, 8, 5, 4, 3, 11)

			val xySchema: StructType = DFUtils.createSchema(names = Seq("x", "y"), tpes = Seq(IntegerType, IntegerType))

			val seqOfRows: Seq[Row] = Seq(xs, ys).transpose.map(Row(_: _*))
			val numDf: DataFrame = sess.createDataFrame(seqOfRows.asJava, xySchema)

			val xbySchema = DFUtils.createSchema(Seq("x","b","y"), Seq(IntegerType, IntegerType, IntegerType))
			val betweenDf: DataFrame = sess.createDataFrame(Seq(xs, bs, ys).transpose.map(Row(_:_*)).asJava, xbySchema)
		}

		object XYNumOptionDf {
			import scala.jdk.CollectionConverters._

			val xns: Seq[Option[Int]] = List(Some(1), Some(2), Some(1), Some(1), Some(3), None, Some(5), None, Some(-9), Some(10), Some(-11), Some(1), Some(3), Some(5), Some(1), Some(7), Some(8), Some(1), Some(4), Some(7))

			val yns: List[Option[Int]] = List(Some(4), Some(3), Some(1), Some(1), Some(4), Some(1), Some(7), Some(8), Some(1), None, Some(1), Some(14), Some(5), Some(1), Some(7), Some(8), None, Some(1), Some(2), Some(10))

			val sch: StructType = DFUtils.createSchema(names = Seq("xn", "yn"), tpes = Seq(IntegerType, IntegerType))

			val seqOfRows: Seq[Row] = Seq(xns, yns).transpose.map(Row(_: _*))
			val numOptionDf: DataFrame = sess.createDataFrame(seqOfRows.asJava, sch)
		}

		object BooleanData {

			val booleanDf: DataFrame = Seq((true, true), (true, false), (false, true), (false, false)).toDF("a", "b")
		}


		object fromSparkByExamples {

			import scala.jdk.CollectionConverters._



			val dfWO: DataFrame = List(("James ", "", "Smith", "36636", "M", 60000),
				("Michael ", "Rose", "", "40288", "M", 70000),
				("Robert ", "", "Williams", "42114", "", 400000),
				("Maria ", "Anne", "Jones", "39192", "F", 500000),
				("Jen", "Mary", "Brown", "", "F", 0)
			).toDF("FirstName","MiddleName","LastName","Birthday","Gender","Salary")


			//  ------------------------------------------------------------------------


			val dataNested_1: Seq[Row] = Seq(
				Row(Row("James ", "", "Smith"), "36636", "M", 3000),
				Row(Row("Michael ", "Rose", ""), "40288", "M", 4000),
				Row(Row("Robert ", "", "Williams"), "42114", "M", 4000),
				Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 4000),
				Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
			)
			val schemaNested_1: StructType = (new StructType()
				.add("name", new StructType()
					.add("firstname", StringType)
					.add("middlename", StringType)
					.add("lastname", StringType))
				.add("dob", StringType)
				.add("gender", StringType)
				.add("salary", IntegerType))

			val dfNested_1: DataFrame = sess.createDataFrame(dataNested_1.asJava, schemaNested_1)

			// Step 1 - create new schema stating the new names
			val innerRenameSchema: StructType = (new StructType()
				.add("FirstName", StringType)
				.add("MiddleName", StringType)
				.add("LastName", StringType))

			val nestedRenamedSchema_1: StructType = (new StructType()
				.add("Name",
					innerRenameSchema)
				.add("DateOfBirth", StringType)
				.add("gender", StringType)
				.add("salary", IntegerType))


			//  ------------------------------------------------------------------------

			val dataNested_2: Seq[Row] = Seq(
				Row(Row("James ", "", "Smith"), Row(Row("CA", "Los Angeles"), Row("CA", "Sandiago"))),
				Row(Row("Michael ", "Rose", ""), Row(Row("NY", "New York"), Row("NJ", "Newark"))),
				Row(Row("Robert ", "", "Williams"), Row(Row("DE", "Newark"), Row("CA", "Las Vegas"))),
				Row(Row("Maria ", "Anne", "Jones"), Row(Row("PA", "Harrisburg"), Row("CA", "Sandiago"))),
				Row(Row("Jen", "Mary", "Brown"), Row(Row("CA", "Los Angles"), Row("NJ", "Newark")))
			)
			val schemaNested_2: StructType = (new StructType()
				.add("name", new StructType()
					.add("firstname", StringType)
					.add("middlename", StringType)
					.add("lastname", StringType))
				.add("address", new StructType()
					.add("current", new StructType()
						.add("state", StringType)
						.add("city", StringType))
					.add("previous", new StructType()
						.add("state", StringType)
						.add("city", StringType))
				))
			val dfNested_2: DataFrame = sess.createDataFrame(dataNested_2.asJava, schemaNested_2)

			// val checkNestingSchema_2: StructType =


			//  ------------------------------------------------------------------------



			// TODO must update this data to reflect the schema! (tme, num sponsers ...)

			val dataCasting: Seq[Row] = Seq(
				Row("James, A, Smith", "S", 34, "2006-01-01", "2006-01-01 09:17:34", 3, true, "M", 3000.60).mapRowStr,
				Row("Michael, Rose, Jones", "J", 33, "1980-01-10", "1980-01-10 23:11:11", 14, true, "F", 3300.80).mapRowStr,
				Row("Robert,K,Williams", "W", 37, "06-01-1992", "1992-06-01 14:02:01", 10, false, "M", 5000.50).mapRowStr,
				Row("Maria,Anne,Jones", "J", 21, "08-30-2022", "08-30-2022 01:15:30", 100, true, "F", 12000).mapRowStr
			)
			//TimestampType: yyyy-MM-dd HH:mm:ss
			// supported types are: string, boolean, byte, short, int, long, float, double, decimal, date, timestamp.
			val schemaCasting = StructType(Array(
				StructField("NameString", StringType, true),
				StructField("lastNameInitial", StringType, true), // char
				StructField("age", StringType, true), // double, float, decimal, integer, string, byte
				StructField("jobStartDate", StringType, true), //date
				StructField("jobStartTime", StringType, true), // timestamp
				StructField("numberOfSponsors", StringType, true), // short, long, int
				StructField("isGraduated", StringType, true), //bool
				StructField("gender", StringType, true),
				StructField("salary", StringType, true) //double , float, decimal
			))
			//val dfCast: DataFrame = sess.createDataFrame(sess.sparkContext.parallelize(dataCasting), schemaCasting)
			val dfCast: DataFrame = sess.createDataFrame(dataCasting.asJava, schemaCasting)

			val checkCastingSchema = StructType(Array(
				StructField("NameArray", ArrayType(StringType, false)), // TODO how to make this true when casting in the tests?
				//StructField("NameString", StringType, true),
				// HELP result of this cast is String not char why
				//StructField("lastNameInitial", CharType(1), true), // char
				StructField("lastNameInitial", StringType, true), // char
				StructField("age", IntegerType, true), // double, float, decimal, integer, string, byte
				StructField("jobStartDate", DateType, true), //date
				StructField("jobStartTime", TimestampType, true), // timestamp
				StructField("numberOfSponsors", ShortType, true), // short, long, int
				StructField("isGraduated", BooleanType, true), //bool
				StructField("gender", StringType, true),
				StructField("salary", DoubleType, true), //double , float, decimal
			))

			//  ------------------------------------------------------------------------

			// SOURCE = https://sparkbyexamples.com/spark/spark-dataframe-where-filter/
			val dataArray: Seq[Row] = Seq(
				Row(Row("James", "", "Smith"), List("Java", "Scala", "C++"), "OH", "M"),
				Row(Row("Anna", "Rose", ""), List("Spark", "Java", "C++"), "NY", "F"),
				Row(Row("Julia", "", "Williams"), List("CSharp", "VB"), "OH", "F"),
				Row(Row("Maria", "Anne", "Jones"), List("CSharp", "VB"), "NY", "M"),
				Row(Row("Jen", "Mary", "Brown"), List("CSharp", "VB"), "NY", "M"),
				Row(Row("Mike", "Mary", "Williams"), List("Python", "VB"), "OH", "M")
			)

			// Create StructType ofr schema
			val schemaArray: StructType = (new StructType()
				.add("name", new StructType()
					.add("firstname", StringType)
					.add("middlename", StringType)
					.add("lastname", StringType))
				.add("languages", ArrayType(StringType))
				.add("state", StringType)
				.add("gender", StringType))

			val dfArray: DataFrame = sess.createDataFrame(dataArray.asJava, schemaArray)
		}

		object fromQuierozfBlog {

			//Date
		}

		object fromAlvinHenrickBlog /*(sess: SparkSession)*/ {

			// TODO put source link (alvin henrick from windowing study)

			val empDf: DataFrame = sess.createDataFrame(Seq(
				(7369, "Smith", "Clerk", 7902, "17-Dec-80", 800, 20, 10),
				(7499, "Allen", "Salesman", 7698, "20-Feb-81", 1600, 300, 30),
				(7521, "Ward", "Salesman", 7698, "22-Feb-81", 1250, 500, 30),
				(7566, "Jones", "Manager", 7839, "2-Apr-81", 2850, 0, 20),
				(7654, "Marin", "Salesman", 7698, "28-Sep-81", 1250, 1400, 30),
				(7698, "Blake", "Manager", 7839, "1-May-81", 2850, 0, 30),
				(7782, "Clark", "Manager", 7839, "9-Jun-81", 2850, 0, 10),
				(7788, "Scott", "Analyst", 7566, "19-Apr-87", 3000, 0, 20),
				(7839, "King", "President", 0, "17-Nov-81", 5000, 0, 10),
				(7844, "Turner", "Salesman", 7698, "8-Sep-81", 1500, 0, 30),
				(7876, "Adams", "Clerk", 7788, "23-May-87", 800, 0, 20),
				// Extra rows here to ensure large enough partitions
				(7342, "Johanna", "Manager", 8923, "14-Apr-99", 2343, 1, 11),
				(5554, "Patrick", "Manager", 8923, "12-May-99", 2343, 2, 5),
				(2234, "Stacey", "Manager", 8923, "5-May-01", 3454, 4, 8),
				(2343, "Jude", "Analyst", 8923, "5-Sept-22", 6788, 3, 5),
				(5676, "Margaret", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
				(5676, "William", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
				(5676, "Bridget", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
				(5676, "Kieroff", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
				(5676, "Quinlin", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
				(6787, "Sarah", "Analyst", 8923, "17-Jun-78", 6788, 1, 2),
				(2342, "David", "President", 8923, "23-Jan-89", 500, 11, 1),
				(2345, "Christian", "Clerk", 8923, "31-Jul-98", 2343, 12, 10),
				(3456, "John", "Salesman", 8923, "21-Dec-00", 2343, 21, 21),
				(7898, "Lizbeth", "President", 8923, "11-Oct-11", 2343, 22, 34),

			)).toDF("EmpNum", "EmpName", "Job", "Mgr", "Hiredate", "Salary", "Comm", "DeptNum")
			val colIDsEmp = DFUtils.colnamesToIndices(empDf)


			//val viewCols: Seq[ColumnName] = List($"EmpName", $"Job", $"Salary")
			val viewCols: Seq[String] = List("EmpName", "Job", "Salary")
			val dropCols: Seq[String] = List("EmpNum", "Mgr", "Hiredate", "Comm", "DeptNum")


			// NOTE: to do sql query way must create temporary view
			empDf.createOrReplaceTempView("empDf")

			//empDf
		}


		object fromEnums {


			import shapeless._
			import shapeless.ops.hlist._
			//import shapeless.ops.tuple._
			//import syntax.std.tuple._ // WARNING either this or product
			import shapeless.ops.product._
			import syntax.std.product._

			//import shapeless.ops.traversab

			import scala.language.implicitConversions

			import scala.collection.JavaConverters._
			import org.apache.spark.sql.types.{StringType, StructField, StructType}

			import utilities.GeneralMainUtils._
			import utilities.EnumUtils.implicits._
			import utilities.EnumHub._




			object TradeDf {

				type Amount = Integer
				type DateOfTransaction = DateYMD

				//val colnamesTrade: List[NameOfCol] = List("Company", "FinancialInstrument", "Amount", "BuyOrSell", "Country")
				//List(Company.name, Instrument.FinancialInstrument.name, "Amount", Transaction.name, Country.name) //.names
				//val colnamesTrade: Seq[NameOfCol] = List(Company, Instrument.FinancialInstrument, "Amount", Transaction, Country).namesAll
				//val colnamesTrade: Seq[NameOfCol] = (Company, Instrument.FinancialInstrument, "Amount", Transaction, Country).toHList.namesEnumOnly.tupled.to[List]
				val colnamesTrade: Seq[NameOfCol] = (Company, Instrument.FinancialInstrument, "Amount", Transaction, "DateOfTransaction", World).tupleToStringList
				val coltypesTrade: List[DataType] = List(StringType, StringType, IntegerType, StringType, DateType, StringType)

				val tradeSchema: StructType = DFUtils.createSchema(colnamesTrade, coltypesTrade)
				// which cols are of stringtype
				val colnamesStrTrade: Seq[String] = DFUtils.getColnamesWithType[String](tradeSchema)

				val tradeSeq: Seq[(Company, Instrument, Amount, Transaction, DateOfTransaction, World)] = Seq(
					(Company.JPMorgan, Instrument.FinancialInstrument.Stock, 2, Transaction.Buy, date(1921, 12, 21), China),
					(Company.Google, Instrument.FinancialInstrument.Swap, 4, Transaction.Sell, date(1911, 11, 30), UnitedStates),
					(Company.GoldmanSachs, Instrument.FinancialInstrument.Equity, 3, Transaction.Sell, date(1901, 3, 11), UnitedStates),
					(Company.Disney, Instrument.FinancialInstrument.Bond, 10, Transaction.Buy, date(1907, 4, 11), Spain),
					(Company.Amazon, Instrument.FinancialInstrument.Commodity.PreciousMetal.Gold, 12, Transaction.Buy, date(1998, 2, 23), CostaRica),
					(Company.Amazon, Instrument.FinancialInstrument.Commodity.PreciousMetal.Silver, 12, Transaction.Buy, date(1938, 5, 23), CostaRica),
					(Company.Amazon, Instrument.FinancialInstrument.Commodity.Gemstone.Ruby, 12, Transaction.Buy, date(1918, 7, 6), CostaRica),
					(Company.Amazon, Instrument.FinancialInstrument.Commodity, 5, Transaction.Buy, date(1965, 3, 12), CostaRica),
					(Company.Google, Instrument.FinancialInstrument.Derivative, 10, Transaction.Sell, date(2001, 1, 17), Arabia),
					(Company.Ford, Instrument.FinancialInstrument.Derivative, 2, Transaction.Sell, date(2003, 9, 18), Argentina),
					(Company.Apple, Instrument.FinancialInstrument.Stock, 1, Transaction.Buy, date(1933, 5, 17), Canada),
					(Company.IBM, Instrument.FinancialInstrument.Commodity.Gemstone.Emerald, 110, Transaction.Buy, date(1984, 5, 9), Brazil),
					(Company.Samsung, Instrument.FinancialInstrument.Commodity.Gemstone.Sapphire, 2, Transaction.Sell, date(1990, 3, 10), China),
					(Company.Tesla, Instrument.FinancialInstrument.Commodity.CrudeOil, 5, Transaction.Sell, date(1977, 5, 15), Estonia),
					(Company.Deloitte, Instrument.FinancialInstrument.Cash, 9, Transaction.Sell, date(1933, 5, 17), Ireland),

					(Company.Apple, Instrument.FinancialInstrument.Derivative, 2, Transaction.Buy, date(1933, 5, 17), Australia),
					(Company.Samsung, Instrument.FinancialInstrument.Bond, 14, Transaction.Sell, date(2004, 4, 19), Africa),
					(Company.Google, Instrument.FinancialInstrument.Stock, 5, Transaction.Buy, date(2004, 4, 30), Africa),
					(Company.Microsoft, Instrument.FinancialInstrument.Commodity.Gemstone.Amethyst, 14, Transaction.Sell, date(2004, 1, 23), Africa.Kenya),
					(Company.IBM, Instrument.FinancialInstrument.Commodity.Gemstone.Ruby, 24, Transaction.Buy, date(2005, 7, 30),  Mauritius),
					(Company.GoldmanSachs, Instrument.FinancialInstrument.Share, 8, Transaction.Sell, date(2006, 8, 15), Africa.Tanzania),
					(Company.Microsoft, Instrument.FinancialInstrument.Future, 15, Transaction.Buy, date(2007, 8, 17), Peru),
					(Company.Facebook, Instrument.FinancialInstrument.Equity, 2, Transaction.Sell, date(2007, 12, 7), Africa.Uganda),
					(Company.Disney, Instrument.FinancialInstrument.Commodity.Gemstone.Diamond, 3, Transaction.Sell, date(1933, 12, 23), Russia),
					(Company.Walmart, Instrument.FinancialInstrument.Commodity.Gemstone.Tourmaline, 3, Transaction.Sell, date(1923, 10, 13), Russia),

					(Company.Ford, Instrument.FinancialInstrument.Option, 4, Transaction.Sell, date(1923, 10, 14), Spain),
					(Company.Ford, Instrument.FinancialInstrument.Commodity.Gemstone.Citrine, 8, Transaction.Buy, date(1911, 11, 19), Spain),
					(Company.Ford, Instrument.FinancialInstrument.Commodity.Gemstone.Moonstone, 5, Transaction.Sell, date(2008, 11, 3), Spain),
					(Company.Ford, Instrument.FinancialInstrument.Commodity.Gemstone.Pearl, 55, Transaction.Sell, date(2011, 8, 2), France),
					(Company.Ford, Instrument.FinancialInstrument.Commodity.Gemstone.Aquamarine, 8, Transaction.Sell, date(2020, 9, 1), France),
					(Company.Walmart, Instrument.FinancialInstrument.Swap, 57, Transaction.Sell, date(2017, 3, 5), France),
					(Company.Samsung, Instrument.FinancialInstrument.Future, 14, Transaction.Buy, date(2014, 5, 7), Italy),
					(Company.Facebook, Instrument.FinancialInstrument.Cash, 5, Transaction.Sell, date(2013, 6, 8), Greece),
					(Company.Facebook, Instrument.FinancialInstrument.Future, 95, Transaction.Buy, date(2011, 7, 11), Greece),
					(Company.Facebook, Instrument.FinancialInstrument.Commodity.Gemstone.Garnet, 7, Transaction.Sell, date(2010, 9, 12), Greece),
					(Company.Facebook, Instrument.FinancialInstrument.Commodity.Gemstone.Beryl, 71, Transaction.Buy, date(2009, 11, 1), Greece),
					(Company.Facebook, Instrument.FinancialInstrument.Commodity.Gemstone.Peridot, 98, Transaction.Sell, date(1904, 12, 2), Greece),
					(Company.Tesla, Instrument.FinancialInstrument.Equity, 11, Transaction.Sell, date(1901, 3, 17), Turkey),
					(Company.Deloitte, Instrument.FinancialInstrument.Option, 111, Transaction.Sell, date(1910, 2, 10), Arabia),
					(Company.JPMorgan, Instrument.FinancialInstrument.Commodity.PreciousMetal.Gold, 33, Transaction.Buy, date(1911, 3, 21), Pakistan),
					(Company.Disney, Instrument.FinancialInstrument.Option, 12, Transaction.Sell, date(1917, 3, 22), Scotland),
					(Company.Ford, Instrument.FinancialInstrument.Option, 56, Transaction.Buy, date(1916, 1, 25), Ireland),
					(Company.Walmart, Instrument.FinancialInstrument.Commodity.Gemstone.Onyx, 63, Transaction.Buy, date(1950, 10, 23), England),
					(Company.GoldmanSachs, Instrument.FinancialInstrument.Equity, 3, Transaction.Buy, date(1950, 11, 21), Germany),
					(Company.Walmart, Instrument.FinancialInstrument.Swap, 1, Transaction.Sell, date(1980, 1, 2), Romania),
					(Company.JPMorgan, Instrument.FinancialInstrument.Derivative, 87, Transaction.Sell, date(1980, 4, 2), Poland),
					(Company.Starbucks, Instrument.FinancialInstrument.Equity, 8, Transaction.Sell, date(1987, 5, 11), Serbia),
					(Company.Apple, Instrument.FinancialInstrument.Cash, 14, Transaction.Buy, date(1971, 3, 6), Slovakia),
					(Company.Disney, Instrument.FinancialInstrument.Derivative, 84, Transaction.Buy, date(2018, 5, 12), Slovenia),
					(Company.Tesla, Instrument.FinancialInstrument.Swap, 18, Transaction.Buy, date(2023, 3, 4), Hungary),
					(Company.Nike, Instrument.FinancialInstrument.Future, 34, Transaction.Buy, date(2023, 7, 15), Croatia),
					(Company.GoldmanSachs, Instrument.FinancialInstrument.Commodity.CrudeOil, 39, Transaction.Buy, date(2001, 7, 8), Estonia),
					(Company.JPMorgan, Instrument.FinancialInstrument.Commodity.PreciousMetal.Silver, 92, Transaction.Buy, date(1998, 9, 7), CostaRica),
					(Company.Starbucks, Instrument.FinancialInstrument.Equity, 10, Transaction.Buy, date(2007, 6, 19), Argentina),
					(Company.JPMorgan, Instrument.FinancialInstrument.Commodity.PreciousMetal.Platinum, 84, Transaction.Buy, date(2011, 2, 27), Iceland),
					(Company.GoldmanSachs, Instrument.FinancialInstrument.Future, 73, Transaction.Buy, date(2010, 8, 31), Greenland),
				)

				//val tradeStrSeq: Seq[(String, String, Amount, String, String)] = tradeSeq.map(_.tupleToHList.enumNames.hlistToTuple /*.tupleToSparkRow*/)
				val tradeStrSeq: Seq[(String, String, Amount, String, String, String)] = tradeSeq.map(tup => tup.toHList.enumNames.hlistToTuple)

				// NOTE: Another way: 	tradeSeq.map(_.to[List].nestedNames), but then cannot turn list to tuple easily again.

				// Finding another way so don't have to go from hlist -> tuple because that function can break if tuple size > 22
				// NOTE: Another way 3 --- but this doesn't work when the tuple has poly-type elements, only single-type elements which fall under the same parent enum.
				//tradeSeq.map(tup => tup.toHList.toList.nestedNames)
				// NOTE: BETTER
				tradeSeq.map(tup => tup.toHList.enumNestedNames) // ending type is this.Out so cannot call toList or tosparkrow on it...
				tradeSeq.map(tup => tup.toHList.enumNestedNames.toList) // yields List[Any]

				val tradeStrRDD: RDD[(String, String, Amount, String, String, String)] = sess.sparkContext.parallelize(tradeStrSeq)

				//val tradeRowSeq: Seq[Row] = tradeSeq.map(_.tupleToHList.enumNames.hlistToSparkRow)
				val tradeRowSeq: Seq[Row] = tradeSeq.map(tup => tup.toHList.enumNames.hlistToSparkRow)

				// WARNING: must put the rowrdd after tradedf or else initialization exception! (in spark AboutDataFrames test suite)
				//val tradeDf: DataFrame = tradeStrSeq.toDF(colnamesTrade: _*)
				// Need to declare this way so that the date col is of DateType
				// HELP left off here cannot infer the date column as datetype
				// val tradeDf: DataFrame = sess.createDataFrame(tradeRowSeq.asJava, schema = tradeSchema)

				// NOTE best create the data frame with string date col first, then cast it to date afterwards...
				val tradeDf: DataFrame = (tradeStrSeq.toDF(colnamesTrade:_*)
					.withColumn("DateOfTransaction", col("DateOfTransaction").cast(DateType))) // will rename the col in-place while casting

				val tradeRowRDD: RDD[Row] = tradeDf.rdd
			}

			// ---------------------------------------------------------------

			object AnimalDf {

				type Amount = Integer

				//val colnamesAnimal: List[NameOfCol] = List("Animal", "Amount", "Country", "Climate")
				// val colnamesAnimal: Seq[NameOfCol] = List(Animal, "Amount", Country, Climate).namesAll // WARNING: this returns typename for each elem which is "String" for the Amount and will return "Strin" when truncated since that function is for dealing with enums, must use this other way:


				val colnamesAnimal: Seq[NameOfCol] = (Animal, "Amount", World, ClimateZone, Biome).tupleToStringList
				val coltypesAnimal: List[DataType] = List(StringType, IntegerType, StringType, StringType, StringType)
				val animalSchema: StructType = DFUtils.createSchema(colnamesAnimal, coltypesAnimal)

				val animalSeq: Seq[(Animal, Amount, World, ClimateZone, Biome)] = Seq(

					(Animal.Cat.WildCat.Lion, 1, Arabia, ClimateZone.Desert, Biome.Desert),
					(Animal.Canine.WildCanine.Hyena, 6, Africa, ClimateZone.Desert, Biome.Grassland.Savannah),
					(Animal.Cat.WildCat.Cheetah, 11, Arabia, ClimateZone.Desert, Biome.Grassland.Savannah),
					(Animal.Bird.Vulture, 19, Africa, ClimateZone.Desert, Biome.Grassland.Savannah),
					(Animal.Camelid.Camel, 29, Arabia.UnitedArabEmirates, ClimateZone.Desert, Biome.Desert),
					(Animal.Camelid.Camel, 13, Arabia.Iraq, ClimateZone.Desert, Biome.Desert),
					(Animal.Canine.WildCanine.Fox.FennecFox, 918, Asia.Iran, ClimateZone.Desert, Biome.Grassland.Prairie),
					(Animal.Mongoose, 614, Arabia.Oman, ClimateZone.Desert, Biome.Desert),
					(Animal.Rodent.Mouse, 73, Arabia.Kuwait, ClimateZone.Desert, Biome.Desert),
					(Animal.Insect.Scorpion, 55, Arabia.Qatar, ClimateZone.Desert, Biome.Desert),
					(Animal.Insect.Beetle, 34, UnitedStates.Mexico, ClimateZone.Desert, Biome.Desert),
					(Animal.Reptile.Snake, 32, UnitedStates.Kansas, ClimateZone.Desert, Biome.Desert),
					(Animal.Canine.WildCanine.Fox.GreyFox, 30, UnitedStates.Colorado, ClimateZone.Desert, Biome.Desert),
					(Animal.Reptile.Lizard.Iguana, 30, UnitedStates.Colorado, ClimateZone.Desert, Biome.Desert),
					(Animal.Canine.WildCanine.Fox.FennecFox, 30, UnitedStates.Mexico, ClimateZone.Desert, Biome.Desert),
					(Animal.Reptile.Lizard.GilaMonster, 30, UnitedStates.Colorado, ClimateZone.Desert, Biome.Desert),

					(Animal.Equine.Zebra, 1, Africa, ClimateZone.Arid, Biome.Grassland.Savannah),
					(Animal.Bird.Ostrich, 12, Africa, ClimateZone.Arid, Biome.Grassland.Savannah),
					(Animal.Cat.WildCat.Jaguar, 19, Argentina, ClimateZone.Arid, Biome.Grassland.Prairie),
					(Animal.Canine.WildCanine.Wolf, 189, Argentina, ClimateZone.Arid, Biome.Grassland.Prairie),
					(Animal.Canine.WildCanine.Fox.FennecFox, 90, Brazil, ClimateZone.Arid, Biome.Grassland.Prairie),
					(Animal.Canine.WildCanine.Fox.GreyFox, 29, Arabia.SaudiaArabia, ClimateZone.Arid, Biome.Desert),
					(Animal.Insect.Scorpion, 13, Arabia.Iraq, ClimateZone.Arid, Biome.Desert),
					(Animal.Mongoose, 918, Asia.Iran, ClimateZone.Arid, Biome.Desert),
					(Animal.Mongoose, 614, Arabia.Oman, ClimateZone.Arid, Biome.Desert),
					(Animal.Insect.Scorpion, 73, Arabia.Kuwait, ClimateZone.Arid, Biome.Desert),
					(Animal.Camelid.Camel, 55, Arabia.Qatar, ClimateZone.Arid, Biome.Desert),

					(Animal.Elephant, 120, Africa, ClimateZone.Dry, Biome.Grassland.Savannah),
					(Animal.Camelid.Camel, 983, Chile, ClimateZone.Dry, Biome.Desert),
					(Animal.Cat.WildCat.Cougar, 73, Africa.Libya, ClimateZone.Dry, Biome.Grassland.Prairie),
					(Animal.Cat.WildCat.Lion, 42, Africa.Egypt, ClimateZone.Dry, Biome.Grassland.Prairie),
					(Animal.Rodent.Mouse, 42, Africa.Chad, ClimateZone.Dry, Biome.Grassland.Prairie),
					(Animal.Canine.WildCanine.Hyena, 42, Africa.Niger, ClimateZone.Dry, Biome.Grassland.Prairie),
					(Animal.Cat.WildCat.Caracal, 32, UnitedStates.Texas, ClimateZone.Dry, Biome.Grassland.Prairie),
					(Animal.Cat.WildCat.Cheetah, 91, UnitedStates.Utah, ClimateZone.Dry, Biome.Grassland.Prairie),
					(Animal.Canine.WildCanine.Fox.GreyFox, 87, UnitedStates.Nevada, ClimateZone.Dry, Biome.Grassland.Prairie),
					(Animal.Bird.Falcon, 46, Asia.Uzbekistan, ClimateZone.Dry, Biome.Desert),
					(Animal.Bird.Falcon, 47, Asia.Kazakhstan, ClimateZone.Dry, Biome.Desert),
					(Animal.Canine.WildCanine.Hyena, 2, Asia.Turkmenistan, ClimateZone.Dry, Biome.Desert),
					(Animal.Canine.WildCanine.Hyena, 1, Asia.Afghanistan, ClimateZone.Dry, Biome.Desert),
					(Animal.Bird.Hawk, 8, Asia.Pakistan, ClimateZone.Dry, Biome.Grassland.Prairie),
					(Animal.Bird.Ostrich, 5, Asia.Kyrgyzstan, ClimateZone.Dry, Biome.Grassland.Prairie),
					(Animal.Canine.WildCanine.Hyena, 8, Asia.Tajikistan, ClimateZone.Dry, Biome.Grassland.Prairie),
					(Animal.Cat.WildCat.SandCat, 23, Asia.Azerbaijan, ClimateZone.Dry, Biome.Desert),

					(Animal.Monkey.Ape.Gorilla, 11, China, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Tiger, 78, UnitedStates.Louisiana, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Jaguar, 100, China, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Amphibian.Frog.PoisonDartFrog, 56, India, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Bird.Macaw, 36, Indonesia, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Bird.Toucan, 62, Philippines, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Jaguar, 11, NorthKorea, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Bird.Toucan, 5, SouthKorea, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Amphibian.Frog.PoisonDartFrog, 13, Mongolia, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Bird.Macaw, 18, HongKong, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Amphibian.Frog.PoisonDartFrog, 34, Cambodia, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Amphibian.Frog.GoldenPoisonFrog, 34, Cambodia, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Amphibian.Frog.PoisonDartFrog, 34, Cambodia, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Amphibian.Frog.GoldenPoisonFrog, 34, Cambodia, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Tiger, 87, Vietnam, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Tiger, 33, Japan, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Reptile.Snake, 17, Myanmar, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Reptile.Snake, 17, Vietnam, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Reptile.Snake, 17, Thailand, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Reptile.Snake, 17, Vietnam, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Monkey.Ape.Orangutan, 55, Singapore, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Monkey.Ape.Gorilla, 405, Taiwan, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Monkey.Ape.Bonobo, 411, Thailand, ClimateZone.Humid, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Jaguar, 33, Bangladesh, ClimateZone.Humid, Biome.Forest.Rainforest),

					(Animal.Reptile.Crocodile, 100, Africa.Nigeria, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Insect.Termite, 120, Africa.Angola, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Monkey.Ape.Gorilla, 43, Africa.Mozambique, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Panther, 8, Africa.Tanzania, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Tiger, 9, Africa.Uganda, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Jaguar, 3, Africa.Sudan, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Insect.Butterfly, 120, China, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Bear.Panda, 120, China, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Leopard, 12, Brazil, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.SeaCreature.Jellyfish, 4, Brazil, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Insect.Dragonfly, 5, Brazil, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Bird.Flamingo, 9, Brazil, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Ocelot, 981, PuertoRico, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Insect.Termite, 111, Guatemala, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Leopard, 178, CostaRica, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Ocelot, 130, Panama, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Insect.Termite, 150, Colombia, ClimateZone.Tropical, Biome.Forest.Rainforest),

					(Animal.Reptile.Snake, 12, Brazil, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.SeaCreature.Dolphin, 10, Australia, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.SeaCreature.Jellyfish, 2, Argentina, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Monkey.Howler, 400, Paraguay, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Insect.Butterfly, 401, Uruguay, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Insect.Termite, 409, Venezuela, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Monkey.Capuchin, 180, Colombia, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Cat.WildCat.Leopard, 981, PuertoRico, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Insect.Butterfly, 111, Guatemala, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Insect.Spider, 178, CostaRica, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Rodent.Rat, 130, Panama, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Monkey.Lemur, 809, Honduras, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Insect.Dragonfly, 810, Ecuador, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Insect.Beetle, 300, Bolivia, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Monkey.Lemur, 410, Peru, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Equine.Horse.Mustang, 3, UnitedStates.Alabama, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Insect.Dragonfly, 14, UnitedStates.Mississippi, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Equine.Horse.DutchWarmbloodHorse, 45, UnitedStates.Georgia, ClimateZone.Tropical, Biome.Forest.Rainforest),
					(Animal.Equine.Horse.Clydesdale, 89, UnitedStates.SouthCarolina, ClimateZone.Tropical, Biome.Forest.Rainforest),

					// TODO left off here must add mediterranean animals / and pair up more kinds of biomes (forests - deciduous/shrubland) for mediterranean climate.
					(Animal.Insect.Bee, 120, Italy, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Bird.Pelican, 12, Spain, ClimateZone.Mediterranean, Biome.Marine.Saltwater.Seashore),
					(Animal.SeaCreature.Anemone, 4, CostaRica, ClimateZone.Mediterranean, Biome.Marine.Saltwater.Ocean),
					(Animal.SeaCreature.Shrimp, 3, France, ClimateZone.Mediterranean, Biome.Marine.Saltwater.Ocean),
					(Animal.Bird.Albatross, 120, Italy.Genoa, ClimateZone.Mediterranean,Biome.Marine.Saltwater.Ocean),
					(Animal.SeaCreature.Crab, 130, Italy, ClimateZone.Mediterranean, Biome.Marine.Saltwater.Ocean),
					(Animal.SeaCreature.Coral, 130, Italy, ClimateZone.Mediterranean, Biome.Marine.Saltwater.Ocean),
					(Animal.SeaCreature.Dolphin, 130, Italy.Milan, ClimateZone.Mediterranean, Biome.Marine.Saltwater.Ocean),
					(Animal.SeaCreature.Shark, 130, Italy.Milan, ClimateZone.Mediterranean, Biome.Marine.Saltwater.Ocean),
					(Animal.SeaCreature.Octopus, 130, Italy.Milan, ClimateZone.Mediterranean, Biome.Marine.Saltwater.Ocean),
					(Animal.SeaCreature.Shrimp, 140, Italy.Venice, ClimateZone.Mediterranean, Biome.Marine.Freshwater.Lake),
					(Animal.SeaCreature.Clam, 120, France.Nice, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Cat.WildCat.IberianLynx, 11, Spain, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Monkey.Macaque, 11, Spain, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Deer.RoeDeer, 11, Spain, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Bird.Goldfinch, 11, Spain, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Bird.Falcon, 11, Spain, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Bird.Eagle.GoldenEagle, 11, Spain, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Deer.RedDeer, 11, France.Versailles, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Deer.RedDeer, 11, France.Avignon, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Bird.Sparrow, 11, France.Paris, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Bird.Robin, 11, France.Nice, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Bird.Canary, 11, France.Lyon, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Bear.BrownBear, 11, France.Besancon, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.WeaselMustelid.Weasel, 11, France.Bordeaux, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Bird.Goldfinch, 11, France.Grenoble, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.WeaselMustelid.Otter, 11, France.Montpellier, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.WeaselMustelid.Ferret, 11, France.SaintTropez, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.WeaselMustelid.Marten, 11, Italy.Milan, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Deer.RedDeer, 11, Italy, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Cat.WildCat.Lynx, 11, Italy, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Rodent.Mouse, 11, Italy.Genoa, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),
					(Animal.Rodent.Squirrel.Marmot, 11, Italy, ClimateZone.Mediterranean, Biome.Grassland.Shrubland),

					(Animal.Cat.WildCat.Lion, 12, Africa.Libya, ClimateZone.Tundra, Biome.Grassland.Savannah),
					(Animal.Giraffe, 1, Africa.Algeria, ClimateZone.Tundra, Biome.Grassland.Savannah),
					(Animal.Canine.WildCanine.Hyena, 3, Africa, ClimateZone.Tundra, Biome.Grassland.Savannah),
					(Animal.Canine.WildCanine.Coyote, 3, Africa, ClimateZone.Tundra, Biome.Grassland.Savannah),
					(Animal.Canine.WildCanine.Jackal, 3, Africa, ClimateZone.Tundra, Biome.Grassland.Savannah),
					(Animal.Canine.WildCanine.Fox.FennecFox, 3, Africa, ClimateZone.Tundra, Biome.Grassland.Savannah),
					(Animal.Hippo, 10, Africa, ClimateZone.Tundra, Biome.Grassland.Savannah),
					(Animal.Bear.Koala, 120, Australia, ClimateZone.Tundra, Biome.Grassland.Prairie),
					(Animal.Cat.WildCat.Lynx, 12, Russia.Khabarovsk, ClimateZone.Tundra, Biome.Grassland.Steppes),
					(Animal.Bird.Hawk, 12, Russia.Murmansk, ClimateZone.Tundra, Biome.Grassland.Steppes),
					(Animal.Bird.Eagle.GoldenEagle, 12, UnitedStates, ClimateZone.Tundra, Biome.Grassland.Prairie),
					(Animal.Bison, 14, Canada.Yukon, ClimateZone.Tundra, Biome.Grassland.Prairie),
					(Animal.Deer.Elk, 23, Canada.Nunavut, ClimateZone.Tundra, Biome.Grassland.Prairie),
					(Animal.Deer.RedDeer, 3, Canada.BritishColumbia, ClimateZone.Tundra, Biome.Grassland.Prairie),
					(Animal.Deer.Caribou, 4, Russia.Sochi, ClimateZone.Tundra, Biome.Grassland.Steppes),
					(Animal.Bird.Falcon, 5, Russia.Volgograd, ClimateZone.Tundra, Biome.Grassland.Steppes),
					(Animal.Bird.Eagle.GoldenEagle, 8, Russia.Tobolsk, ClimateZone.Tundra, Biome.Grassland.Steppes),
					(Animal.Bird.Crow, 90, Russia.Kalingrad, ClimateZone.Tundra, Biome.Grassland.Steppes),
					(Animal.Canine.WildCanine.Wolf, 10, Russia.Ufa, ClimateZone.Tundra, Biome.Grassland.Steppes),
					(Animal.Bird.Sparrow, 11, Russia, ClimateZone.Tundra, Biome.Grassland.Steppes),
					(Animal.Cat.WildCat.Cougar, 11, Russia, ClimateZone.Tundra, Biome.Grassland.Steppes),
					(Animal.Canine.WildCanine.Coyote, 11, Russia, ClimateZone.Tundra, Biome.Grassland.Steppes),

					(Animal.SeaCreature.Oyster, 120, UnitedStates, ClimateZone.Continental, Biome.Marine.Freshwater.Wetland),
					(Animal.Rodent.Beaver, 63, Canada.Manitoba, ClimateZone.Continental, Biome.Marine.Freshwater.Wetland),
					(Animal.Rodent.Squirrel.Chipmunk, 90, Canada.Alberta, ClimateZone.Continental, Biome.Forest.ConiferousForest),
					(Animal.Rodent.Mouse, 73, Canada.BritishColumbia, ClimateZone.Continental, Biome.Grassland.Prairie),
					(Animal.Rodent.Beaver, 73, Canada.BritishColumbia, ClimateZone.Continental, Biome.Marine.Freshwater.Stream),
					(Animal.Bird.Duck, 73, Canada.BritishColumbia, ClimateZone.Continental, Biome.Forest.ConiferousForest),
					(Animal.Bird.Eagle.BaldEagle, 73, Canada.BritishColumbia, ClimateZone.Continental, Biome.Grassland.Prairie),
					(Animal.Bird.Eagle.BaldEagle, 12, UnitedStates.Michigan, ClimateZone.Continental, Biome.Grassland.Prairie),
					(Animal.Bird.Eagle.GoldenEagle, 18, UnitedStates.Pennsylvania, ClimateZone.Continental, Biome.Forest.ConiferousForest),
					(Animal.Amphibian.Frog.TrueFrog, 77, UnitedStates.Maine, ClimateZone.Continental, Biome.Marine.Freshwater.Pond),
					(Animal.Amphibian.Frog.TrueFrog, 3, Canada.Ontario, ClimateZone.Continental, Biome.Marine.Freshwater.Pond),
					(Animal.Rodent.Squirrel.RedSquirrel, 4, Canada.Quebec, ClimateZone.Continental, Biome.Forest.ConiferousForest),
					(Animal.Bear.BrownBear, 78, Poland, ClimateZone.Continental, Biome.Marine.Freshwater.River),
					(Animal.Deer.RedDeer, 90, Hungary, ClimateZone.Continental, Biome.Grassland.Prairie),
					(Animal.Bird.Falcon, 8, Serbia, ClimateZone.Continental, Biome.Marine.Freshwater.Wetland),
					(Animal.Canine.WildCanine.Wolf, 8, Slovakia, ClimateZone.Continental, Biome.Forest.ConiferousForest),
					(Animal.Canine.WildCanine.Fox.GreyFox, 8, Czechia, ClimateZone.Continental, Biome.Forest.ConiferousForest),
					(Animal.Bird.Raven, 8, Croatia, ClimateZone.Continental, Biome.Forest.DeciduousForest),
					(Animal.Bird.Sparrow, 8, Romania, ClimateZone.Continental, Biome.Forest.ConiferousForest),
					(Animal.Bird.Duck, 8, Moldova, ClimateZone.Continental, Biome.Forest.DeciduousForest),
					(Animal.Bird.Eagle.GoldenEagle, 8, Lithuania, ClimateZone.Continental, Biome.Forest.ConiferousForest),
					(Animal.WeaselMustelid.Mink, 8, Latvia, ClimateZone.Continental, Biome.Marine.Freshwater.Wetland),
					(Animal.Bird.Starling, 8, Liechtenstein, ClimateZone.Continental, Biome.Grassland.Prairie),
					(Animal.Bird.Owl, 8, Luxembourg, ClimateZone.Continental, Biome.Grassland.Prairie),
					(Animal.Bird.Nightingale, 8, Switzerland, ClimateZone.Continental, Biome.Forest.DeciduousForest),
					(Animal.Cat.WildCat.Lynx, 8, Ukraine, ClimateZone.Continental, Biome.Forest.ConiferousForest),
					(Animal.Cat.WildCat.Bobcat, 8, Hungary, ClimateZone.Continental, Biome.Forest.ConiferousForest),
					(Animal.WeaselMustelid.Marten, 8, Bulgaria, ClimateZone.Continental, Biome.Forest.ConiferousForest),
					(Animal.Bird.Swallow, 8, Latvia, ClimateZone.Continental, Biome.Grassland.Prairie),
					(Animal.Bird.Swan, 8, Serbia, ClimateZone.Continental, Biome.Marine.Freshwater.Lake),
					(Animal.Rodent.Squirrel.RedSquirrel, 8, Slovakia, ClimateZone.Continental, Biome.Forest.DeciduousForest),
					(Animal.Bird.Woodpecker, 8, Czechia, ClimateZone.Continental, Biome.Forest.DeciduousForest),

					(Animal.Camelid.Llama, 4, Chile, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Rodent.Chinchilla, 3, Colombia, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Rodent.Chinchilla, 13, Honduras, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					//(Animal.Monkey.Bushbaby, 32, Ecuador, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Camelid.Alpaca, 23, Bolivia, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Camelid.Alpaca, 30, Peru, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Bison, 93, UnitedStates.Kansas, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Canine.WildCanine.Coyote, 45, UnitedStates.Colorado, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Canine.WildCanine.Fox.RedFox, 8, UnitedStates.Texas, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Canine.WildCanine.PrairieDog, 7, UnitedStates.Utah, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.WeaselMustelid.Ferret, 58, UnitedStates.Nevada, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Bear.GrizzlyBear, 89, Canada.BritishColumbia, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Bear.GrizzlyBear, 1, Canada.Alberta, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Bear.GrizzlyBear, 2, Canada.Yukon, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Bear.GrizzlyBear, 4, Canada.Nunavut, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Cat.WildCat.Leopard, 5, China, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Bear.Panda, 5, China, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Cat.WildCat.Tiger, 5, China, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),
					(Animal.Canine.WildCanine.Fox.TibetanFox, 5, China, ClimateZone.MountainHighland, Biome.Forest.ConiferousForest),

					(Animal.Bear.GrizzlyBear, 14, Canada, ClimateZone.Temperate, Biome.Forest.DeciduousForest),
					(Animal.Bear.BrownBear, 11, Russia.Omsk, ClimateZone.Temperate, Biome.Forest.ConiferousForest),
					(Animal.Canine.WildCanine.Fox.RedFox, 3, Russia.SaintPetersburg, ClimateZone.Temperate, Biome.Forest.ConiferousForest),
					(Animal.Bird.Eagle.GoldenEagle, 4, Russia.Kazan, ClimateZone.Temperate, Biome.Forest.ConiferousForest),
					(Animal.WeaselMustelid.Weasel, 21, Canada, ClimateZone.Temperate, Biome.Forest.DeciduousForest),
					(Animal.Deer.Reindeer, 41, Canada, ClimateZone.Temperate, Biome.Forest.DeciduousForest),
					(Animal.Rodent.Squirrel, 44, Canada, ClimateZone.Temperate, Biome.Forest.DeciduousForest),
					(Animal.Canine.WildCanine.Fox, 43, Russia.Smolensk, ClimateZone.Temperate, Biome.Forest.ConiferousForest),
					(Animal.Canine.WildCanine.Fox, 42, Canada.Quebec, ClimateZone.Temperate, Biome.Forest.ConiferousForest),
					(Animal.Rabbit, 120, UnitedStates, ClimateZone.Temperate, Biome.Forest.ConiferousForest),
					(Animal.Cat.DomesticCat.SiameseCat, 8, China, ClimateZone.Temperate, Biome.Forest.ConiferousForest),
					(Animal.Cat.DomesticCat.ShorthairedCat, 12, UnitedStates, ClimateZone.Temperate, Biome.Forest.ConiferousForest),
					(Animal.Bird.Sparrow, 12, France, ClimateZone.Temperate, Biome.Forest.DeciduousForest),
					(Animal.Bird.Sparrow, 12, Ireland, ClimateZone.Temperate, Biome.Forest.DeciduousForest),
					(Animal.Bird.Sparrow, 12, Scotland, ClimateZone.Temperate, Biome.Forest.DeciduousForest),
					(Animal.Bird.Woodpecker, 115, Spain, ClimateZone.Temperate, Biome.Forest.DeciduousForest),
					(Animal.Cat.WildCat.Bobcat, 16, Spain, ClimateZone.Temperate, Biome.Forest.DeciduousForest),
					(Animal.Bear.BlackBear, 113, France, ClimateZone.Temperate, Biome.Forest.DeciduousForest),
					(Animal.Bear.BlackBear, 67, Italy, ClimateZone.Temperate, Biome.Forest.DeciduousForest),
					(Animal.Rodent.Squirrel.Chipmunk, 87, Italy.ApenninePeninsula.PapalStates.Rome, ClimateZone.Temperate, Biome.Forest.DeciduousForest),
					(Animal.Bird.Eagle.BaldEagle, 10, Greece, ClimateZone.Temperate, Biome.Forest.DeciduousForest),
					(Animal.Rodent.Mouse, 4, Norway, ClimateZone.Temperate, Biome.Forest.ConiferousForest),

					(Animal.Bear, 120, Russia.Sarov, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Deer.RedDeer, 4, Russia.Alexandrov, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Bear.GrizzlyBear, 190, Canada.Yukon, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Canine.WildCanine.Fox.ArcticFox, 120, Russia.Derbent, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Canine.WildCanine.Fox.GreyFox, 160, Canada.NorthwestTerritories, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Cat.WildCat.MountainLion, 1, Russia.Yekaterinburg, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Bird.Sparrow, 2, Canada.NovaScotia, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Cat.WildCat.Lynx, 5, Russia.Moscow, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Deer.Reindeer, 4, Russia.NizhnyNovgorod, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Deer.Caribou, 5, Greenland, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Deer.Elk, 8, Iceland, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Bird.Penguin, 3, Antarctica, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Bear.PolarBear, 9, Antarctica, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Bird.Penguin, 312, Antarctica, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Bird.Penguin, 678, UnitedStates.Alaska, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.WeaselMustelid.Marten, 13, Sweden, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.WeaselMustelid.Weasel, 12, Norway, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
					(Animal.Deer.Reindeer, 3, Finland, ClimateZone.Arctic, Biome.Forest.TaigaBorealForest),
				)


				val animalStrSeq: Seq[(EnumString, Amount, EnumString, EnumString, EnumString)] = animalSeq.map(tup => tup.toHList.enumNames.hlistToTuple)
					//animalSeq.map(_.tupleToHList.enumNames.hlistToTuple)

				val animalStrRDD: RDD[(EnumString, Amount, EnumString, EnumString, EnumString)] = sess.sparkContext.parallelize(animalStrSeq)

				/// val animalRowSeq: Seq[Row] = animalSeq.map(tup => tup.tupleToHList.enumNames.hlistToSparkRow)
				val animalRowSeq: Seq[Row] = animalSeq.map(tup => tup.toHList.enumNames.hlistToSparkRow)


				// WARNING: must put the rowrdd after tradedf or else initialization exception! (in spark AboutDataFrames test suite)
				val animalDf: DataFrame = animalStrSeq.toDF(colnamesAnimal: _*)
				val animalRowRDD: RDD[Row] = animalDf.rdd
			}

			// ---------------------------------------------------------------

			object ArtistDf {



				type YearPublished = Int
				type TitleOfWork = String
				type PlaceOfBirth = World
				type PlaceOfDeath = World

				// TODO AddColSpecs will add a column (array-col) that aggregates the results of each column .e.g if person is both sculptor,painter,singer, the final col contains [Singer, Painter, Sculptor] for example.
				// TODO 2 - add column, if Musician, then add his instrument(s) as array-list in the column.


				import Human._
				import ArtPeriod._
				import Artist._
				import Scientist._ ; import NaturalScientist._ ; import Mathematician._;  import Engineer._
				import Craft._;
				import Art._; import Literature._; import PublicationMedium._;  import Genre._
				import Science._; import NaturalScience._ ; import Mathematics._ ; import Engineering._ ;

				// human, art, period, title, yearpub, birthplace, deathplace, ...

				// TODO left off here tuples don't have so many elements
				//val colnamesArtist: Seq[NameOfCol] = Tuple8(Human, Craft, Genre, ArtPeriod, "TitleOfWork", "YearPublished", "PlaceOfBirth", "PlaceOfDeath", Painter, Sculptor, Musician, Dancer, Singer, Writer, Architect, Actor).tupleToNameList

				//val coltypesArtist: Seq[DataType] = List(StringType, StringType, StringType, StringType, StringType, IntegerType, StringType, StringType, StringType,  StringType, StringType, StringType, StringType, StringType, StringType, StringType)

				val coltypesMain: Seq[DataType] = Seq(StringType, StringType, StringType, StringType, StringType, IntegerType, StringType, StringType)
				val colnamesMain: Seq[NameOfCol] = Seq(Human, Craft, Genre, ArtPeriod, "TitleOfWork", "YearPublished", "PlaceOfBirth", "PlaceOfDeath").typeNames

				val colnamesSci: Seq[NameOfCol] =
					(Mathematician,  Engineer, Architect,
						Botanist, Chemist, Geologist, Doctor, Physicist).tupleToStringList
				val coltypesSci: Seq[DataType] = Seq(StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType)

				val colnamesArt: Seq[NameOfCol] =
					(Painter, Sculptor,
						Musician, Dancer, Singer, Actor, Designer, Inventor, Producer, Director,
						Writer, Linguist).tupleToStringList
				val coltypesArt: Seq[DataType] = Seq(StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType)

				//val artistSchema: StructType = DFUtils.createSchema(colnamesArtist, coltypesArtist)
				val schemaCraftMain: StructType = DFUtils.createSchema(colnamesMain, coltypesMain)
				val schemaMath: StructType = DFUtils.createSchema(colnamesSci, coltypesSci)
				val schemaArt: StructType = DFUtils.createSchema(colnamesArt, coltypesArt)



				// Tuple3 -
				// 	- part1 holds the human ... year .. locations
				// 	- part 2 holds the artist craft
				// 	- part 3 holds the maths craft
				// using this structure because cannot have Seq of tuple with length > 22 so decided to partition
				type HumanInfo = Tuple8[Human, Craft, Genre, ArtPeriod, TitleOfWork, YearPublished, PlaceOfBirth, PlaceOfDeath]
				type ScienceCrafts = Tuple8[Mathematician, Engineer, Architect, Botanist, Chemist, Geologist, Doctor, Physicist]
				type ArtCrafts = Tuple12[Painter, Sculptor, Musician, Dancer, Singer, Actor, Designer, Inventor, Producer, Director, Writer, Linguist]


				val artistTupLists: Seq[Tuple3[HumanInfo, ScienceCrafts, ArtCrafts]] = Seq(

					// SOURCE: https://en.wikipedia.org/wiki/Charlotte_Bront%C3%AB#Publications
					Tuple3(
						Tuple8(Human.CharlotteBronte, Literature.PublicationMedium.Novel, Literature.Genre.Fiction, Romanticism, "Jane Eyre", 1847, England.WestYorkshire.Bradford.Thornton, England.WestYorkshire.Haworth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.CharlotteBronte, Literature.PublicationMedium.Novel, Literature.Genre.Fiction, Romanticism, "Shirley", 1849, England.WestYorkshire.Bradford.Thornton, England.WestYorkshire.Haworth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.CharlotteBronte, Literature.PublicationMedium.Novel, Literature.Genre.Fiction, Romanticism, "Villette", 1853, England.WestYorkshire.Bradford.Thornton, England.WestYorkshire.Haworth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.CharlotteBronte, Literature.PublicationMedium.Novel, Literature.Genre.Fiction, Romanticism, "The Professor", 1857, England.WestYorkshire.Bradford.Thornton, England.WestYorkshire.Haworth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.CharlotteBronte, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Poems by Currer, Ellis, and Acton Bell", 1846, England.WestYorkshire.Bradford.Thornton, England.WestYorkshire.Haworth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),

					// ----------
					// Literature.PublicationMedium.Poetry, Literature.PublicationMedium.Prose, Literature.Genre.Criticism
					Tuple3(
						Tuple8(Human.SamuelTaylorColeridge, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Frost at Midnight", 1798, England.DevonCounty.DevonDistrict.OtteryStMary, England.Middlesex.Highgate),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.SamuelTaylorColeridge, Ballad, Literature.Genre.Fiction, Romanticism, "The Rime of the Ancient Mariner", 1798, England.DevonCounty.DevonDistrict.OtteryStMary, England.Middlesex.Highgate),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.SamuelTaylorColeridge, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Kubla Khan (Xanadu)", 1816, England.DevonCounty.DevonDistrict.OtteryStMary, England.Middlesex.Highgate),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.SamuelTaylorColeridge, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Dejection: An Ode", 1802, England.DevonCounty.DevonDistrict.OtteryStMary, England.Middlesex.Highgate),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.SamuelTaylorColeridge, Literature.PublicationMedium.Autobiography, Literature.Genre.Nonfiction, Romanticism, "Biographia Literaria", 1817, England.DevonCounty.DevonDistrict.OtteryStMary, England.Middlesex.Highgate),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.SamuelTaylorColeridge, Literature.PublicationMedium.Prose, Literature.Genre.Fiction, Romanticism, "The Friend", 1969, England.DevonCounty.DevonDistrict.OtteryStMary, England.Middlesex.Highgate),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.SamuelTaylorColeridge, Literature.PublicationMedium.Prose, Literature.Genre.Criticism, Romanticism, "On the Constitution of the Church and State", 1976, England.DevonCounty.DevonDistrict.OtteryStMary, England.Middlesex.Highgate),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.SamuelTaylorColeridge, Literature.PublicationMedium.Essay, Literature.Genre.Nonfiction, Romanticism, "Literature.PublicationMedium.Essays on His Times in The Morning Post and The Courier", 1978, England.DevonCounty.DevonDistrict.OtteryStMary, England.Middlesex.Highgate),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.SamuelTaylorColeridge, Literature.PublicationMedium.Prose, Literature.Genre.Nonfiction, Romanticism, "The Watchman", 1796, England.DevonCounty.DevonDistrict.OtteryStMary, England.Middlesex.Highgate),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),

										// TEMPORARY dickens
					Tuple3(
						Tuple8(Human.CharlesDickens, Literature.PublicationMedium.Novel, Literature.Genre.Fiction, Romanticism, "", 1, England, England),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					// -------
					// SOURCE = https://en.wikipedia.org/wiki/List_of_Emily_Dickinson_poems
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Hope is the Thing With Feathers", 1896, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Morbidity, Romanticism, "His Heart Was Darker than the Starless Night", 1945, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Four Trees - upon a solitary Acre", 1945, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "From Us She wandered now a Year", 1896, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Further in Summer Than the Birds", 1891, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Gathered into the Earth", 1945, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Do People moulder equally", 1945, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Bliss is the Plaything of the child", 1945, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Morbidity, Romanticism, "Because I could not stop for Death", 1890, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Satire, Romanticism, "Faith is a fine invention", 1890, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "As imperceptibly as Grief", 1891, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "As if the Sea should part", 1929, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Above Oblivion's Tide there is a Pier", 1945, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "A Sparrow took a Slice of Twig", 1945, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "A Mien to move a Queen", 1935, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),

					// -------
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.Novel, Literature.Genre.Morality, Romanticism, "The Birth-Mark", 1843, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.Prose, Literature.Genre.Morality, Romanticism, "Fanshawe", 1828, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.Prose, Literature.Genre.Fiction, Romanticism, "The Scarlet Literature.PublicationMedium.Letter, A Romance", 1850, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.Novel, Literature.Genre.Fiction, Romanticism, "The House of the Seven Gables, A Romance", 1851, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.Novel, Literature.Genre.Fiction, Romanticism, "The Blithedale Romance", 1852, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.Novel, Literature.Genre.Fiction, Romanticism, "Doctor Grimshawe's Secret: A Romance", 1882, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.Novel, Literature.Genre.Fiction, Romanticism, "The Marble Faun", 1860, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.Novel, Literature.Genre.Fiction, Romanticism, "Legends Of the Province House", 1839, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.Novel, Literature.Genre.Fiction, Romanticism, "Tanglewood Tales", 1853, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.ShortStory, Literature.Genre.Fiction, Romanticism, "The Hollow of the Three Hills", 1830, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.ShortStory, Literature.Genre.Fiction, Romanticism, "Young Goodman Brown", 1835, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.ShortStory, Literature.Genre.Fiction, Romanticism, "Wakefield", 1835, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.ShortStory, Literature.Genre.Fiction, Romanticism, "Rappaccini's Daughter", 1844, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.ShortStory, Literature.Genre.Fiction, Romanticism, "The Great Stone Face", 1850, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.Genre.Nonfiction, Literature.Genre.Nonfiction, Romanticism, "Life of Franklin Pierce", 1852, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.Genre.Nonfiction, Literature.Genre.Fiction, Romanticism, "Our Old Home", 1863, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.NathanielHawthorne, Literature.PublicationMedium.ShortStory, Literature.Genre.Fiction, Romanticism, "The Celestial Railroad", 1843, UnitedStates.Massachusetts.EssexCounty.Salem, UnitedStates.NewHampshire.GraftonCounty.Plymouth),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),

					// -------
					Tuple3(
						Tuple8(Human.RalphWaldoEmerson, Literature.PublicationMedium.Essay, Literature.Genre.Nonfiction, Romanticism, "Nature", 1836, UnitedStates.Massachusetts.Boston, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.RalphWaldoEmerson, Literature.PublicationMedium.Essay, Literature.Genre.Nonfiction, Romanticism, "Compensation", 1841, UnitedStates.Massachusetts.Boston, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.RalphWaldoEmerson, Literature.PublicationMedium.Essay, Literature.Genre.Nonfiction, Romanticism, "Representative Men", 1850, UnitedStates.Massachusetts.Boston, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.RalphWaldoEmerson, Literature.PublicationMedium.Essay, Literature.Genre.Criticism, Romanticism, "Society and Solitude", 1870, UnitedStates.Massachusetts.Boston, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.RalphWaldoEmerson, Literature.PublicationMedium.Essay, Literature.Genre.Nonfiction, Romanticism, "Self-Reliance", 1841, UnitedStates.Massachusetts.Boston, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.RalphWaldoEmerson, Literature.PublicationMedium.Essay, Literature.Genre.Nonfiction, Romanticism, "Saadi", 1864, UnitedStates.Massachusetts.Boston, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.RalphWaldoEmerson, Literature.PublicationMedium.Poetry, Literature.Genre.Nonfiction, Romanticism, "Concord Hymn", 1837, UnitedStates.Massachusetts.Boston, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.RalphWaldoEmerson, Literature.PublicationMedium.Poetry, Literature.Genre.Nonfiction, Romanticism, "The Rhodora", 1834, UnitedStates.Massachusetts.Boston, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.RalphWaldoEmerson, Literature.PublicationMedium.Poetry, Literature.Genre.Religion, Romanticism, "Uriel", 1847, UnitedStates.Massachusetts.Boston, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.RalphWaldoEmerson, Literature.PublicationMedium.Letter, Literature.Genre.Nonfiction, Romanticism, "Letter to Martin van Buren", 1838, UnitedStates.Massachusetts.Boston, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),

					// -------
					Tuple3(
						Tuple8(Human.EdgarAllanPoe, Literature.PublicationMedium.ShortStory, Literature.Genre.Horror, DarkRomanticism, "The Masque of the Red Death", 1842, UnitedStates.Massachusetts.Boston, UnitedStates.Maryland.Baltimore),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EdgarAllanPoe, Literature.PublicationMedium.Poetry, Literature.Genre.Horror, Gothic, "The Raven", 1845, UnitedStates.Massachusetts.Boston, UnitedStates.Maryland.Baltimore),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EdgarAllanPoe, Literature.PublicationMedium.ShortStory, Literature.Genre.Horror, Gothic, "The Fall of the House of Usher", 1839, UnitedStates.Massachusetts.Boston, UnitedStates.Maryland.Baltimore),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EdgarAllanPoe, Literature.PublicationMedium.ShortStory, Literature.Genre.Horror, Gothic, "Ligeia", 1838, UnitedStates.Massachusetts.Boston, UnitedStates.Maryland.Baltimore),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EdgarAllanPoe, Literature.PublicationMedium.Poetry, Literature.Genre.Morbidity, Gothic, "Annabel Lee", 1843, UnitedStates.Massachusetts.Boston, UnitedStates.Maryland.Baltimore),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EdgarAllanPoe, Literature.PublicationMedium.ShortStory, Literature.Genre.Horror, Gothic, "The Black Cat", 1843, UnitedStates.Massachusetts.Boston, UnitedStates.Maryland.Baltimore),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EdgarAllanPoe, Literature.PublicationMedium.ShortStory, Literature.Genre.Horror, Gothic, "The Cask of Amontillado", 1846, UnitedStates.Massachusetts.Boston, UnitedStates.Maryland.Baltimore),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EdgarAllanPoe, Literature.PublicationMedium.ShortStory, Literature.Genre.Horror, Gothic, "The Oval Portrait", 1842, UnitedStates.Massachusetts.Boston, UnitedStates.Maryland.Baltimore),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EdgarAllanPoe, Literature.PublicationMedium.Play, Literature.Genre.HistoricalFiction, DarkRomanticism, "Politician", 1835, UnitedStates.Massachusetts.Boston, UnitedStates.Maryland.Baltimore),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.EdgarAllanPoe, Literature.PublicationMedium.Poetry, Literature.Genre.HistoricalFiction, DarkRomanticism, "Tamerlane", 1838, UnitedStates.Massachusetts.Boston, UnitedStates.Maryland.Baltimore),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),

					// -------
					Tuple3(
						// SOURCE: https://en.wikipedia.org/wiki/William_Wordsworth#Major_works
						Tuple8(Human.WilliamWordsworth, Ballad, Literature.Genre.Fiction, Romanticism, "Simon Lee", 1798, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.WilliamWordsworth, Ballad, Literature.Genre.Fiction, Romanticism, "We are Seven", 1798, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.WilliamWordsworth, Ballad, Literature.Genre.Fiction, Romanticism, "Lines Written in Early Spring", 1798, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.WilliamWordsworth, Ballad, Literature.Genre.Fiction, Romanticism, "She Dwelt among the Untrodden Ways", 1800, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.WilliamWordsworth, Ballad, Literature.Genre.Fiction, Romanticism, "A Slumber Did my Spirit Seal", 1798, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.WilliamWordsworth, Ballad, Literature.Genre.Fiction, Romanticism, "Lucy Gray", 1798, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.WilliamWordsworth, Ballad, Literature.Genre.Fiction, Romanticism, "The Two April Mornings", 1798, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.WilliamWordsworth, Ballad, Literature.Genre.Fiction, Romanticism, "Nutting", 1798, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.WilliamWordsworth, Ballad, Literature.Genre.Fiction, Romanticism, "The Kitten At Play", 1798, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.WilliamWordsworth, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "I Wandered Lonely as a Cloud", 1807, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.WilliamWordsworth, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "London", 1802, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.WilliamWordsworth, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "My Heart Leaps Up", 1807, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.WilliamWordsworth, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "The Solitary Reaper", 1802, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.WilliamWordsworth, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "The White Doe of Rylstone", 1815, England.CumberlandCounty.Cockermouth, England.Westmorland.Rydal),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),

					// -------
					Tuple3(
						// SOURCE: https://www.cliffsnotes.com/literature/t/thoreau-emerson-and-transcendentalism/henry-david-thoreau/selected-chronology-of-thoreaus-writings
						Tuple8(Human.HenryDavidThoreau, Literature.PublicationMedium.Essay, Literature.Genre.Fiction, Romanticism, "The Seasons", 1827, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.HenryDavidThoreau, Literature.PublicationMedium.Essay, Literature.Genre.HistoricalFiction, Romanticism, "Aulus Perseus Flaccus", 1840, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.HenryDavidThoreau, Literature.PublicationMedium.Essay, Literature.Genre.History, Romanticism, "Natural History of Massachusetts", 1842, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.HenryDavidThoreau, Literature.PublicationMedium.Essay, Literature.Genre.Criticism, Romanticism, "Herald of Freedom", 1844, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.HenryDavidThoreau, Literature.PublicationMedium.Poetry, Literature.Genre.Nonfiction, Romanticism, "Sympathy", 1840, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.HenryDavidThoreau, Literature.PublicationMedium.Poetry, Literature.Genre.Nonfiction, Romanticism, "Poems of Nature", 1895, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.HenryDavidThoreau, Literature.PublicationMedium.Poetry, Literature.Genre.Nonfiction, Romanticism, "To the Maiden in the East", 1842, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.HenryDavidThoreau, Literature.PublicationMedium.Prose, Literature.Genre.Nonfiction, Romanticism, "Walden", 1906, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.HenryDavidThoreau, Literature.PublicationMedium.Prose, Literature.Genre.Nonfiction, Romanticism, "Walden", 1817, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),

					// -------
					Tuple3(
						Tuple8(Human.JohnKeats, Ballad, Literature.Genre.Fiction, Romanticism, "La Belle Dame sans Merci", 1819, England.London.Moorgate, Italy.ApenninePeninsula.PapalStates.Rome),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.JohnKeats, Ballad, Literature.Genre.Fiction, Romanticism, "Ode to Psyche", 1819, England.London.Moorgate, Italy.ApenninePeninsula.PapalStates.Rome),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.JohnKeats, Ballad, Literature.Genre.Fiction, Romanticism, "Ode to Melancholy", 1819, England.London.Moorgate, Italy.ApenninePeninsula.PapalStates.Rome),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.JohnKeats, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "To Autumn", 1819, England.London.Moorgate, Italy.ApenninePeninsula.PapalStates.Rome),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.JohnKeats, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Bright Star", 1819, England.London.Moorgate, Italy.ApenninePeninsula.PapalStates.Rome),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.JohnKeats, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Lamia", 1819, England.London.Moorgate, Italy.ApenninePeninsula.PapalStates.Rome),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.JohnKeats, Ballad, Literature.Genre.Fiction, Romanticism, "La Belle Dame sans Merci", 1819, England.London.Moorgate, Italy.ApenninePeninsula.PapalStates.Rome),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),

					// -------
					// SOURCE = https://en.wikipedia.org/wiki/Victor_Hugo#Works
					Tuple3(
						Tuple8(Human.VictorHugo, Literature.PublicationMedium.Novel, Literature.Genre.Criticism, Romanticism, "Les Miserables", 1862, France.Besancon, France.Paris),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.VictorHugo, Literature.PublicationMedium.Novel, Literature.Genre.HistoricalFiction, Gothic, "Hans of Iceland", 1820, France.Besancon, France.Paris),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.VictorHugo, Literature.PublicationMedium.ShortStory, Literature.Genre.Fiction, Romanticism, "Bug-Jargal", 1820, France.Besancon, France.Paris),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.VictorHugo, Literature.PublicationMedium.Novel, Literature.Genre.Fiction, Romanticism, "The Hunchback of Notre Dame", 1831, France.Besancon, France.Paris),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.VictorHugo, Literature.PublicationMedium.Novel, Literature.Genre.Criticism, Romanticism, "The Poor People", 1854, France.Besancon, France.Paris),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.VictorHugo, Literature.PublicationMedium.Play, Literature.Genre.Nonfiction, Romanticism, "Cromwell", 1819, France.Besancon, France.Paris),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.VictorHugo, Literature.PublicationMedium.Drama, Literature.Genre.Fiction, Romanticism, "Hernani", 1830, France.Besancon, France.Paris),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.VictorHugo, Literature.PublicationMedium.Play, Literature.Genre.HistoricalFiction, Romanticism, "Marion de Lorme", 1831, France.Besancon, France.Paris),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.VictorHugo, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Les Fueilles d'automne", 1831, France.Besancon, France.Paris),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.VictorHugo, Literature.PublicationMedium.Epic, Literature.Genre.Religion, Romanticism, "Dieu", 1891, France.Besancon, France.Paris),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.VictorHugo, Literature.PublicationMedium.Drama, Literature.Genre.Fiction, Romanticism, "Hernani", 1830, France.Besancon, France.Paris),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),

					// -------
					// https://en.wikipedia.org/wiki/Percy_Bysshe_Shelley#Selected_works
					Tuple3(
						Tuple8(Human.PercyByssheShelley, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Ozymandias", 1818, England.WestSussexCounty.HorshamDistrict.Warnham, Italy.Sardinia.GulfOfLaSpezia),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.PercyByssheShelley, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Queen Mab", 1813, England.WestSussexCounty.HorshamDistrict.Warnham, Italy.Sardinia.GulfOfLaSpezia),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.PercyByssheShelley, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Mont Blanc", 1817, England.WestSussexCounty.HorshamDistrict.Warnham, Italy.Sardinia.GulfOfLaSpezia),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.PercyByssheShelley, Literature.PublicationMedium.Poetry, Literature.Genre.History, Romanticism, "The Revolt of Islam", 1818, England.WestSussexCounty.HorshamDistrict.Warnham, Italy.Sardinia.GulfOfLaSpezia),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.PercyByssheShelley, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "To a Skylark", 1820, England.WestSussexCounty.HorshamDistrict.Warnham, Italy.Sardinia.GulfOfLaSpezia),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.PercyByssheShelley, Literature.PublicationMedium.Poetry, Literature.Genre.Nonfiction, Romanticism, "The Cloud", 1820, England.WestSussexCounty.HorshamDistrict.Warnham, Italy.Sardinia.GulfOfLaSpezia),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.PercyByssheShelley, Literature.PublicationMedium.Drama, Literature.Genre.HistoricalFiction, Romanticism, "The Cenci", 1819, England.WestSussexCounty.HorshamDistrict.Warnham, Italy.Sardinia.GulfOfLaSpezia),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.PercyByssheShelley, Literature.PublicationMedium.Drama, Literature.Genre.Mythology, Romanticism, "Prometheus Unbound", 1820, England.WestSussexCounty.HorshamDistrict.Warnham, Italy.Sardinia.GulfOfLaSpezia),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.PercyByssheShelley, Literature.PublicationMedium.Prose, Literature.Genre.Fiction, Romanticism, "The Assassins", 1814, England.WestSussexCounty.HorshamDistrict.Warnham, Italy.Sardinia.GulfOfLaSpezia),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.PercyByssheShelley, Literature.PublicationMedium.Prose, Literature.Genre.Fable, Romanticism, "Una Favola", 1819, England.WestSussexCounty.HorshamDistrict.Warnham, Italy.Sardinia.GulfOfLaSpezia),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.PercyByssheShelley, Literature.PublicationMedium.Essay, Literature.Genre.Religion, Romanticism, "The Necessity of Atheism", 1811, England.WestSussexCounty.HorshamDistrict.Warnham, Italy.Sardinia.GulfOfLaSpezia),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human.PercyByssheShelley, Literature.PublicationMedium.Letter, Literature.Genre.Criticism, Romanticism, "A Letter to Lord Ellenborough", 1812, England.WestSussexCounty.HorshamDistrict.Warnham, Italy.Sardinia.GulfOfLaSpezia),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),

					// -------
					// TODO temporary rows - just for sake of the array_contains test in FilterSpecs
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(Mathematician, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, Engineer, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, Architect, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, Botanist, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, Chemist, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, Geologist, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, Doctor, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, null, Physicist),
						Tuple12(null, null, null, null, null, null, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(Painter, null, null, null, null, null, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, Sculptor, null, null, null, null, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, Musician, null, null, null, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, Dancer, null, null, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, Singer, null, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, Actor, null, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, Designer, null, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, Inventor, null, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, Producer, null, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, Director, null, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
					),
					Tuple3(
						Tuple8(Human, null, null, null, "Title", 1000, World, World),
						Tuple8(null, null, null, null, null, null, null, null),
						Tuple12(null, null, null, null, null, null, null, null, null, null, null, Linguist)
					),

					// TODO continue finishing Romanticism writers + add other ArtPeriod writers + add Painters, Sculptors etc.
				)

				// NOTE: design decision - to use tuples to carry the elements instead of Seq because need to create the df from sturcture Seq[Tuple] not from Seq[Seq]

				val mainStrSeq: Seq[(String, String, String, String, String, Int, String, String)] = artistTupLists.map(tup3 => tup3._1.tupleToHList.enumNames.hlistToTuple)
				val sciStrSeq: Seq[(String, String, String, String, String, String, String, String)] = artistTupLists.map(tup3 => tup3._2.tupleToHList.enumNames.hlistToTuple)
				val artStrSeq: Seq[(String, String, String, String, String, String, String, String, String, String, String, String)] = artistTupLists.map(tup3 => tup3._3.tupleToHList.enumNames.hlistToTuple)

				val mainStrRDD: RDD[(String, String, String, String, String, Int, String, String)] = sess.sparkContext.parallelize(mainStrSeq)
				val sciStrRDD: RDD[(String, String, String, String, String, String, String, String)] = sess.sparkContext.parallelize(sciStrSeq)
				val artStrRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String, String)] = sess.sparkContext.parallelize(artStrSeq)

				val mainRowSeq: Seq[Row] = artistTupLists.map(tup3 => tup3._1.tupleToHList.enumNames.hlistToSparkRow)
				val sciRowSeq: Seq[Row] = artistTupLists.map(tup3 => tup3._2.tupleToHList.enumNames.hlistToSparkRow)
				val artRowSeq: Seq[Row] = artistTupLists.map(tup3 => tup3._3.tupleToHList.enumNames.hlistToSparkRow)

				val mainDf: DataFrame = mainStrSeq.toDF(colnamesMain:_*)
				val sciDf: DataFrame = sciStrSeq.toDF(colnamesSci:_*)
				val artDf: DataFrame = artStrSeq.toDF(colnamesArt:_*)

				// The final culmination
				val craftDf: DataFrame = mainDf.appendDf(sciDf).appendDf(artDf)
				val colnamesCraft: Seq[NameOfCol] = colnamesMain ++ colnamesSci ++ colnamesArt

				// --------

				val mainRowRDD: RDD[Row] = mainDf.rdd
				val sciRowRDD: RDD[Row] = sciDf.rdd
				val artRowRDD: RDD[Row] = artDf.rdd


				/*val artistStrSeq: Seq[(EnumString, EnumString, EnumString, EnumString, TitleOfWork, YearPublished, EnumString, EnumString, EnumString, EnumString, EnumString, EnumString, EnumString, EnumString, EnumString, EnumString)] = artistTupLists.map(_.tupleToHList.enumNames.hlistToTuple /*.tupleToSparkRow*/)

				val artistStrRDD: RDD[(EnumString, EnumString, EnumString, EnumString, TitleOfWork, YearPublished, EnumString, EnumString, EnumString, EnumString, EnumString, EnumString, EnumString, EnumString, EnumString, EnumString)] = sess.sparkContext.parallelize(artistStrSeq)

				val artistRowSeq: Seq[Row] = artistTupLists.map(_.tupleToHList.enumNames.hlistToSparkRow)

				// WARNING: must put the rowrdd after tradedf or else initialization exception! (in spark AboutDataFrames test suite)
				val artistDf: DataFrame = artistStrSeq.toDF(colnamesArtist: _*)
				val artistRowRDD: RDD[Row] = artistDf.rdd*/
			}
			// ---------------------------------------------------------------

			object MusicDf {



				import Instrument.MusicalInstrument._ //bass,w ood, string

				type Year = Integer

				val colnamesMusic: Seq[NameOfCol] = (Artist.Musician, "YearBorn", "YearDied", Instrument.MusicalInstrument, World).tupleToStringList
				val coltypesMusic: List[DataType] = List(StringType, IntegerType, IntegerType, StringType, StringType)

				val musicSchema: StructType = DFUtils.createSchema(colnamesMusic, coltypesMusic)


				val musicSeq: Seq[(Musician, Year, Year, MusicalInstrument, World)] = Seq(
					(Artist.Musician.LouisArmstrong(), 23, 23, BassInstrument.Trumpet, UnitedStates),
				)

				// which cols are of stringtype
				//val colnamesStrMusic: Seq[String] = DFUtils.getColnamesWithType[String](musicSchema)
				//: Seq[(Musician, Year, Year, MusicalInstrument, Country)]


				val musicStrSeq: Seq[(EnumString, Year, Year, EnumString, EnumString)] = musicSeq.map(_.tupleToHList.enumNames.hlistToTuple /*.tupleToSparkRow*/)
				// NOTE: Another way: 	tradeSeq.map(_.to[List].nestedNames), but then cannot turn list to tuple easily again.

				// Finding another way so don't have to go from hlist -> tuple because that function can break if tuple size > 22
				// NOTE: Another way 3 --- but this doesn't work when the tuple has poly-type elements, only single-type elements which fall under the same parent enum.
				//tradeSeq.map(tup => tup.toHList.toList.nestedNames)
				// NOTE: BETTER
				musicSeq.map(tup => tup.toHList.enumNestedNames) // ending type is this.Out so cannot call toList or tosparkrow on it...
				musicSeq.map(tup => tup.toHList.enumNestedNames.toList) // yields List[Any]


				val musicStrRDD: RDD[(EnumString, Year, Year, EnumString, EnumString)] = sess.sparkContext.parallelize(musicStrSeq)

				val musicRowSeq: Seq[Row] = musicSeq.map(_.tupleToHList.enumNames.hlistToSparkRow)

				// WARNING: must put the rowrdd after tradedf or else initialization exception! (in spark AboutDataFrames test suite)
				val musicDf: DataFrame = musicStrSeq.toDF(colnamesMusic: _*)
				val musicRowRDD: RDD[Row] = musicDf.rdd
			}
			// ---------------------------------------------------------------


			object OuterSpaceDf {

				import CelestialBody._

				type HasRings = Boolean
				type HasMoons = Boolean
				type NumRings = Integer
				type NumMoons = Integer
				type ListMoons = List[Moon]
				type ListPlanets = List[Planet]
				type NumLightYearsFromSun = Double
				type IsGoingSupernova = Boolean

				val colnamesNebula: Seq[NameOfCol] = (CelestialBody, "HasRings", "NumRings", "HasMoons", "NumMoons", "ListMoons", "ListPlanets", "NumLightYearsFromSun"," IsGoingSupernova").tupleToStringList
				val coltypesNebula: Seq[DataType] = List(StringType, BooleanType, IntegerType, BooleanType, IntegerType, ArrayType(StringType), ArrayType(StringType), IntegerType, BooleanType)

				val nebulaSchema: StructType = DFUtils.createSchema(colnamesNebula, coltypesNebula)
				// which cols are of stringtype
				val colnamesStrNebula: Seq[NameOfCol] = DFUtils.getColnamesWithType[String](nebulaSchema)

				import CelestialBody._
				/*val  nebulaSeq: Seq[(CelestialBody, HasRings, NumRings, HasMoons, NumMoons, ListMoons, ListPlanets, NumLightYearsFromSun, IsGoingSupernova)] = Seq(
					(Sun, false, 0, false, 0, 0.0, true),

				)


				val nebulaStrSeq: Seq[(EnumString, Boolean, Int, Boolean, Int, Seq[EnumString], Seq[EnumString], Int, Boolean)] = nebulaSeq.map(_.tupleToHList.enumNames.hlistToTuple)

				val nebulaStrRDD: RDD[(String, String, Amount, String, String)] = sess.sparkContext.parallelize(nebulaStrSeq)

				val nebulaRowSeq: Seq[Row] = nebulaSeq.map(_.tupleToHList.enumNames.hlistToSparkRow)

				val nebulaDf: DataFrame = nebulaStrSeq.toDF(colnamesNebula: _*)
				val nebulaRowRDD: RDD[Row] = nebulaDf.rdd*/
			}

		}

		/*object Dfs {

			import Raw._

			val tradeDf: DataFrame = tradeStrSeq.toDF(colnamesTrade: _*)

			//val animalDf: DataFrame = animalStrSeq.toDF(colnamesAnimal: _*)
		}*/



		// ---------------------------------------------------------------

		// ---------------------------------------------------------------
		// ---------------------------------------------------------------
		// ---------------------------------------------------------------



	}
	//})


	/*println("business df = ")
	println(s"tradeseq = ${ManualDataFrames.fromEnums.Raw.tradeSeq}")
	println(s"tradestrseq = ${ManualDataFrames.fromEnums.Raw.tradeStrSeq}")
	println(s"traderowseq = ${ManualDataFrames.fromEnums.Raw.tradeRowSeq}")
	//println(s"tradestrRDD = ${ManualDataFrames.fromEnums.Raw.tradeStrRDD}")
	//println(s"tradeRowRDD = ${ManualDataFrames.fromEnums.Raw.tradeRowRDD}")


	//ManualDataFrames.fromEnums.Dfs.tradeDf.show
	//ManualDataFrames.fromEnums.Dfs.animalDf.show


	val df = ImportedDataFrames.fromBillChambersBook.flightDf
	df.show
	//val res: Array[DataType] = df.schema.fields.map(_.dataType)
	println(s"schema = ${df.printSchema()}")
	println(s"datatypes = ${df.schema.fields.map(_.dataType).toList/*.mkString("Array(", ", ", ")")*/}") // TODO see if prints out list of datatypes instead of having to format that strange way
	println(s"fields = ${df.schema.fields}")*/

}
