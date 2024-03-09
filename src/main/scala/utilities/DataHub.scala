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


		}

		object fromHoldenBook {


		}

		object fromJeanPerrinBook {

		}

	}

	object ManualDataFrames {

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



			type EnumString = String

			object TradeDf {


				type Amount = Integer
				type DateOfTransaction = String

				//val colnamesTrade: List[NameOfCol] = List("Company", "FinancialInstrument", "Amount", "BuyOrSell", "Country")
				//List(Company.name, Instrument.FinancialInstrument.name, "Amount", Transaction.name, Country.name) //.names
				//val colnamesTrade: Seq[NameOfCol] = List(Company, Instrument.FinancialInstrument, "Amount", Transaction, Country).namesAll
				//val colnamesTrade: Seq[NameOfCol] = (Company, Instrument.FinancialInstrument, "Amount", Transaction, Country).toHList.namesEnumOnly.tupled.to[List]
				val colnamesTrade: Seq[NameOfCol] = (Company, Instrument.FinancialInstrument, "Amount", Transaction, "DateOfTransaction", World).tupleToStringList
				val coltypesTrade: List[DataType] = List(StringType, StringType, IntegerType, StringType, StringType, StringType)

				val tradeSchema: StructType = DFUtils.createSchema(colnamesTrade, coltypesTrade)
				// which cols are of stringtype
				val colnamesStrTrade: Seq[String] = DFUtils.getColnamesWithType[String](tradeSchema)

				val tradeSeq: Seq[(Company, Instrument, Amount, Transaction, DateOfTransaction, World)] = Seq(
					(Company.JPMorgan, Instrument.FinancialInstrument.Stock, 2, Transaction.Buy, DateYMD(1998, 3, 23), China),
					(Company.Google, Instrument.FinancialInstrument.Swap, 4, Transaction.Sell, "1998-03-23", UnitedStates),
					(Company.GoldmanSachs, Instrument.FinancialInstrument.Equity, 3, Transaction.Sell, "1998-03-23", UnitedStates),
					(Company.Disney, Instrument.FinancialInstrument.Bond, 10, Transaction.Buy, "1998-03-23", Spain),
					(Company.Amazon, Instrument.FinancialInstrument.Commodity.PreciousMetal.Gold, 12, Transaction.Buy, "1998-03-23", CostaRica),
					(Company.Amazon, Instrument.FinancialInstrument.Commodity.PreciousMetal.Silver, 12, Transaction.Buy, "1998-03-23", CostaRica),
					(Company.Amazon, Instrument.FinancialInstrument.Commodity.Gemstone.Ruby, 12, Transaction.Buy, "1998-03-23", CostaRica),
					(Company.Amazon, Instrument.FinancialInstrument.Commodity, 5, Transaction.Buy, "1998-03-23", CostaRica),
					(Company.Google, Instrument.FinancialInstrument.Derivative, 10, Transaction.Sell, "1998-03-23", Arabia),
					(Company.Ford, Instrument.FinancialInstrument.Derivative, 2, Transaction.Sell, "1998-03-23", Argentina),
					(Company.Apple, Instrument.FinancialInstrument.Stock, 1, Transaction.Buy, "1998-03-23", Canada),
					(Company.IBM, Instrument.FinancialInstrument.Commodity.Gemstone.Emerald, 110, Transaction.Buy, "1998-03-23", Brazil),
					(Company.Samsung, Instrument.FinancialInstrument.Commodity.Gemstone.Sapphire, 2, Transaction.Sell, "1998-03-23", China),
					(Company.Tesla, Instrument.FinancialInstrument.Commodity.CrudeOil, 5, Transaction.Sell, "1998-03-23", Estonia),
					(Company.Deloitte, Instrument.FinancialInstrument.Cash, 9, Transaction.Sell, "1998-03-23", Ireland),

					(Company.Apple, Instrument.FinancialInstrument.Derivative, 2, Transaction.Buy, "1998-03-23", Australia),
					(Company.Samsung, Instrument.FinancialInstrument.Bond, 14, Transaction.Sell, "1998-03-23", Africa),
					(Company.Google, Instrument.FinancialInstrument.Stock, 5, Transaction.Buy, "1998-03-23", Africa),
					(Company.Microsoft, Instrument.FinancialInstrument.Commodity.Gemstone.Amethyst, 14, "1998-03-23", Transaction.Sell, Africa.Kenya),
					(Company.IBM, Instrument.FinancialInstrument.Commodity.Gemstone.Ruby, 24, Transaction.Buy,"1998-03-23",  Mauritius),
					(Company.GoldmanSachs, Instrument.FinancialInstrument.Share, 8, Transaction.Sell, "1998-03-23", Africa.Tanzania),
					(Company.Microsoft, Instrument.FinancialInstrument.Future, 15, Transaction.Buy, "1998-03-23", Peru),
					(Company.Facebook, Instrument.FinancialInstrument.Equity, 2, Transaction.Sell, "1998-03-23", Africa.Uganda),
					(Company.Disney, Instrument.FinancialInstrument.Commodity.Gemstone.Diamond, 3, Transaction.Sell, "1998-03-23", Russia),
					(Company.Walmart, Instrument.FinancialInstrument.Commodity.Gemstone.Tourmaline, 3, Transaction.Sell, "1998-03-23", Russia),

					(Company.Ford, Instrument.FinancialInstrument.Option, 4, Transaction.Sell, "1998-03-23", Spain),
					(Company.Ford, Instrument.FinancialInstrument.Commodity.Gemstone.Citrine, 8, Transaction.Buy, "1998-03-23", Spain),
					(Company.Ford, Instrument.FinancialInstrument.Commodity.Gemstone.Moonstone, 5, Transaction.Sell, "1998-03-23", Spain),
					(Company.Ford, Instrument.FinancialInstrument.Commodity.Gemstone.Pearl, 55, Transaction.Sell, "1998-03-23", France),
					(Company.Ford, Instrument.FinancialInstrument.Commodity.Gemstone.Aquamarine, 8, Transaction.Sell, "1998-03-23", France),
					(Company.Walmart, Instrument.FinancialInstrument.Swap, 57, Transaction.Sell, "1998-03-23", France),
					(Company.Samsung, Instrument.FinancialInstrument.Future, 14, Transaction.Buy, "1998-03-23", Italy),
					(Company.Facebook, Instrument.FinancialInstrument.Cash, 5, Transaction.Sell, "1998-03-23", Greece),
					(Company.Facebook, Instrument.FinancialInstrument.Future, 95, Transaction.Buy, "1998-03-23", Greece),
					(Company.Facebook, Instrument.FinancialInstrument.Commodity.Gemstone.Garnet, 7, Transaction.Sell, "1998-03-23", Greece),
					(Company.Facebook, Instrument.FinancialInstrument.Commodity.Gemstone.Beryl, 71, Transaction.Buy, "1998-03-23", Greece),
					(Company.Facebook, Instrument.FinancialInstrument.Commodity.Gemstone.Peridot, 98, Transaction.Sell, "1998-03-23", Greece),
					(Company.Tesla, Instrument.FinancialInstrument.Equity, 11, Transaction.Sell, "1998-03-23", Turkey),
					(Company.Deloitte, Instrument.FinancialInstrument.Option, 111, Transaction.Sell, "1998-03-23", Arabia),
					(Company.JPMorgan, Instrument.FinancialInstrument.Commodity.PreciousMetal.Gold, 33, Transaction.Buy, "1998-03-23", Pakistan),
					(Company.Disney, Instrument.FinancialInstrument.Option, 12, Transaction.Sell, Scotland),
					(Company.Ford, Instrument.FinancialInstrument.Option, 56, Transaction.Buy, Ireland),
					(Company.Walmart, Instrument.FinancialInstrument.Commodity.Gemstone.Onyx, 63, Transaction.Buy, England),
					(Company.GoldmanSachs, Instrument.FinancialInstrument.Equity, 3, Transaction.Buy, Germany),
					(Company.Walmart, Instrument.FinancialInstrument.Swap, 1, Transaction.Sell, Romania),
					(Company.JPMorgan, Instrument.FinancialInstrument.Derivative, 87, Transaction.Sell, Poland),
					(Company.Starbucks, Instrument.FinancialInstrument.Equity, 8, Transaction.Sell, Serbia),
					(Company.Apple, Instrument.FinancialInstrument.Cash, 14, Transaction.Buy, Slovakia),
					(Company.Disney, Instrument.FinancialInstrument.Derivative, 84, Transaction.Buy, Slovenia),
					(Company.Tesla, Instrument.FinancialInstrument.Swap, 18, Transaction.Buy, Hungary),
					(Company.Nike, Instrument.FinancialInstrument.Future, 34, Transaction.Buy, Croatia),
					(Company.GoldmanSachs, Instrument.FinancialInstrument.Commodity.CrudeOil, 39, Transaction.Buy, Estonia),
					(Company.JPMorgan, Instrument.FinancialInstrument.Commodity.PreciousMetal.Silver, 92, Transaction.Buy, CostaRica),
					(Company.Starbucks, Instrument.FinancialInstrument.Equity, 10, Transaction.Buy, Argentina),
					(Company.JPMorgan, Instrument.FinancialInstrument.Commodity.PreciousMetal.Platinum, 84, Transaction.Buy, Iceland),
					(Company.GoldmanSachs, Instrument.FinancialInstrument.Future, 73, Transaction.Buy, Greenland),
				)

				//val tradeStrSeq: Seq[(String, String, Amount, String, String)] = tradeSeq.map(_.tupleToHList.enumNames.hlistToTuple /*.tupleToSparkRow*/)
				val tradeStrSeq: Seq[(String, String, Amount, String, String)] = tradeSeq.map(tup => tup.toHList.enumNames.hlistToTuple)

				// NOTE: Another way: 	tradeSeq.map(_.to[List].nestedNames), but then cannot turn list to tuple easily again.

				// Finding another way so don't have to go from hlist -> tuple because that function can break if tuple size > 22
				// NOTE: Another way 3 --- but this doesn't work when the tuple has poly-type elements, only single-type elements which fall under the same parent enum.
				//tradeSeq.map(tup => tup.toHList.toList.nestedNames)
				// NOTE: BETTER
				tradeSeq.map(tup => tup.toHList.enumNestedNames) // ending type is this.Out so cannot call toList or tosparkrow on it...
				tradeSeq.map(tup => tup.toHList.enumNestedNames.toList) // yields List[Any]


				val tradeStrRDD: RDD[(String, String, Amount, String, String)] = sess.sparkContext.parallelize(tradeStrSeq)

				//val tradeRowSeq: Seq[Row] = tradeSeq.map(_.tupleToHList.enumNames.hlistToSparkRow)
				val tradeRowSeq: Seq[Row] = tradeSeq.map(tup => tup.toHList.enumNames.hlistToSparkRow)


				// WARNING: must put the rowrdd after tradedf or else initialization exception! (in spark AboutDataFrames test suite)
				val tradeDf: DataFrame = tradeStrSeq.toDF(colnamesTrade: _*)
				val tradeRowRDD: RDD[Row] = tradeDf.rdd
			}

			// ---------------------------------------------------------------

			object AnimalDf {

				type Amount = Integer

				//val colnamesAnimal: List[NameOfCol] = List("Animal", "Amount", "Country", "Climate")
				// val colnamesAnimal: Seq[NameOfCol] = List(Animal, "Amount", Country, Climate).namesAll // WARNING: this returns typename for each elem which is "String" for the Amount and will return "Strin" when truncated since that function is for dealing with enums, must use this other way:


				val colnamesAnimal: Seq[NameOfCol] = (Animal, "Amount", World, Climate).tupleToStringList
				val coltypesAnimal: List[DataType] = List(StringType, IntegerType, StringType, StringType)
				val animalSchema: StructType = DFUtils.createSchema(colnamesAnimal, coltypesAnimal)

				val animalSeq: Seq[(Animal, Amount, World, Climate)] = Seq(
					(Animal.Cat.WildCat.Lion, 12, Africa, Climate.Tundra),
					(Animal.Cat.WildCat.Lion, 12, Arabia, Climate.Desert),
					(Animal.Hyena, 12, Africa, Climate.Desert),

					(Animal.Zebra, 1, Africa, Climate.Arid),
					(Animal.Giraffe, 1, Africa, Climate.Tundra),
					(Animal.Hippo, 10, Africa, Climate.Tundra),
					(Animal.Reptile.Crocodile, 100, Africa, Climate.Rainforest),
					(Animal.Insect.Termite, 120, Africa, Climate.Rainforest),
					(Animal.Insect.Butterfly, 120, China, Climate.Rainforest),
					(Animal.Insect.Bee, 120, Italy, Climate.Mediterranean),
					(Animal.Elephant, 120, Africa, Climate.Dry),
					(Animal.Gorilla, 43, Africa, Climate.Rainforest),

					(Animal.Panda, 120, China, Climate.Rainforest),
					(Animal.Koala, 120, Australia, Climate.Tundra),

					(Animal.Bear, 120, Canada, Climate.Temperate),
					(Animal.Bear, 120, Canada, Climate.Polar),
					(Animal.Bear, 120, Russia, Climate.Temperate),
					(Animal.Bear, 120, Russia, Climate.Polar),
					(Animal.Weasel, 120, Canada, Climate.Temperate),
					(Animal.Marmot, 120, UnitedStates, Climate.Continental),
					(Animal.Reindeer, 120, Canada, Climate.Temperate),
					(Animal.Squirrel, 120, Canada, Climate.Temperate),
					(Animal.Fox, 120, Russia, Climate.Polar),
					(Animal.Fox, 120, Russia, Climate.Temperate),
					(Animal.Fox, 120, Canada, Climate.Polar),
					(Animal.Fox, 120, Canada, Climate.Temperate),
					(Animal.Rabbit, 120, UnitedStates, Climate.Temperate),

					(Animal.Cat.WildCat.Leopard, 12, Brazil, Climate.Rainforest),
					(Animal.SeaCreature.Jellyfish, 12, Brazil, Climate.Rainforest),
					(Animal.Insect.Dragonfly, 12, Brazil, Climate.Rainforest),
					(Animal.Reptile.Snake, 12, Brazil, Climate.Tropical),
					(Animal.Cat.WildCat.Panther, 12, Africa, Climate.Rainforest),
					(Animal.Cat.WildCat.Cougar, 12, Russia, Climate.Polar),
					(Animal.Cat.WildCat.Tiger, 12, Africa, Climate.Rainforest),
					(Animal.Cat.WildCat.Lynx, 12, Russia, Climate.Tundra),
					(Animal.Cat.DomesticCat.PersianCat, 12, Arabia, Climate.Desert),
					(Animal.Cat.DomesticCat.SiameseCat, 12, China, Climate.Temperate),
					(Animal.Cat.DomesticCat.ShorthairedCat, 12, UnitedStates, Climate.Temperate),

					(Animal.Bird.Vulture, 12, Africa, Climate.Desert),
					(Animal.Bird.Pelican, 12, Spain, Climate.Mediterranean),
					(Animal.Bird.Sparrow, 12, France, Climate.Temperate),
					(Animal.Bird.Sparrow, 12, Canada, Climate.Polar),
					(Animal.Bird.Sparrow, 12, Ireland, Climate.Temperate),
					(Animal.Bird.Sparrow, 12, Scotland, Climate.Temperate),
					(Animal.Bird.Hawk, 12, Russia, Climate.Tundra),
					(Animal.Bird.Eagle.GoldenEagle, 12, UnitedStates, Climate.Tundra),
					(Animal.Bird.Flamingo, 12, Brazil, Climate.Rainforest),
					(Animal.Bird.Ostrich, 12, Africa, Climate.Arid),

					(Animal.SeaCreature.Anemone, 4, CostaRica, Climate.Mediterranean),
					(Animal.SeaCreature.Shrimp, 3, France, Climate.Mediterranean),
					(Animal.SeaCreature.Dolphin, 10, Australia, Climate.Tropical),
					(Animal.SeaCreature.Jellyfish, 2, Argentina, Climate.Tropical)
				)
				val animalStrSeq: Seq[(EnumString, Amount, EnumString, EnumString)] = animalSeq.map(tup => tup.toHList.enumNames.hlistToTuple)
					//animalSeq.map(_.tupleToHList.enumNames.hlistToTuple)

				val animalStrRDD: RDD[(EnumString, Amount, EnumString, EnumString)] = sess.sparkContext.parallelize(animalStrSeq)

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
