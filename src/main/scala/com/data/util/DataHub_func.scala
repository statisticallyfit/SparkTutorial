//package com.data.util
//
///**
// *
// */
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql._
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.functions._
//import utilities.SparkSessionWrapper
//
//import scala.reflect.runtime.universe._
//
///**
// * GOAL: create datasets here already preloaded so not have to load multiple times
// */
//object DataHub_func extends /*SparkSessionWrapper with*/ App {
//
//
//	val sess: SparkSession = SparkSession.builder().master("local[1]").appName("sparkDocumentationByTesting").getOrCreate()
//	//import sparkMainSession.implicits._
//	//withLocalSparkContext(sess => {
//	//val sess = sparkSessionWrapper
//
//	import sess.implicits._
//
//
//	object ImportedDataFrames {
//
//		//val PATH: String = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/data/BillChambers_SparkTheDefinitiveGuide"
//
//		val PATH: String = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/data"
//		val FORMAT_JSON: String = "json"
//		val FORMAT_CSV: String = "csv"
//
//
//		val folderBillChambers: String = "BillChambers_SparkTheDefinitiveGuide"
//		val folderDamji: String = "Damji_LearningSpark"
//		val folderHolden: String = "ZahariaHolden_LearningSpark"
//		val folderJeanGeorgesPerrin: String = "JeanGeorgesPerrin_SparkInAction"
//
//
//		object fromBillChambersBook /*(sess: SparkSession)*/ {
//
//			val flightDf: DataFrame = sess.read.format(FORMAT_JSON).load(s"$PATH/$folderBillChambers/flight-data/json/2015-summary.json")
//			//val bikeDf: DataFrame = sparkSession.read.format(FORMAT_JSON).load( s"$PATH/$folderBillChambers/bike-data")
//		}
//
//		object fromDamjiBook {
//
//
//		}
//
//		object fromHoldenBook {
//
//
//		}
//
//		object fromJeanPerrinBook {
//
//		}
//
//	}
//
//	object ManualDataFrames {
//
//
//		object fromAlvinHenrickBlog /*(sess: SparkSession)*/ {
//
//			// TODO put source link (alvin henrick from windowing study)
//
//			val empDf: DataFrame = sess.createDataFrame(Seq(
//				(7369, "Smith", "Clerk", 7902, "17-Dec-80", 800, 20, 10),
//				(7499, "Allen", "Salesman", 7698, "20-Feb-81", 1600, 300, 30),
//				(7521, "Ward", "Salesman", 7698, "22-Feb-81", 1250, 500, 30),
//				(7566, "Jones", "Manager", 7839, "2-Apr-81", 2850, 0, 20),
//				(7654, "Marin", "Salesman", 7698, "28-Sep-81", 1250, 1400, 30),
//				(7698, "Blake", "Manager", 7839, "1-May-81", 2850, 0, 30),
//				(7782, "Clark", "Manager", 7839, "9-Jun-81", 2850, 0, 10),
//				(7788, "Scott", "Analyst", 7566, "19-Apr-87", 3000, 0, 20),
//				(7839, "King", "President", 0, "17-Nov-81", 5000, 0, 10),
//				(7844, "Turner", "Salesman", 7698, "8-Sep-81", 1500, 0, 30),
//				(7876, "Adams", "Clerk", 7788, "23-May-87", 800, 0, 20),
//				// Extra rows here to ensure large enough partitions
//				(7342, "Johanna", "Manager", 8923, "14-Apr-99", 2343, 1, 11),
//				(5554, "Patrick", "Manager", 8923, "12-May-99", 2343, 2, 5),
//				(2234, "Stacey", "Manager", 8923, "5-May-01", 3454, 4, 8),
//				(2343, "Jude", "Analyst", 8923, "5-Sept-22", 6788, 3, 5),
//				(5676, "Margaret", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
//				(5676, "William", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
//				(5676, "Bridget", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
//				(5676, "Kieroff", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
//				(5676, "Quinlin", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
//				(6787, "Sarah", "Analyst", 8923, "17-Jun-78", 6788, 1, 2),
//				(2342, "David", "President", 8923, "23-Jan-89", 500, 11, 1),
//				(2345, "Christian", "Clerk", 8923, "31-Jul-98", 2343, 12, 10),
//				(3456, "John", "Salesman", 8923, "21-Dec-00", 2343, 21, 21),
//				(7898, "Lizbeth", "President", 8923, "11-Oct-11", 2343, 22, 34),
//
//			)).toDF("EmpNum", "EmpName", "Job", "Mgr", "Hiredate", "Salary", "Comm", "DeptNum")
//
//
//			//val viewCols: Seq[ColumnName] = List($"EmpName", $"Job", $"Salary")
//			val viewCols: Seq[String] = List("EmpName", "Job", "Salary")
//			val dropCols: Seq[String] = List("EmpNum", "Mgr", "Hiredate", "Comm", "DeptNum")
//
//
//			// NOTE: to do sql query way must create temporary view
//			empDf.createOrReplaceTempView("empDf")
//
//			//empDf
//		}
//
//
//		object fromEnums {
//
//			import shapeless._
//			import syntax.std.tuple._
//			//import syntax.std.product._
//			import shapeless.ops.hlist._
//			import scala.language.implicitConversions
//			import scala.collection.JavaConverters._
//			import org.apache.spark.sql.types.{StringType, StructField, StructType}
//
//			import utilities.GeneralUtils._
//			import com.data.util.EnumHub._
//
//
//
//			def PrepareRawData(sess: SparkSession) = {
//
//				type Amount = Integer
//				type EnumString = String
//
//
//				val colnamesTrade: List[String] = List("Company", "FinancialInstrument", "Amount", "BuyOrSell", "Country")
//				val coltypesTrade: List[DataType] = List(StringType, StringType, IntegerType, StringType, StringType)
//
//				val tradeSeq: Seq[(Company, Instrument, Amount, Transaction, Country)] = Seq(
//					(Company.JPMorgan, Instrument.Financial.Stock, 2, Transaction.Buy, Country.China),
//					(Company.Google, Instrument.Financial.Swap, 4, Transaction.Sell, Country.America),
//					(Company.GoldmanSachs, Instrument.Financial.Equity, 3, Transaction.Sell, Country.America),
//					(Company.Disney, Instrument.Financial.Bond, 10, Transaction.Buy, Country.Spain),
//					(Company.Amazon, Instrument.Financial.Commodity.PreciousMetal.Gold, 12, Transaction.Buy, Country.CostaRica),
//					(Company.Amazon, Instrument.Financial.Commodity.PreciousMetal.Silver, 12, Transaction.Buy, Country.CostaRica),
//					(Company.Amazon, Instrument.Financial.Commodity.Gemstone.Ruby, 12, Transaction.Buy, Country.CostaRica),
//					(Company.Amazon, Instrument.Financial.Commodity, 5, Transaction.Buy, Country.CostaRica),
//					(Company.Google, Instrument.Financial.Derivative, 10, Transaction.Sell, Country.Arabia),
//					(Company.Ford, Instrument.Financial.Derivative, 2, Transaction.Sell, Country.Argentina),
//					(Company.Apple, Instrument.Financial.Stock, 1, Transaction.Buy, Country.Canada),
//					(Company.IBM, Instrument.Financial.Share, 110, Transaction.Buy, Country.Brazil),
//					(Company.Samsung, Instrument.Financial.Share, 2, Transaction.Sell, Country.China),
//					(Company.Tesla, Instrument.Financial.Commodity.CrudeOil, 5, Transaction.Sell, Country.Estonia),
//					(Company.Deloitte, Instrument.Financial.Cash, 9, Transaction.Sell, Country.Ireland)
//				)
//				val tradeStrSeq: Seq[(EnumString, EnumString, Amount, EnumString, EnumString)] = tradeSeq.map(_.tupleToHList.enumsToString.hlistToTuple /*.tupleToSparkRow*/)
//
//				val tradeStrRDD: RDD[(EnumString, EnumString, Amount, EnumString, EnumString)] = sess.sparkContext.parallelize(tradeStrSeq)
//
//				//val tradeRowRDD: RDD[Row] = Dfs.tradeDf.rdd
//
//				val tradeRowSeq: Seq[Row] = tradeSeq.map(_.tupleToHList.enumsToString.hlistToSparkRow)
//
//
//				return (tradeSeq, tradeStrSeq, tradeStrRDD, tradeRowSeq)
//
//
//				// ---------------------------------------------------------------
//
////				val colnamesAnimal: List[String] = List("Animal", "Amount", "Country", "Climate")
////				val coltypesAnimal: List[DataType] = List(StringType, IntegerType, StringType, StringType)
////
////				val animalSeq: Seq[(Animal, Amount, Country, Climate)] = Seq(
////					(Animal.Cat.Lion, 12, Country.Africa, Climate.Tundra),
////					(Animal.Cat.Lion, 12, Country.Arabia, Climate.Desert),
////					(Animal.Hyena, 12, Country.Africa, Climate.Desert),
////
////					(Animal.Zebra, 1, Country.Africa, Climate.Arid),
////					(Animal.Giraffe, 1, Country.Africa, Climate.Tundra),
////					(Animal.Hippo, 10, Country.Africa, Climate.Tundra),
////					(Animal.Crocodile, 100, Country.Africa, Climate.Rainforest),
////					(Animal.Termite, 120, Country.Africa, Climate.Rainforest),
////					(Animal.Elephant, 120, Country.Africa, Climate.Arid),
////					(Animal.Gorilla, 43, Country.Africa, Climate.Rainforest),
////
////					(Animal.Panda, 120, Country.China, Climate.Rainforest),
////					(Animal.Koala, 120, Country.Australia, Climate.Tundra),
////
////					(Animal.Bear, 120, Country.Canada, Climate.Temperate),
////					(Animal.Bear, 120, Country.Russia, Climate.Polar),
////					(Animal.Weasel, 120, Country.Canada, Climate.Temperate),
////					(Animal.Marmot, 120, Country.America, Climate.Continental),
////					(Animal.Reindeer, 120, Country.Canada, Climate.Temperate),
////					(Animal.Squirrel, 120, Country.Canada, Climate.Temperate),
////					(Animal.Fox, 120, Country.Russia, Climate.Polar),
////					(Animal.Rabbit, 120, Country.America, Climate.Temperate),
////
////					(Animal.Cat.Leopard, 12, Country.Brazil, Climate.Rainforest),
////					(Animal.Cat.Panther, 12, Country.Africa, Climate.Rainforest),
////					(Animal.Cat.Cougar, 12, Country.Russia, Climate.Polar),
////					(Animal.Cat.Tiger, 12, Country.Africa, Climate.Rainforest),
////					(Animal.Cat.Lynx, 12, Country.Russia, Climate.Tundra),
////					(Animal.Cat.HouseCat.PersianCat, 12, Country.Arabia, Climate.Desert),
////					(Animal.Cat.HouseCat.SiameseCat, 12, Country.China, Climate.Temperate),
////					(Animal.Cat.HouseCat.ShorthairedCat, 12, Country.America, Climate.Temperate),
////
////					(Animal.Bird.Vulture, 12, Country.Africa, Climate.Desert),
////					(Animal.Bird.Pelican, 12, Country.Spain, Climate.Mediterranean),
////					(Animal.Bird.Sparrow, 12, Country.France, Climate.Temperate),
////					(Animal.Bird.Sparrow, 12, Country.Canada, Climate.Polar),
////					(Animal.Bird.Sparrow, 12, Country.Ireland, Climate.Temperate),
////					(Animal.Bird.Sparrow, 12, Country.Scotland, Climate.Temperate),
////					(Animal.Bird.Hawk, 12, Country.Russia, Climate.Tundra),
////					(Animal.Bird.Eagle.GoldenEagle, 12, Country.America, Climate.Tundra),
////					(Animal.Bird.Flamingo, 12, Country.Brazil, Climate.Rainforest),
////					(Animal.Bird.Ostrich, 12, Country.Africa, Climate.Arid),
////
////					(Animal.SeaCreature.Anemone, 4, Country.CostaRica, Climate.Mediterranean),
////					(Animal.SeaCreature.Shrimp, 3, Country.France, Climate.Mediterranean),
////					(Animal.SeaCreature.Dolphin, 10, Country.Australia, Climate.Tropical),
////					(Animal.SeaCreature.Jellyfish, 2, Country.Argentina, Climate.Tropical)
////				)
////				val animalStrSeq: Seq[(String, Amount, String, String)] = animalSeq.map(_.tupleToHList.enumsToString.hlistToTuple)
////
////
////
////				// ---------------------------------------------------------------
////
////				type Year = Int
////				type TitleOfWork = String
////
////				val colnamesArtists = List("ArtDomain", "ArtistName", "TitleOfWork", "Year", "MusicalInstrument")
////
////				// TODO fix or leave this to continue data frame chapter 5 bill chambers
////
////				/*val artistSeq: Seq[(Art, Artist, Option[TitleOfWork], ArtPeriod, Year, Option[MusicalInstrument])] = Seq(
////					(Art.Painting, Artist.Painter.LeonardoDaVinci, Some("Mona Lisa"), 1503, None),
////					(Art.Painting, Artist.Painter.LeonardoDaVinci, Some("The Last Supper"), 1498, None),
////					(Art.Painting, Artist.Painter.LeonardoDaVinci, Some("The Baptism of Christ"), 1475, None),
////					(Art.Sculpture, Artist.Sculptor.LeonardoDaVinci, Some("Horse in Bronze"), 1482, None),
////
////					(Art.Painting, Artist.Painter.Michelangelo, Some("Hercules"), 1492, None),
////					(Art.Sculpture, Artist.Sculptor.Michelangelo, Some("Battle of the Centaurs"), 1492, None),
////					(Art.Sculpture, Artist.Sculptor.Michelangelo, Some("Crucifix"), 1493, None),
////					(Art.Sculpture, Artist.Sculptor.Michelangelo, Some("David"), 1501, None),
////					(Art.Sculpture, Artist.Sculptor.Michelangelo, Some("Madonna and Child"), 1501, None),
////
////					(Art.Music, Artist.Musician.JellyRollMorton, Some("Burnin' the Iceberg"), 1900, Some(Instrument.Musical.Piano)),
////					(Art.Music, Artist.Musician.HerbieMann, None, 1900, Some(Instrument.Musical.WoodwindInstrument.Flute)),
////
////				)*/
//			}
//
//			def PrepareDFData(sess: SparkSession) {
//
//				val (tradeSeq, tradeStrSeq, tradeStrRDD, tradeRowSeq) = PrepareRawData(sess)
//
//
//				val tradeDf: DataFrame = tradeStrSeq.toDF(raw.colnamesTrade: _*)
//				val tradeRowRDD: RDD[Row] = tradeDf.rdd
//
//				//val animalDf: DataFrame = animalStrSeq.toDF(colnamesAnimal: _*)
//			}
//
//
//
//
//
//
//
//
//
//
//
//			// ---------------------------------------------------------------
//
//			// ---------------------------------------------------------------
//			// ---------------------------------------------------------------
//			// ---------------------------------------------------------------
//
//
//		}
//	}
//	//})
//
//
//	println("business df = ")
//	val raw = ManualDataFrames.fromEnums.PrepareRawData(sess)
//	println(s"tradeseq = ${raw.tradeSeq}")
//	println(s"tradestrseq = ${raw.tradeStrSeq}")
//	println(s"traderowseq = ${raw.tradeRowSeq}")
//	println(s"tradestrRDD = ${raw.tradeStrRDD}")
//
//	val dfs = ManualDataFrames.fromEnums.PrepareDFData(sess)
//	println(s"tradedf = ${dfs.tradeDf}")
//	println(s"tradeRowRDD = ${dfs.tradeRowRDD}")
//
//
//	//ManualDataFrames.fromEnums.Dfs.tradeDf.show
//	//ManualDataFrames.fromEnums.Dfs.animalDf.show
//
//
//	val df = ImportedDataFrames.fromBillChambersBook.flightDf
//	df.show
//	//val res: Array[DataType] = df.schema.fields.map(_.dataType)
//	println(s"schema = ${df.printSchema()}")
//	println(s"datatypes = ${df.schema.fields.map(_.dataType).toList /*.mkString("Array(", ", ", ")")*/}") // TODO see if prints out list of datatypes instead of having to format that strange way
//	println(s"fields = ${df.schema.fields}")
//
//}
//
