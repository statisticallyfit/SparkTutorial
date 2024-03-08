package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutFiltering


import org.apache.spark.sql.catalyst.expressions._ //genericrowwithschema...
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, Dataset, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize }
import org.apache.spark.sql.types._
//import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions._

import utilities.DFUtils; import DFUtils._ ; import DFUtils.TypeAbstractions._; import DFUtils.implicits._
import utilities.GeneralMainUtils._
import utilities.GeneralMainUtils.implicits._
import utilities.DataHub.ImportedDataFrames.fromBillChambersBook._
import utilities.DataHub.ManualDataFrames.fromEnums._
import utilities.DataHub.ManualDataFrames.fromSparkByExamples._
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
import Animal._ ; import SeaCreature._; import Bird._ ; import Eagle._ ; import Cat._
import Instrument._; import FinancialInstrument._ ; import Commodity._

import World.Africa._
import World.Europe._
import World.NorthAmerica._
import World.SouthAmerica._
import World._
import World.Asia._
import World.Oceania._
import World.CentralAmerica._

//import com.SparkSessionForTests
import com.SparkDocumentationByTesting.CustomMatchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import org.scalatest.Assertions._
import utilities.SparkSessionWrapper




/**
 * SOURCE: spark-test-repo:
 * 	- https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L617-L636
 */
class FilterSpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {

	/*import AnimalState._
	import FlightState._
	import TradeState._*/

	import sparkSessionWrapper.implicits._


	// TODO restructure later
	describe("Filtering (same as where)"){


		describe("can be done via multiple syntaxes"){


			val historySnippetSeqSeq: Seq[Tuple3[HumanInfo, ScienceCrafts, ArtCrafts]] = Seq(
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
				Tuple3(
					Tuple8(Human.HenryDavidThoreau, Literature.PublicationMedium.Essay, Literature.Genre.HistoricalFiction, Romanticism, "Aulus Perseus Flaccus", 1840, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord, UnitedStates.Massachusetts.MiddlesexCountyUS.Concord),
					Tuple8(null, null, null, null, null, null, null, null),
					Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
				),
				Tuple3(
					Tuple8(Human.VictorHugo, Literature.PublicationMedium.Novel, Literature.Genre.HistoricalFiction, Gothic, "Hans of Iceland", 1820, France.Besancon, France.Paris),
					Tuple8(null, null, null, null, null, null, null, null),
					Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
				),
				Tuple3(
					Tuple8(Human.VictorHugo, Literature.PublicationMedium.Play, Literature.Genre.HistoricalFiction, Romanticism, "Marion de Lorme", 1831, France.Besancon, France.Paris),
					Tuple8(null, null, null, null, null, null, null, null),
					Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
				),
				Tuple3(
					Tuple8(Human.PercyByssheShelley, Literature.PublicationMedium.Drama, Literature.Genre.HistoricalFiction, Romanticism, "The Cenci", 1819, England.WestSussexCounty.HorshamDistrict.Warnham, Italy.Sardinia.GulfOfLaSpezia),
					Tuple8(null, null, null, null, null, null, null, null),
					Tuple12(null, null, null, null, null, null, null, null, null, null, Writer, null)
				),
			)

			val css: Seq[(String, String, String, String, String, Int, String, String)] = artistTupLists.map(tup3 => tup3._1.tupleToHList.enumNames.hlistToTuple)
			val mss: Seq[(String, String, String, String, String, String, String, String)] = artistTupLists.map(tup3 => tup3._2.tupleToHList.enumNames.hlistToTuple)
			val ass: Seq[(String, String, String, String, String, String, String, String, String, String, String, String)] = artistTupLists.map(tup3 => tup3._3.tupleToHList.enumNames.hlistToTuple)

			val cdf: DataFrame = css.toDF(colnamesMain: _*)
			val mdf: DataFrame = mss.toDF(colnamesSci: _*)
			val adf: DataFrame = ass.toDF(colnamesArt: _*)

			// The final culmination
			val historySnippetDf: DataFrame = cdf.appendDf(mdf).appendDf(adf)

			// This snippet should only have historical fiction genre
			// TODO why when calling collectEnumCol[Genre] it fails? what is wrongw ith using deeper-nested enums???
			historySnippetDf.select(Genre.enumName).collectEnumCol[Genre].toSet.head shouldEqual Genre.HistoricalFiction



			// --------

			it("using string condition") {

				craftDf.filter("Genre == 'HistoricalFiction'") should equalDataFrame(historySnippetDf)

				craftDf.filter(s"${Genre.enumName} == '${Genre.HistoricalFiction.enumName}'") should equalDataFrame(historySnippetDf)

				(craftDf.where(expr("upper(TitleOfWork)").contains("X"))
					.select("TitleOfWork")
					.collectCol[NameOfCol]
					.head) should equal("Kubla Khan (Xanadu)")
				/*should  equalDataFrame( Seq(
					(Human.SamuelTaylorColeridge, Literature.PublicationMedium.Poetry, Literature.Genre.Fiction, Romanticism, "Kulba Khan (Xanadu)", 1816, England.DevonCounty.DevonDistrict.OtteryStMary, England.Middlesex.Highgate, null, null, null, null, null, Writer, null, null),
				).map(tup => tup.tupleToHList.enumNames.hlistToTuple).toDF(colnamesArtist:_*))*/
			}

			// --------

			describe("using column condition"){

				describe("column condition: can vary the number of conditions"){

					it("single column") {
						craftDf.filter(col(Genre.enumName) === Genre.HistoricalFiction.enumName) should equalDataFrame(historySnippetDf)
						craftDf.filter($"Genre" === Genre.HistoricalFiction.enumName) should equalDataFrame(historySnippetDf)
						craftDf.filter(craftDf(Genre.enumName) === Genre.HistoricalFiction.enumName) should equalDataFrame(historySnippetDf)
					}

					it("multiple columns") {

						val rowCheck1: Row = Row(
							Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Morbidity, Romanticism, "His Heart Was Darker than the Starless Night", 1945, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst).tupleToStringList: _*
						)
						val rowCheck2: Row = Row(
							Tuple8(Human.EmilyDickinson, Literature.PublicationMedium.Poetry, Literature.Genre.Morbidity, Romanticism, "Because I could not stop for Death", 1890, UnitedStates.Massachusetts.Amherst, UnitedStates.Massachusetts.Amherst).tupleToStringList: _*
						)
						val rowCheck3: Row = Row(
							Tuple8(Human.EdgarAllanPoe, Literature.PublicationMedium.Poetry, Literature.Genre.Morbidity, Gothic, "Annabel Lee", 1843, UnitedStates.Massachusetts.Boston, UnitedStates.Maryland.Baltimore).tupleToStringList: _*
						)

						val resultMultiCol: Seq[Row] = craftDf.filter(
							col(Craft.enumName) === PublicationMedium.Poetry.enumName &&
								col(Genre.enumName) === Genre.Morbidity.enumName
						).drop((colnamesSci ++ colnamesArt): _*).collectAll

						resultMultiCol shouldEqual Seq(rowCheck1, rowCheck2, rowCheck3)
					}
				}



				// SOURCE: spark-test-repo =  https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L416-L428
				describe("column condition: using between()") {

					it("between() checks if value from a column is between two parameters"){

						import utilities.DataHub.ManualDataFrames.XYNumDf._

						val expectedBetweenXY: Seq[Row] = betweenDf.collectAll.filter(row => (row.getInt(1) >= row.getInt(0)) && (row.getInt(1) <= row.getInt(2)))

						val dfBetweenXY: Dataset[Row] = betweenDf.filter($"b".between($"x", $"y"))

						dfBetweenXY.collectAll should equal(expectedBetweenXY)
						dfBetweenXY.collectAll should equal(Seq(
							Row(5, 7, 7), Row(8, 8, 8), Row(-9, -7, 1), Row(-11, 0, 1), Row(1, 12, 14), Row(3, 5, 5), Row(1, 1, 7), Row(7, 8, 8)
						))
						// between expects col args not string args
						betweenDf.filter($"b".between("x", "y")).count() should be(0)

						// ---

						val dfBetweenYX: Dataset[Row] = betweenDf.filter($"b".between($"y", $"x"))
						val expectedBetweenYX: Seq[Row] = betweenDf.collectAll.filter(row => (row.getInt(1) >= row.getInt(2)) && (row.getInt(1) <= row.getInt(0)))
						dfBetweenYX.collectAll should equal(expectedBetweenYX)
						dfBetweenYX.collectAll should equal(Seq(
							Row(8, 8, 8), Row(5, 3, 1), Row(4, 3, 2)
						))
					}
				}


				// ---------


				// SOURCE = https://sparkbyexamples.com/spark/filter-spark-dataframe-using-values-from-a-list/#h-2-2-filter-rows-based-on-multiple-columns
				describe("column condition: using isin(), isInCollection()") {

					it("works when columns have appropriate types") {

						val filterList: Seq[PublicationMedium] = List(Poetry, Ballad)

						val resultIsIn: Seq[PublicationMedium] = (craftDf.filter(col(Craft.enumName).isin(filterList.enumNames: _*))
							.select(Craft.enumName)
							.collectEnumCol[PublicationMedium])
							.distinct

						resultIsIn shouldEqual filterList


						// -----


						import utilities.DataHub.ManualDataFrames.XYNumDf._

						val dfXIsIn: Dataset[Row] = numDf.filter($"x".isin(0, 5, 8))
						val dfXIsInIter: Dataset[Row] = numDf.filter($"x".isInCollection(List(0, 5, 8)))
						val expectedXIsIn: Seq[Row] = numDf.collectAll.filter(row => row.getInt(0) == 0 || row.getInt(0) == 5 || row.getInt(0) == 8)

						dfXIsIn should equalDataFrame(dfXIsInIter)
						dfXIsIn.collectAll should equal(expectedXIsIn)
						dfXIsIn.collectAll shouldEqual Seq(
							Row(5, 7), Row(8, 8), Row(5, 1), Row(8, 9)
						)
						// ---
						val dfYIsIn: Dataset[Row] = numDf.filter($"y".isin(0, 5, 8))
						val expectedYIsIn: Seq[Row] = numDf.collectAll.filter(row => row.getInt(1) == 0 || row.getInt(1) == 5 || row.getInt(1) == 8)

						dfYIsIn.collectAll shouldEqual expectedYIsIn
						dfYIsIn.collectAll shouldEqual Seq(
							Row(8, 8), Row(3, 5), Row(7, 8)
						)

						// TODO verify auto-casting: https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L445-L451

					}


					it("throws exception when cols have too-different types") {

						import org.apache.spark.sql.AnalysisException

						val df: DataFrame = Seq((1, Seq(1)), (2, Seq(2)), (3, Seq(3))).toDF("a", "b")

						val err = intercept[AnalysisException] {
							df.filter($"a".isin($"b"))
						}
						err.getMessage should (include("[DATATYPE_MISMATCH.DATA_DIFF_TYPES] Cannot resolve \"(a IN (b))\" due to data type mismatch: Input to `in` should all be the same type, but it's [\"INT\", \"ARRAY<INT>\"]"))


						// ---------



						// Step 1: creating additional column list to store the list of skills of the people

						val craftSummaryDf: DataFrame = DFUtils.gatherNonNullsToListCol(
							craftDf,
							colsToGetFrom = colnamesSci ++ colnamesArt,
							lstColname = "ListOfSkills"
						).drop((colnamesSci ++ colnamesArt): _*)


						// Step 2: explosion
						import org.apache.spark.sql.catalyst.ExtendedAnalysisException

						val catchErr = intercept[ExtendedAnalysisException] {
							craftSummaryDf.filter(col("ListOfSkills").isin(List(Mathematician, Engineer, Architect, Botanist, Chemist, Geologist, Doctor, Physicist).enumNames: _*))
						}
						catchErr shouldBe an[ExtendedAnalysisException]
						catchErr.getMessage should include("[DATATYPE_MISMATCH.DATA_DIFF_TYPES] Cannot resolve \"(ListOfSkills IN (Mathematician, Engineer, Architect, Botanist, Chemist, Geologist, Doctor, Physicist))\" due to data type mismatch: Input to `in` should all be the same type, but it's [\"ARRAY<STRING>\", \"STRING\", \"STRING\", \"STRING\", \"STRING\", \"STRING\", \"STRING\", \"STRING\", \"STRING\"]")

					}
				}




				// -------

				// SOURCE = https://sparkbyexamples.com/spark/spark-filter-contains-like-rlike-examples/
				describe("column condition: checking if column contains() a value"){

					it("contains(): called on single column"){

						val resultContains: Seq[Human] = (craftDf.filter(col(Human.enumName).contains("Dick"))
							.select(Human.enumName)
							.collectEnumCol[Human]
							.distinct)

						resultContains shouldEqual Seq(Human.EmilyDickinson, Human.CharlesDickens)

					}

					// SOURCE = https://hyp.is/lkvfwNycEe6s6I8VgRGCAQ/sparkbyexamples.com/spark/filter-spark-dataframe-using-values-from-a-list/
					it("contains(): called on multiple columns"){

						val fs: Seq[FinancialInstrument] = List(Stock, Bond, PreciousMetal.Gold, Gemstone.Diamond)
						val ws: Seq[World] = List(Africa, Europe.Italy, Europe.France, Asia.Russia)
						val listToFilterFrom: Seq[(EnumString, EnumString)] = fs.zip(ws).map(tup => (tup._1.enumName, tup._2.enumName))

						val resultContainsMultiDf: DataFrame = tradeDf.filter((row: Row) => {
							val nameCol1: String = row.getAs[String](FinancialInstrument.enumName)
							val nameCol2: String = row.getAs[String](World.enumName)

							// WARNING: weird syntax - usually contains() is called on a COLUMN object. For single cols, this would be like calling .contains() directly on the string colname.
							listToFilterFrom.contains((nameCol1, nameCol2))
						})

						import Company._;
						import Transaction._

						resultContainsMultiDf.collectAll shouldEqual Seq(
							Row((Google, Stock, 5, Buy, Africa).tupleToStringList: _*),
							Row((Disney, Gemstone.Diamond, 3, Sell, Russia).tupleToStringList: _*)
						)
					}

					// TODO sql version of contains = https://hyp.is/fZOETt06Ee6ZzntzB8V6QA/sparkbyexamples.com/spark/spark-filter-contains-like-rlike-examples/
				}



				// SOURCE = https://sparkbyexamples.com/spark/spark-filter-startswith-endswith-examples/
				describe ("column condition: checking if column name starts / ends with a value"){

					it("using startsWith()"){


						val resultStartsWith: Seq[FinancialInstrument] = (tradeDf.filter(col(FinancialInstrument.enumName).startsWith("S") and !(col(FinancialInstrument.enumName) isin (Commodity.values.enumNames: _*))
						).select(col(FinancialInstrument.enumName))
							.collectEnumCol[FinancialInstrument]
							.distinct)

						resultStartsWith shouldEqual Seq(Stock, Swap, Share)
					}

					it("using endsWith()"){

						val resultEndsWith: Seq[World] = (tradeDf.filter(col(World.enumName).endsWith("ia"))
							.select(col(World.enumName))
							.collectEnumCol[World]
							.distinct)

						resultEndsWith shouldEqual Seq(Arabia, Estonia, Australia, Tanzania, Russia, Romania, Serbia, Slovakia, Slovenia, Croatia)
					}
					// TODO start/endwith using SQL = https://hyp.is/edpwZt1NEe6vLL-6IWfOTw/sparkbyexamples.com/spark/spark-filter-startswith-endswith-examples/
				}


				// --------
				it("column condition: regex function") {
					import Human._

					// find authosr with uppercase C in their name
					val resultRegex: Seq[Human] = (craftDf.where(col(Human.enumName).rlike("[C].*"))
						.select(Human.enumName)
						.collectEnumCol[Human]
						.distinct)

					resultRegex shouldEqual Seq(CharlotteBronte, SamuelTaylorColeridge)
				}



				// --------


				// SOURCE: spark-test-repo = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L330-L414
				describe("column condition: using boolean binary comparison operation") {

					import utilities.DataHub.ManualDataFrames.XYNumDf.numDf
					import utilities.DataHub.ManualDataFrames.XYNumOptionDf.numOptionDf

					it("===") {


						val xOnesDf: Dataset[Row] = numDf.filter($"x" === 1)
						val expectedXOnes: Seq[Row] = numDf.collectAll.filter(row => row.getInt(0) == 1)

						val bothOnesDf: Dataset[Row] = numDf.filter(($"x" === 1) && ($"y" === 1))
						val expectedBothOnes: Seq[Row] = numDf.collectAll.filter(row => (row.getInt(0) == 1) && (row.getInt(1) == 1))

						xOnesDf.collectAll shouldEqual expectedXOnes
						xOnesDf.collectAll shouldEqual Seq(
							Row(1, 4), Row(1, 1), Row(1, 1), Row(1, 14), Row(1, 7), Row(1, 1)
						)

						bothOnesDf.collectAll shouldEqual expectedBothOnes
						bothOnesDf.collectAll shouldEqual Seq(Row(1, 1), Row(1, 1), Row(1, 1))
					}

					it("<==> (equality that works for None)") {
						import utilities.DataHub.ManualDataFrames.XYNumOptionDf._

						numOptionDf.filter($"xn" === null).count() should equal(0)
						numOptionDf.filter($"xn" <=> null).collectAll shouldEqual Seq(
							Row(null, 1), Row(null, 8)
						)

					}
					it("=!=  (inequality)") {

						numDf.filter($"x" =!= 1).collectAll should contain allElementsOf numDf.collectAll.filter(row => row.getInt(0) != 1)


						// NOTE: does not work when there are nulls present
						numOptionDf.filter($"xn" =!= null).count() shouldEqual 0
						// but there are nulls
						numOptionDf.select($"xn").collectCol[Int].filter(_ != null).isEmpty shouldBe false
					}

					it(">, >=") {

						//val expectedGreaterThanConst: Seq[Row] = Seq(Row(8, 8), Row(10, 2), Row(7, 8), Row(8, 9), Row(7, 10))
						val dfGreaterThanVal: Dataset[Row] = numDf.filter($"x" > 5)

						dfGreaterThanVal.collectAll should equal(Seq(Row(8, 8), Row(10, 2), Row(7, 8), Row(8, 9), Row(10, 7))) //(expectedGreaterThanConst)
						dfGreaterThanVal.collectAll should equal(numDf.collectAll.filter(row => row.getInt(0) > 5))


						val dfGreaterThanCol: Dataset[Row] = numDf.filter($"x" > $"y")
						dfGreaterThanCol.collectAll should equal(Seq(Row(4, 1), Row(10, 2), Row(5, 1), Row(4, 2), Row(10, 7)))
						dfGreaterThanCol.collectAll should equal(numDf.collectAll.filter(row => row.getInt(0) > row.getInt(1)))


						// Can distinguish even when there are nulls.
						numOptionDf.filter($"xn" > 5).collectAll should equal(Seq(
							Row(10, null), Row(7, 8), Row(8, null), Row(7, 10)
						))


						// ----------
						numDf.filter($"y" >= 10).collectAll shouldEqual numDf.collectAll.filter(row => row.getInt(1) >= 10)
					}

					it("<, <=") {

						numDf.filter($"y" < 0).collectAll shouldEqual numDf.collectAll.filter(row => row.getInt(1) < 0)
						numDf.filter(($"y" < 10) && (col("x") >= 3)).collectAll shouldEqual numDf.collectAll.filter(row => (row.getInt(1) < 10) && (row.getInt(0) >= 3))
					}


					import utilities.DataHub.ManualDataFrames.BooleanData._

					//SOURCE: spark-test-repo = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L589-L615

					it("&&") {

						val dfATrue: Dataset[Row] = booleanDf.filter($"a" && true)
						dfATrue.collectAll shouldEqual booleanDf.collectAll.filter(row => row.getBoolean(0) && true)
						dfATrue.collectAll shouldEqual Seq(Row(true, true), Row(true, false))

						val dfAFalse: Dataset[Row] = booleanDf.filter($"a" && false)
						dfAFalse.collectAll shouldEqual booleanDf.collectAll.filter(row => row.getBoolean(0) && false)
						dfAFalse.collectAll shouldEqual Seq()

						val dfAB: Dataset[Row] = booleanDf.filter($"a" && $"b")
						dfAB.collectAll shouldEqual booleanDf.collectAll.filter(row => row.getAs[Boolean](0) && row.getAs[Boolean](1))
						dfAB.collectAll shouldEqual Seq(Row(true, true))
					}


					it("||") {

						// case - enums
						val houseCats: Seq[EnumString] = Animal.Cat.DomesticCat.values.enumNames

						val isDomesticCatCondition: Column = houseCats.tail.foldLeft(col(Animal.enumName) === houseCats.head)((accExpr: Column, nextCat: EnumString) => accExpr || (col(Animal.enumName) === nextCat))

						val resultOr: Seq[Row] = animalDf.filter(isDomesticCatCondition).collectAll

						resultOr shouldEqual Seq(
							Row((DomesticCat.PersianCat, 12, Arabia, Climate.Desert).tupleToStringList: _*),
							Row((DomesticCat.SiameseCat, 12, China, Climate.Temperate).tupleToStringList: _*),
							Row((DomesticCat.ShorthairedCat, 12, UnitedStates, Climate.Temperate).tupleToStringList: _*)
						)


						// --------

						// case - simple dfs

						val dfATrue: Dataset[Row] = booleanDf.filter($"a" || true)
						dfATrue.collectAll shouldEqual booleanDf.collectAll.filter(row => row.getBoolean(0) || true)
						dfATrue.collectAll shouldEqual booleanDf.collectAll

						val dfAFalse: Dataset[Row] = booleanDf.filter($"a" || false)
						dfAFalse.collectAll shouldEqual booleanDf.collectAll.filter(row => row.getBoolean(0) || false)
						dfAFalse.collectAll should equal(Seq(Row(true, true), Row(true, false)))
						//dfAFalse.collectAll should contain allElementsOf booleanDf.take(2)

						val dfAB: Dataset[Row] = booleanDf.filter($"a" || $"b")
						dfAB.collectAll shouldEqual booleanDf.collectAll.filter(row => row.getAs[Boolean](0) || row.getAs[Boolean](1))
						dfAB.collectAll shouldEqual Seq(Row(true, true), Row(true, false), Row(false, true))
						//dfAB.collectAll should contain allElementsOf booleanDf.take(3)

					}
					// TODO upgrade using the enum dfs (more conceptual than just simple numbers)
				}



				// --------


				// SOURCE = https://sparkbyexamples.com/spark/spark-dataframe-where-filter/

				// SOURCE = https://hyp.is/G2cnINylEe6nlickLLQtmw/sparkbyexamples.com/spark/filter-spark-dataframe-using-values-from-a-list/

				describe("column condition: using a SQL array (filtering) function") {

					// WARNING if you don't include Human. in front of each name, the creation of df won't work - will say these persons are not of type Human - weird why?

					val skillSeq: Seq[(Human, Seq[Craft])] = Seq(
						(Human.VanGogh, List(Painter)),
						(Human.LeonardoDaVinci, List(Painter, Sculptor, Linguist, Writer, Musician, Mathematician, Architect, Engineer, Geologist, Botanist)),
						(Human.AlbertEinstein, List(Physicist, Mathematician)),
						(Human.Michelangelo, List(Painter, Sculptor)),
						(Human.ClaudeMonet, List(Painter)),
						(Human.PhilCollins, List(Musician, Singer)),
						(Human.RodStewart, List(Musician, Singer)),
						(Human.RalphWaldoEmerson, List(Writer)),
						(Human.AnnaPavlova, List(Dancer)),
						(Human.MayaAngelou, List(Writer, Producer, Director, Dancer, Linguist, Actor))
					)
					val skillStrSeq: Seq[(EnumString, Seq[EnumString])] = skillSeq.map { case (human, lst) => (human.enumName, lst.enumNames) }

					val skillDf: DataFrame = skillStrSeq.toDF(Human.enumName, "ListOfSkills")

					import Human._


					it("array_contains()") {

						val resultArrayContains: Seq[Human] = (skillDf.filter(array_contains(col("ListOfSkills"), Mathematician.enumName))
							.select(Human.enumName)
							.collectEnumCol[Human])

						resultArrayContains shouldEqual Seq(LeonardoDaVinci, AlbertEinstein)

					}

					it("array_filter() -- renamed to filter??") {

					}

					it("using the size of the array in the column") {
						val resultArraySize: Seq[Human] = (skillDf.filter(sqlSize(col("ListOfSkills")) > 2)
							.select(col(Human.enumName))
							.collectCol[Human])

						resultArraySize shouldEqual Seq(LeonardoDaVinci, MayaAngelou)
					}

				}



				// --------
				

				// SOURCE (wrong) = https://hyp.is/YEkP6NylEe6uuDcFkPxKWQ/sparkbyexamples.com/spark/filter-spark-dataframe-using-values-from-a-list/

				describe("column condition: filter on nested struct columns") {

					it("can filter through multiple layers of nesting") {

						val resultFilterNested: Row = (dfNested_2
							.filter(col("address.current.city") === "Newark")
							.select("name.firstname")
							.head)

						resultFilterNested shouldEqual Row("Robert")
					}
				}

			}
		}
	}
}
