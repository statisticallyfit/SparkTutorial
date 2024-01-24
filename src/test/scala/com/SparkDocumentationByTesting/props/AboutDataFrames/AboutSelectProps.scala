package com.SparkDocumentationByTesting.props.AboutDataFrames



import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


import com.SparkDocumentationByTesting.state.SpecState._

import utilities.DFUtils
import utilities.DFUtils.implicits._
import utilities.DFUtils.TypeAbstractions._

import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums.{TradeDf, AnimalDf}
import TradeDf._
import AnimalDf._
import com.data.util.EnumHub._

import scala.reflect.runtime.universe._

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper // intercept
import org.scalacheck._
import org.scalatestplus.scalacheck.{ScalaCheckDrivenPropertyChecks, Checkers} //  forAll
import org.scalatest.Assertion


/**
 *
 */
//trait AnyFunSpecAsTrait extends AnyFunSpec

class AboutSelectProps extends AnyFunSpec /*Properties("AboutSelect")*/ with Matchers with ScalaCheckDrivenPropertyChecks with Checkers with SparkSessionWrapper{


	import sparkSessionWrapper.implicits._





	def runPropSelect[C: TypeTag](df: DataFrame, colnameToIndexMap: Map[NameOfCol, Int],
							colnameToTypeMap: Map[TypenameOfCol, DataType],
							logicPropSelect: SelectLogicArgs[C] => Assertion) = {

		val genColName: Gen[NameOfCol] = Gen.oneOf(DFUtils.getColnamesWithType[String](df))

		// logging
		println(s"gen col names: ${ List.fill[Option[NameOfCol]](10)(genColName.sample)}")


		forAll(genColName) { anyStrColname: NameOfCol =>
			val dtpe: DataType = colnameToTypeMap(anyStrColname)

			logicPropSelect(SelectLogicArgs[C](df, anyStrColname, colnameToIndexMap, toRuntimeType(dtpe)))
		}

	}

	// NOTE: cannot pass type dynamically to function ... will have to just choose randomly from String colnames instead to preserve randomness but maintain type staticness.... :( ????




	/*val p = Prop.forAll { (l1: List[Int], l2: List[Int]) =>
		l1.size + l2.size == (l1 ::: l2).size
	}
	check(p)*/


	describe("Selecting columns"){

		describe("selecting by string column name"){


			def logicPropSelectByColname[C: TypeTag](args: SelectLogicArgs[C]): Assertion = {
				val (df: DataFrame, nameOfCol: NameOfCol, colnameToIndexMap: Map[NameOfCol, Int], tph: TypeHolder[C]) = (args.df, args.colName, args.colnameToIndexMap, args.tph)

				//LogicArgs[C] => Assertion = /*(df: DataFrame, colName: NameOfCol, colnameToIndexMap: Map[NameOfCol, Int], tph: TypeHolder[C])*/ /*(df, colName, colnameToIndexMap, tph)*/ => {
				//(df: DataFrame, colName: NameOfCol, colnameToIndexMap: Map[NameOfCol, Int], tph: TypeHolder[C]): Assertion = {
				//val colByOverallCollect: Seq[Long] = flightDf.collect().toSeq.map(row => row.getAs[Long](F.C3))
				//val colByOverallCollect: Seq[Long] = flightDf.collect().toSeq.map(row => row.getAs[Long](F.nameIndexMap(n)))

				val colByOverallCollect_rowtype: Seq[Row] = df.collect().toSeq
				val colBySelectName_rowtype: Seq[Row] = df.select(nameOfCol).collect().toSeq


				val colByOverallCollect: Seq[C] = df.collect().toSeq.map(row => row.getAs[C](colnameToIndexMap(nameOfCol)))
				val colBySelectName: Seq[C] = df.select(nameOfCol).collectCol[C] //.collect().toSeq.map(row => row.getAs[C](0)) // use simple id = 0 because already selecting one column so the row will have length = 1

				val lenRowByOverallCollect: Int = df.collect().head.size
				val lenRowBySelectName: Int = df.select(nameOfCol).collect().head.size

				colByOverallCollect_rowtype shouldBe a[Seq[Row]]
				colBySelectName_rowtype shouldBe a[Seq[Row]]
				colBySelectName shouldBe a[Seq[C]]
				colByOverallCollect shouldBe a[Seq[C]]

				// TESTING equality of the selecting method
				colBySelectName should equal(colByOverallCollect)
				// TESTING the col lengths are all equal
				colBySelectName.length should equal (df.count()) // num rows

				// TESTING row lengths from each selection method
				lenRowByOverallCollect should equal(df.columns.length)
				lenRowBySelectName should equal(1)
				lenRowByOverallCollect should be >= lenRowBySelectName
			}

			it("selecting the string-typed columns"){
				runPropSelect[String](flightDf, FlightState.mapOfNameToIndex, FlightState.mapOfNameToType, logicPropSelectByColname[String])

				runPropSelect[String](animalDf, AnimalState.mapOfNameToIndex, AnimalState.mapOfNameToType, logicPropSelectByColname[String])
			}
			it("selecting the integer-typed columns") {
				runPropSelect[Integer](flightDf, FlightState.mapOfNameToIndex, FlightState.mapOfNameToType, logicPropSelectByColname[Integer])

				runPropSelect[Integer](animalDf, AnimalState.mapOfNameToIndex, AnimalState.mapOfNameToType, logicPropSelectByColname[Integer])
			}
		}



		describe("selecting by symbol ($) column name") {

			def logicPropSelectByColSymbol[C: TypeTag](args: SelectLogicArgs[C]): Assertion = {
				val (df: DataFrame, nameOfCol: NameOfCol, colnameToIndexMap: Map[NameOfCol, Int], tph: TypeHolder[C]) = (args.df, args.colName, args.colnameToIndexMap, args.tph)


				val colByOverallCollect: Seq[C] = df.collect().toSeq.map(row => row.getAs[C](colnameToIndexMap(nameOfCol)))
				val colBySelectSymbol: Seq[C] = df.select($"${nameOfCol}").collectCol[C] //.collect().toSeq.map(row => row.getAs[C](0)) // use simple id = 0 because already selecting one column so the row will have length = 1

				val lenRowByOverallCollect: Int = df.collect().head.size
				val lenRowBySelectSymbol: Int = df.select($"${nameOfCol}").collect().head.size

				colByOverallCollect shouldBe a[Seq[C]]
				colBySelectSymbol shouldBe a[Seq[C]]

				// TESTING equality of the selecting method
				colBySelectSymbol should equal(colByOverallCollect)
				// TESTING the col lengths are all equal
				colBySelectSymbol.length should equal(df.count()) // num rows

				// TESTING row lengths from each selection method
				lenRowByOverallCollect should equal(df.columns.length)
				lenRowBySelectSymbol should equal(1)
				lenRowByOverallCollect should be >= lenRowBySelectSymbol
			}

			it("selecting the string-typed columns") {
				runPropSelect[String](flightDf, FlightState.mapOfNameToIndex, FlightState.mapOfNameToType, logicPropSelectByColSymbol[String])

				runPropSelect[String](animalDf, AnimalState.mapOfNameToIndex, AnimalState.mapOfNameToType, logicPropSelectByColSymbol[String])
			}

			it("selecting the integer-typed columns") {

				runPropSelect[Integer](flightDf, FlightState.mapOfNameToIndex, FlightState.mapOfNameToType, logicPropSelectByColSymbol[Integer])

				runPropSelect[Integer](animalDf, AnimalState.mapOfNameToIndex, AnimalState.mapOfNameToType, logicPropSelectByColSymbol[Integer])
			}
		}

		describe("selecting by col() functions") {

			def logicPropSelectByColFunctions[C: TypeTag](s: SelectLogicArgs[C]): Assertion = {
				val (df: DataFrame, nameOfCol: NameOfCol, colnameToIndexMap: Map[NameOfCol, Int], tph: TypeHolder[C]) = (s.df, s.colName, s.colnameToIndexMap, s.tph)

				val colByOverallCollect: Seq[C] = df.collect().toSeq.map(row => row.getAs[C](colnameToIndexMap(nameOfCol)))
				// use simple id = 0 because already selecting one column so the row will have length = 1
				val colBySelectColfunc: Seq[C] = df.select(col(nameOfCol)).collectCol[C] //.collect().toSeq.map(row => row.getAs[C](0))
				val colBySelectDfcolfunc: Seq[C] = df.select(df.col(nameOfCol)).collectCol[C] //.collect().toSeq.map(row => row.getAs[C](0))
				val colBySelectColumnfunc: Seq[C] = df.select(column(nameOfCol)).collectCol[C] //.collect().toSeq.map(row => row.getAs[C](0))
				// using Symbol instead of ' since ' is deprecated
				val colBySelectApostropheColfunc: Seq[C] = df.select(Symbol(nameOfCol)).collectCol[C] //.collect().toSeq.map(row => row.getAs[C](0))

				colByOverallCollect shouldBe a[Seq[C]]
				colBySelectColfunc shouldBe a[Seq[C]]
				colBySelectDfcolfunc shouldBe a [Seq[C]]
				colBySelectColumnfunc shouldBe a [Seq[C]]
				colBySelectApostropheColfunc shouldBe a [Seq[C]]

				// TESTING equality of the selecting method
				// Testing that all the list cols are the same
				List(colBySelectColfunc, colBySelectDfcolfunc, colBySelectColumnfunc, colBySelectApostropheColfunc)
					.distinct.head should equal(colByOverallCollect)
				// TESTING the col lengths are all equal
				List(colBySelectColfunc, colBySelectDfcolfunc, colBySelectColumnfunc, colBySelectApostropheColfunc, colByOverallCollect)
					.map(_.length).distinct.head should  equal(df.count()) // num rows

				// TESTING row lengths from each selection method
				val lenRowByOverallCollect: Int = df.collect().head.size
				val lenRowBySelectColfunc: Int = df.select(col(nameOfCol)).collect().head.size
				val lenRowBySelectDfcolfunc: Int = df.select(df.col(nameOfCol)).collect().head.size
				val lenRowBySelectColumnfunc: Int = df.select(column(nameOfCol)).collect().head.size
				val lenRowBySelectApostropheColfunc: Int = df.select(Symbol(nameOfCol)).collect().head.size

				lenRowByOverallCollect should equal(df.columns.length)

				val lenEachRowBySingleSelect: Int = List(lenRowBySelectColfunc, lenRowBySelectDfcolfunc, lenRowBySelectColumnfunc, lenRowBySelectApostropheColfunc).distinct.head
				lenEachRowBySingleSelect should equal(1)

				lenRowByOverallCollect should be >= lenEachRowBySingleSelect
			}

			it("selecting the string-typed columns") {
				runPropSelect[String](flightDf, FlightState.mapOfNameToIndex, FlightState.mapOfNameToType, logicPropSelectByColFunctions[String])

				runPropSelect[String](animalDf, AnimalState.mapOfNameToIndex, AnimalState.mapOfNameToType, logicPropSelectByColFunctions[String])
			}

			it("selecting the integer-typed columns") {

				runPropSelect[Integer](flightDf, FlightState.mapOfNameToIndex, FlightState.mapOfNameToType, logicPropSelectByColFunctions[Integer])

				runPropSelect[Integer](animalDf, AnimalState.mapOfNameToIndex, AnimalState.mapOfNameToType, logicPropSelectByColFunctions[Integer])
			}
		}

		describe("selecting columns by selectExpr() and expr()") {

			def logicPropSelectByExpr[C: TypeTag](args: SelectLogicArgs[C]): Assertion = {
				val (df: DataFrame, nameOfCol: NameOfCol, colnameToIndexMap: Map[NameOfCol, Int], tph: TypeHolder[C]) = (args.df, args.colName, args.colnameToIndexMap, args.tph)

				//val dfByOveralCollect: DataFrame = df.collect()
				val dfBySelectExpr: DataFrame = df.selectExpr(s"$nameOfCol")
				val dfByExpr: DataFrame = df.select(expr(nameOfCol))

				val colByOverallCollect: Seq[C] = df.collect().toSeq.map(row => row.getAs[C](colnameToIndexMap(nameOfCol)))
				val colBySelectExpr: Seq[C] = dfBySelectExpr.collectCol[C]
				val colByExpr: Seq[C] = dfByExpr.collectCol[C] //.collect().toSeq.map(row => row.getAs[C](0)) // use simple id = 0 because already selecting one column so the row will have length = 1

				val lenRowByOverallCollect: Int = df.collect().head.size
				val lenRowByExpr: Int = dfByExpr.head.size
				val lenRowBySelectExpr: Int = dfBySelectExpr.head.size

				colByOverallCollect shouldBe a[Seq[C]]
				colBySelectExpr shouldBe a [Seq[C]]
				colByExpr shouldBe a[Seq[C]]

				// TESTING equality of the selecting method
				colByExpr should equal (colBySelectExpr)
				colByExpr should equal(colByOverallCollect)

				// TESTING the col lengths are all equal
				List(colByExpr, colBySelectExpr, colByOverallCollect).map(_.length).distinct.head should  equal (df.count())

				// TESTING row lengths from each selection method
				lenRowByOverallCollect should equal(df.columns.length)
				lenRowByExpr should equal(1)
				lenRowBySelectExpr should equal (1)
				lenRowByOverallCollect should be >= lenRowByExpr
			}

			it("selecting the string-typed columns") {
				runPropSelect[String](flightDf, FlightState.mapOfNameToIndex, FlightState.mapOfNameToType, logicPropSelectByExpr[String])

				runPropSelect[String](animalDf, AnimalState.mapOfNameToIndex, AnimalState.mapOfNameToType, logicPropSelectByExpr[String])
			}

			it("selecting the integer-typed columns") {

				runPropSelect[Integer](flightDf, FlightState.mapOfNameToIndex, FlightState.mapOfNameToType, logicPropSelectByExpr[Integer])

				runPropSelect[Integer](animalDf, AnimalState.mapOfNameToIndex, AnimalState.mapOfNameToType, logicPropSelectByExpr[Integer])
			}
		}
	}


}


/*val colByOverallCollect: Seq[Long] = flightDf.collect().toSeq.map(row => row.getAs[Long](F.C3))
val colBySelectName: Seq[Long] = flightDf.select($"count").collect().toSeq.map(row => row.getAs[Long](0)) // use simple id = 0 because already selecting one column so the row will have length = 1

val lenRowByOverallCollect: Int = flightDf.collect().head.size
val lenRowBySelectName: Int = flightDf.select($"count").collect().head.size

colBySelectName shouldBe a[Seq[Long]]
colByOverallCollect shouldBe a[Seq[Long]]
colBySelectName should equal(colByOverallCollect)

lenRowByOverallCollect should equal(flightDf.columns.length)
lenRowBySelectName should equal(1)
lenRowByOverallCollect should be >= lenRowBySelectName*/