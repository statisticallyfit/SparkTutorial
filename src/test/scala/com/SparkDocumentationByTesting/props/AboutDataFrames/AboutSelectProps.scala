package com.SparkDocumentationByTesting.props.AboutDataFrames



import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


import com.SparkDocumentationByTesting.state.ColumnTestsState._

import utilities.DFUtils
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


			def logicPropSelectByColname[C: TypeTag](s: SelectLogicArgs[C]): Assertion = {
				val (df: DataFrame, nameOfCol: NameOfCol, colnameToIndexMap: Map[NameOfCol, Int], tph: TypeHolder[C]) = (s.df, s.colName, s.colnameToIndexMap, s.tph)

				//LogicArgs[C] => Assertion = /*(df: DataFrame, colName: NameOfCol, colnameToIndexMap: Map[NameOfCol, Int], tph: TypeHolder[C])*/ /*(df, colName, colnameToIndexMap, tph)*/ => {
				//(df: DataFrame, colName: NameOfCol, colnameToIndexMap: Map[NameOfCol, Int], tph: TypeHolder[C]): Assertion = {
				//val colByOverallCollect: Seq[Long] = flightDf.collect().toSeq.map(row => row.getAs[Long](F.C3))
				//val colByOverallCollect: Seq[Long] = flightDf.collect().toSeq.map(row => row.getAs[Long](F.nameIndexMap(n)))
				val colByOverallCollect: Seq[C] = df.collect().toSeq.map(row => row.getAs[C](colnameToIndexMap(nameOfCol)))
				val colByNameSelect: Seq[C] = df.select(nameOfCol).collect().toSeq.map(row => row.getAs[C](0)) // use simple id = 0 because already selecting one column so the row will have length = 1

				val lenRowByOverallCollect: Int = df.collect().head.size
				val lenRowByNameSelect: Int = df.select(nameOfCol).collect().head.size

				colByNameSelect shouldBe a[Seq[C]]
				colByOverallCollect shouldBe a[Seq[C]]

				colByNameSelect should equal(colByOverallCollect)

				lenRowByOverallCollect should equal(flightDf.columns.length)
				lenRowByNameSelect should equal(1)
				lenRowByOverallCollect should be >= lenRowByNameSelect
			}

			it("selecting the string-typed columns"){
				runPropSelect[String](flightDf, FlightState.nameIndexMap, FlightState.nameTypeMap, logicPropSelectByColname[String])
			}
			it("selecting the integer-typed columns") {
				runPropSelect[Integer](flightDf, FlightState.nameIndexMap, FlightState.nameTypeMap, logicPropSelectByColname[Integer])
			}
		}



		describe("selecting by symbol ($) column name") {

			def logicPropSelectByColSymbol[C: TypeTag](s: SelectLogicArgs[C]): Assertion = {
				val (df: DataFrame, nameOfCol: NameOfCol, colnameToIndexMap: Map[NameOfCol, Int], tph: TypeHolder[C]) = (s.df, s.colName, s.colnameToIndexMap, s.tph)

				val colByOverallCollect: Seq[C] = df.collect().toSeq.map(row => row.getAs[C](colnameToIndexMap(nameOfCol)))
				val colByNameSelect: Seq[C] = df.select($"${nameOfCol}").collect().toSeq.map(row => row.getAs[C](0)) // use simple id = 0 because already selecting one column so the row will have length = 1

				val lenRowByOverallCollect: Int = df.collect().head.size
				val lenRowByNameSelect: Int = df.select($"${nameOfCol}").collect().head.size

				colByNameSelect shouldBe a[Seq[C]]
				colByOverallCollect shouldBe a[Seq[C]]

				colByNameSelect should equal(colByOverallCollect)

				lenRowByOverallCollect should equal(flightDf.columns.length)
				lenRowByNameSelect should equal(1)
				lenRowByOverallCollect should be >= lenRowByNameSelect
			}
			it("selecting the string-typed columns") {
				runPropSelect[String](flightDf, FlightState.nameIndexMap, FlightState.nameTypeMap, logicPropSelectByColSymbol[String])
			}
			it("selecting the integer-typed columns") {
				runPropSelect[Integer](flightDf, FlightState.nameIndexMap, FlightState.nameTypeMap, logicPropSelectByColSymbol[Integer])
			}
		}
	}


}


/*val colByOverallCollect: Seq[Long] = flightDf.collect().toSeq.map(row => row.getAs[Long](F.C3))
val colByNameSelect: Seq[Long] = flightDf.select($"count").collect().toSeq.map(row => row.getAs[Long](0)) // use simple id = 0 because already selecting one column so the row will have length = 1

val lenRowByOverallCollect: Int = flightDf.collect().head.size
val lenRowByNameSelect: Int = flightDf.select($"count").collect().head.size

colByNameSelect shouldBe a[Seq[Long]]
colByOverallCollect shouldBe a[Seq[Long]]
colByNameSelect should equal(colByOverallCollect)

lenRowByOverallCollect should equal(flightDf.columns.length)
lenRowByNameSelect should equal(1)
lenRowByOverallCollect should be >= lenRowByNameSelect*/