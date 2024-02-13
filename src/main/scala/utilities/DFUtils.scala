package utilities

import com.data.util.DataHub.ManualDataFrames.fromEnums.EnumString
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}

import scala.reflect.runtime.universe


//import util.DataFrameCheckUtils._
import org.apache.spark.sql.expressions.{Window, WindowSpec}

// Sinec scala 2.13 need to use this other import instead: https://stackoverflow.com/a/6357299
//import scala.jdk.CollectionConverters._
import scala.collection.JavaConverters._
//import scala.collection.JavaConversions._


import scala.reflect.runtime.universe._



/**
 *
 */
object DFUtils extends SparkSessionWrapper {


	object TypeAbstractions {


		type NameOfCol = String
		type TypenameOfCol = String
	}
	import TypeAbstractions._


	import sparkSessionWrapper.implicits._

	val dts: List[DataType] = List(IntegerType, StringType, BooleanType, DoubleType)
	val sts: List[String] = List("Int", "String", "Boolean", "Double")
	val dataTypeToScalaType: Map[DataType, String] = dts.zip(sts).toMap

	// Gets the column as type relating to the type it has already (e.g. if col type is "IntegerType" then return
	// List[Int] -- C is Int, or if coltype is "StringType" then return "String" -- C is String)
	// NOTE: idea of this function (relative to the other one below with typetag) is not NOT HAVE TO pass a scala
	//  primitve type in the brackets, to just have it infer automatically with below code
	/*def getCol[C](df: DataFrame, colname: String): List[C] = {

		// getting datatype at the column (e.g. "IntegerType")
		val colDataType: DataType = typeOfColumn(df, colname)

		// Use the above map that converts from DataType to ordinary scala type
		val convType: String = dataTypeToScalaType.get(colDataType).get // out of option
		// HELP cannot cast because it is in String format + also not known until runtime

		// Converting here manually (cannot automate this process to put arbitrary type in the brackets because it
		// won't work to pass it even with typetag
		convType match {
			case "String" => df.select(colname).collect.map(row => row.getAs[String](0)).toList.asInstanceOf[List[C]]
			case "Int" => 	df.select(colname).collect.map(row => row.getAs[Int](0)).toList.asInstanceOf[List[C]]
			case "Double" => 	df.select(colname).collect.map(row => row.getAs[Double](0)).toList.asInstanceOf[List[C]]
			case "Boolean" => 	df.select(colname).collect.map(row => row.getAs[Boolean](0)).toList.asInstanceOf[List[C]]
		}
	}*/

	def colnamesToIndices(df: DataFrame): Map[NameOfCol, Int] = {
		df.schema.fieldNames.zipWithIndex.toMap
	}

	def colnamesToDataTypes(df: DataFrame): Map[NameOfCol, DataType] = {
		// Convert the datatypes to string
		val ts = df.schema.fields.map(_.dataType)
		// get the colnames
		val ns = df.schema.fieldNames

		ns.zip(ts).toMap
	}

	def colnamesToTypes(df: DataFrame): Map[NameOfCol, TypenameOfCol] = {
		// Convert the datatypes to string
		val ts = df.schema.fields.map((f: StructField) => DFUtils.dataTypeToStrName(f.dataType))
		// get the colnames
		val ns = df.schema.fieldNames

		ns.zip(ts).toMap
	}


	/**
	 * Get only the names of the columns which are the given type
	 *
	 * WARNING: the type T must be passed explicitly
	 *
	 * @param df
	 * @return
	 */
	def getColnamesWithType[T: TypeTag](df: DataFrame): Seq[String] = {

		val givenType: String = typeOf[T].toString

		val ns: Array[String] = df.schema.fieldNames
		val ts: Array[DataType] = df.schema.fields.map(_.dataType)

		ns.zip(ts).filter { case (n, dtpe) => dataTypeToStrName(dtpe) == givenType }.unzip._1
	}

	def getColnamesWithType[T: TypeTag](dfSchema: StructType): Seq[String] = {

		val givenType: String = typeOf[T].toString

		val ns: Array[String] = dfSchema.fieldNames
		val ts: Array[DataType] = dfSchema.fields.map(_.dataType)

		ns.zip(ts).filter { case (n, dtpe) => dataTypeToStrName(dtpe) == givenType }.unzip._1
	}

	def dataTypeToStrName(d: DataType) = d.toString.replace("Type", "")



	// TODO figure out difference between simple way and _complicated way of getting column (below)
	def getColAsType[T: TypeTag](df: DataFrame, colname: String): Seq[T] = {
		df.select(colname).collect().toSeq.map(row => row.getAs[T](0))
	}


	// Another way to convert the column to the given type; collecting first - actually faster!
	// NOTE; also handling case where conversion is not suitable (like int on "name")
	def getColAs_complicated[A: TypeTag](df: DataFrame, colname: String): List[Option[A]] = {

		// STEP 1 = try to check the col type can be converted, verifying with current schema col type
		val typesStr: List[String] = List(IntegerType, StringType, BooleanType, DoubleType).map(_.toString)
		val givenType: String = typeOf[A].toString
		//val castType: String = typesStr.filter(t => t.contains(givenType)).head
		val curColType: String = df.schema.fields.filter(_.name == colname).head.dataType.toString

		// CHECK 1 = if can convert the coltype
		/*val check1: Boolean = curColType.contains(givenType)*/
		// if not, then the schema doesn't have same type as
		// desired  type SO may not be able to convert

		// Getting the column from the df (now thesulting df will contain the column converted to the right
		// DataType when passing the scala type here, e.g. if givenType is Int, resulting "emp_dept_id" col will be
		// IntegerType when it was StringType before)
		val dfWithConvertedCol: DataFrame = df.withColumn(colname, col(colname).cast(givenType))


		// CHECK 2 = if can convert the coltype - if all are null then it means the conversion was not suitable
		/*val check2: Boolean = dfWithConvertedCol.select(colname).collect.forall(row => row(0) == null)

		val canConvert: Boolean = check1 && (! check2)*/

		// Choice 1 = leave the list unconverted List[Any] or
		// Choice 2 = make List[Option[A]]
		// Either way, can still access elements and compare xs(i) with an element of type A so no need for
		//conversion anyway
		// Doing choice 1
		/*dfWithConvertedCol.select(colname)
			.collect
			.map(row => row(0))
			.toList */// leaves as List[Any]

		// Doing choice 2
		dfWithConvertedCol.select(colname)
			.collect
			.map(row => row(0))
			.toList // leaves as List[Any]
			.map(Option(_))
			.asInstanceOf[List[Option[A]]]

		/*canConvert match {
			case false => {
				dfWithConvertedCol.select(colname)
					.collect
					.map(row => row(0))
					.toList.asInstanceOf[List[A]]
			}// return empty list as sign of graceful error // TODO assert here result is empty! results in error
			// otherwise
			case true => {
				dfWithConvertedCol.select(colname)
					.collect
					.map(row => row.getAs[A](0))
					.toList
				// TODO problem - for type A = Int, this returns 0 when element is 'null' and don't want that. However, the 'null' remains when using 'case false'
			}
		}*/
	}

	def getCols(df: DataFrame): Seq[Seq[Any]] = {
		df.columns.map(colname => df.select(colname).collect.map(r => r(0)).toList)
	}


	def has[T](df: DataFrame, colname: String, valueToCheck: T): Boolean = {
		val res: Array[Row] = df.filter(df.col(colname).contains(valueToCheck)).collect()
		! res.isEmpty
	}


	def typeOfColumn(df: DataFrame, colname: String): DataType = {
		df.schema.fields.filter(_.name == colname).head.dataType
	}

	// Gets the row that corresponds to the given value under the given column name
	// -- targetValue: if None represents null, else Some(v) represents the value v in the df.
	def rowsAt[A: TypeTag](df: DataFrame, colname: String, targetValue: Option[A]): List[Row] = {

		// NOTE - Instead use: df.where(df.col(name) == value)).collect.toList

		val colWithTheValue: List[Option[A]] = getColAs_complicated[A](df, colname)

		val rows: Array[Row] = df.collect()

		val targetRows: List[Row] = rows.zip(colWithTheValue).toList
			.filter{ case(row, optValue) => optValue == targetValue}
			.map(_._1)

		targetRows
	}


	def numMismatchCases[T: TypeTag](dfLeft: DataFrame, dfRight: DataFrame,
							   colnameLeft: String, colnameRight: String): (Int, Int) = {

		val colLeft: List[Option[T]] = getColAs_complicated[T](dfLeft, colnameLeft)
		val colRight: List[Option[T]] = getColAs_complicated[T](dfRight, colnameRight)
		val diffLeftVersusRight: Set[Option[T]] = colLeft.toSet.diff(colRight.toSet)
		val diffRightVersusLeft: Set[Option[T]] = colRight.toSet.diff(colLeft.toSet)

		// Count number of times each different element appears in the respective column of each df
		/*val numDiffsA: List[(T, Int)] = setDiffAToB.toList.map(setElem =>
			(setElem, colA.count(colElem => colElem	== setElem)))
		val numDiffsB: List[(T, Int)] = setDiffBToA.toList.map(setElem =>
			(setElem, colB.count(colElem => colElem	== setElem)))*/

		// Count num cols and num rows of the join data sets
		val numColOfJoinedDFs: Int = dfLeft.columns.length + dfRight.columns.length

		val numRowOfInnerJoinDF: Int = colLeft.toSet.intersect(colRight.toSet).toList.map(setElem =>
			(setElem, colLeft.count(colElem => colElem == setElem))
		).unzip._2.sum

		val numMismatchOuterJoin: Int = (diffLeftVersusRight ++ diffRightVersusLeft).toList.map(diffElem =>
			(diffElem, (colLeft ++ colRight).count(colElem => colElem == diffElem))
		).unzip._2.sum


		val numRowOfOuterJoinDF = numRowOfInnerJoinDF + numMismatchOuterJoin

		// Return pair of num matching rows and number of mismatched rows
		(numRowOfInnerJoinDF, numRowOfOuterJoinDF)
	}

	// Type A = the type that the columns are converted to when joining dataframes (IntegerType -> Int)
	// GOAL: get the rows that don't match in the outer join with respect to dfA (_._1) and dfB (_._2)
	def getMismatchRows[T: TypeTag](dfLeft: DataFrame, dfRight: DataFrame,
							  colnameLeft: String, colnameRight: String): (List[Row], List[Row]) = {

		val colLeft: List[Option[T]] = getColAs_complicated[T](dfLeft, colnameLeft)
		val colRight: List[Option[T]] = getColAs_complicated[T](dfRight, colnameRight)
		val diffLeftVersusRight: Set[Option[T]] = colLeft.toSet.diff(colRight.toSet)
		val diffRightVersusLeft: Set[Option[T]] = colRight.toSet.diff(colLeft.toSet)

		// Get all the rows (per df) that contain the different element (the non-matching element, respective to
		// each df)
		// TODO fix rowsAt to handle option
		val rowDiffLeft: List[Row] = diffLeftVersusRight.toList.flatMap(diffedElem => rowsAt[T](dfLeft, colnameLeft, diffedElem))
		val rowDiffRight: List[Row] = diffRightVersusLeft.toList.flatMap(diffedElem => rowsAt[T](dfRight, colnameRight, diffedElem))

		// Now fill each with null (first df has nulls at the end, since dfB is added after, while dfB's row has
		// nulls at beginning, to accomodate values from dfA) since when join, it is dfA + dfB not dfB + dfA
		val nulledRowsLeft: List[Seq[Any]] = rowDiffLeft.map(row => row.toSeq ++ Seq.fill(dfRight.columns.length)(null) )
		val nulledRowsRight: List[Seq[Any]] = rowDiffRight.map(row => Seq.fill(dfLeft.columns.length)(null) ++ row.toSeq)

		// First _._1 = the rows that don't match in the outer join, for dfA relative to dfB
		// Second _._2 = the rows that don't match in the outer join, for dfB relative to dfA
		val rowsMismatchLeft: List[Row] = nulledRowsLeft.map(Row(_:_*))
		val rowsMismatchRight: List[Row] = nulledRowsRight.map(Row(_:_*))

		assert(rowsMismatchLeft.map(row => row.toSeq.takeRight(dfRight.columns.length).forall(_ == null)).forall(_ == true),
			"Test: Last elements in the row that don't match are always null (first df relative to second)"
		)
		assert(rowsMismatchRight.map(row => row.toSeq.take(dfLeft.columns.length).forall(_ == null)).forall(_ == true),
			"Test: first elements in the row that don't match are always null (second df relative to first"
		)

		(rowsMismatchLeft, rowsMismatchRight)
	}



	// ---------------------------------------------------------------------

	/**
	 *
	 * @param leftColOuter a single column from the leftDF side of a particular kind of outer join (outer / left
	 *                     outer / right outer joins)
	 * @param rightColOuter a single column from the rightDF side of a particular kind of outer join (left outer /
	 *                      right outer / outer join)
	 * @param rightDF DF that is joined on the right side when the outer join is created
	 * @param anOuterJoin a kind of outer join dataframe (left outer / outer / right outer join)
	 * @tparam T the type that the column should be, when extracted already from the outer join df
	 * @return
	 */
	def recordNonNullSpotsColumnwise[T](leftColOuter: List[Option[T]],
								 rightColOuter: List[Option[T]],
								 rightDF: DataFrame,
								 anOuterJoin: DataFrame
								): Array[List[List[Boolean]]] = {

		// For each common element between left and right df (for this column), get that common element out of the
		// left df column, then get that common element's INDEX.
		val iCommonsLeftToRight: List[List[Int]] = leftColOuter.toSet.intersect(rightColOuter.toSet).toList.filter(_!= None).map(commElem =>leftColOuter.zipWithIndex.filter{ case (elem, i) => elem == commElem}.unzip._2)

		// From the outer join, get the columns corresponding to the right df side (just to have the nulls from the
		// outer join)
		val rightColsFromOuterJoin: Array[List[Any]] = rightDF.columns.map(colNameStr => anOuterJoin.select(colNameStr).collect.map(row => row(0)).toList)

		// For each right df column, and for each common elem index between left and right df columns check that
		// the element at that position in the right df column is NOT null.
		// (e.g. elem 20 is same in leftvsright --> occurs at index i = 1 columnwise in the left col ---> check that
		//  in right df there is NO NULL at i = 1)
		rightColsFromOuterJoin.map(colList => iCommonsLeftToRight.map(commonIndexList => commonIndexList.map(i => colList(i) !=
			null)))

	}



	/**
	 *
	 * @param leftColOuter a single column from the leftDF side of the an outer join (right / left / or simple outer join)
	 * @param rightColOuter a single column from the rightDF side of an outer join (right / left / or simple outer join)
	 * @param rightDF DF that is joined on the right side when the outer join is created
	 * @param anOuterJoin any kind of outer join (right / left / or simple outer join)
	 * @tparam T the type that the column should be, when extracted already from the anOuterJoin
	 * @return
	 */
	def recordNullSpotsColumnwise[T](leftColOuter: List[Option[T]],
							   rightColOuter: List[Option[T]],
							   rightDF: DataFrame,
							   anOuterJoin: DataFrame
							  ): Array[List[List[Boolean]]] = {

		// indices corresponding to different elems from left df vs. right df
		val iDiffsLeftToRight: List[List[Int]] = leftColOuter.toSet.diff(rightColOuter.toSet).toList
			.filter(_ != None)
			.map(diffElem => leftColOuter.zipWithIndex.filter{ case(elem, i) => elem == diffElem}.unzip._2)

		// Get cols corresponding to right df from the outer join (to have the nulls from oute rjoin)
		val rightColsFromTheOuterJoin: Array[List[Any]] = rightDF.columns.map(colNameStr => anOuterJoin.select(colNameStr).collect.map(row => row(0)).toList)

		// For each different elem from leftdf vs right df (now recorded as index corresponding to that elem, check
		// that corresponding position in the other df contains a null
		// (e.g. elem 50 is diff in leftvsright --> occurs at index i = 5 columnwise ---> check that in right df
		//  there is null at i = 5)
		rightColsFromTheOuterJoin.map(colList => iDiffsLeftToRight.map(diffIndexList => diffIndexList.map(i => colList(i) == null)))
	}




	/// ----------------------

	// Implement rank() sql window function, manually

	def manualRanker(df: DataFrame, dropCols: Seq[String], viewCols: Seq[String]) = {

		val windowPartOrdSpec: WindowSpec = Window.partitionBy(viewCols(0)).orderBy(viewCols(1))

		val rowNumberDf: DataFrame = df.withColumn("RowNum", row_number().over(windowPartOrdSpec)).drop(dropCols: _*)

		val tupsInOrder = rowNumberDf.select($"*").collect().toSeq.map(row => row.toSeq match {
			case Seq(firstCol, secCol, id) => (firstCol, secCol, id).asInstanceOf[(String, Integer, Integer)]
		})

		def groupMid(lst: List[(String, Integer, Integer)]): Map[Integer, List[(String, Integer, Integer)]] = lst.groupBy { case (first, mid, id) => mid } // .values.map(_.toList)

		val tupsMid: List[List[(String, Integer, Integer)]] = tupsInOrder
			.groupBy { case (first, mid, id) => first }
			.values.map(_.toList).toList
			.map(groupMid(_).values.toList).flatten

		import scala.collection.mutable.ListBuffer

		val buf: ListBuffer[Integer] = ListBuffer()

		def getNewId(lst: List[(String, Integer, Integer)]) = {
			buf += lst.head._3;
			lst.head._3
		}

		tupsMid.map(lst => lst.length match {
			case n if n > 1 => {
				val newId = getNewId(lst)
				lst.map { case (first, mid, id) => (first, mid, newId) }
			}
			case _ => lst

		})

	}


	// -------------------------

	/**
	 * Creates a schema that we can pass in to create a dataframe (liek for session's createDataFrame)
	 * @param names
	 * @param tpes
	 */
	def createSchema(names: Seq[String], tpes: Seq[DataType]) = {
		StructType(
			names.zip(tpes).map{ case (n, t) => StructField(n, t)}
		)
		// Or with fold:
		//names.zip(types).foldLeft(new StructType()){ case (accStruct, (n, t)) => accStruct.add(n, t)}
	}


	object implicits {

		implicit class RowOps(row: Row) {
			def mapRowStr: Seq[String] = row.toSeq.map(_.toString)
		}


		implicit class DFOps(df: DataFrame) {
			/**
			 * Collects the element in the row, assert only one element in the row from this one-column df
			 * Usage: to collect the single-col into Seq after doing a select() operation which outputs column with row of size 1
			 *
			 * @tparam T = the type to which you want to convert the value inside the Rows.
			 */
			def collectCol[T: TypeTag]: Seq[T] = {
				require(df.columns.length == 1)

				df.collect().toSeq.map(row => row.getAs[T](0))
			}

			/**
			 * When E is EnumEntry then cannot cast the dataframe String to EnumEntry so must do this the manual way
			 */

			import enumeratum._

			import utilities.EnumUtils.implicits._
			import scala.reflect.runtime._
			import scala.tools.reflect.ToolBox
			/**
			 * Treats EnumEntry like Enum[EnumEntry] so can convert String => EnumEntry using the Enum's withName() function.
			 * @param tt
			 * @tparam Y
			 * @return
			 */
			def collectEnumCol/*[E <: Enum[Y]]*/ [Y <: EnumEntry](implicit tt: TypeTag[Y]): Seq[Y] = {

				val cm: universe.Mirror = universe.runtimeMirror(getClass.getClassLoader)
				val tb: ToolBox[universe.type] = cm.mkToolBox()


				require(df.columns.length == 1)

				// Step 1: first select the column from the data frame as Seq[String] to be able to cast it later.
				val enumStrCol: Seq[EnumString] = df.collect.toSeq.map(row => row.getAs[String](0))

				type CodeString = String

				/**
				 * Key Hacky Strategy: Treating Y = EnumEntry like an E = Enum[Y] so can call the withName method that exists only for Enum[Y]. If not doing this then have to pass both as type parameters within the function like so:
				 * e.g. collectCol[Y, E](obj: E)
				 * and that looks ugly and too stuffy when calling the function,
				 * e.g. collectCol[Animal, Animal.type](Animal)
				 */
				// NOTE: the withName() function returns type ENumEntry anyway so no need to worry of converting the result to Enum[Y], which would have type Animal.Fox.type instead of the type here Animal.Fox
				val funcEnumStrToCode: EnumString => CodeString = enumStr =>
					s"""
					   |import com.data.util.EnumHub._
					   |import com.data.util.EnumHub.Hemisphere._
					   |import enumeratum._
					   |import scala.reflect.runtime.universe._
					   |
					   |${parentEnumTypeName[Y]}.withName("$enumStr")
					   |""".stripMargin

				val funcCodeToEnumEntry: CodeString => Y = codeStr => tb.eval(tb.parse(codeStr)).asInstanceOf[Y]

				//tb.eval(tb.parse(thecode)).asInstanceOf[Y]
				enumStrCol.map { (estr: EnumString) =>
					//println(s"code string = $funcEnumStrToCode")
					//println(s"code string(e) = ${funcEnumStrToCode(estr)}")
					val result: Y = funcCodeToEnumEntry(funcEnumStrToCode(estr))
					//println(s"code string(e) - evaluated = ${result}")
					result
				}
			}

			def collectAll: Seq[Row] = {
				require(df.columns.length >= 1)
				df.collect().toSeq
			}
		}
	}
}
