package utilities

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}



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


		type EnumString = String
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
	 * Gets the names from a nested schema, e.g. if schema has name(a,b,c) nested then it returns Seq[name, a, b, c...]
	 * Example: try with dfNested from sparkbyexamples data in DataHub.
	 * @param schema
	 * @return
	 */
	def getNestedSchemaNames(schema: StructType): Seq[String] = {

		def helper(acc: Seq[String], rest: Seq[StructField]): Seq[String] = {
			if (rest.isEmpty) acc
			// checking if nested
			else if (rest.head.dataType.isInstanceOf[StructType]) helper(acc ++ Seq(rest.head.name) ++ getNestedSchemaNames(rest.head.dataType.asInstanceOf[StructType]), rest.tail)
			else helper(acc :+ rest.head.name, rest.tail)
		}

		helper(Seq.empty[String], schema.fields)
	}


	/**
	 * Renames a dataframe columns when there is also nesting, given the desired schema of the renamed df
	 * Works for non-nested df.
	 */
	def renameNestedDfByFold(df: DataFrame, sch: StructType): DataFrame = {

		val oldNamesPairNewFields: Seq[(String, StructField)] = df.columns.zip(sch.fields)

		val renameDf: DataFrame = oldNamesPairNewFields.foldLeft(df) {

			// NOTE cannot use withColumn in non-nested cases, doesn't rename in-place when not nested
			case (accDf, (oldName, structField)) => structField.dataType match {
				case _: StructType => accDf.withColumn(structField.name, col(oldName).cast(structField.dataType))
				case _ => accDf.withColumnRenamed(oldName, structField.name) // no need to cast
			}
		}
		renameDf
	}

	/**
	 * Rename just non-nested df using fold
	 * Does not work for nested df.
	 * @param df
	 * @param newCols
	 * @return
	 */
	def renameDfByFold(df: DataFrame, newCols: Seq[NameOfCol]): DataFrame = {
		require(newCols.length == df.columns.length)

		val oldNewNamePairs: Array[(NameOfCol, NameOfCol)] = df.columns.zip(newCols)

		val renameDf: DataFrame = oldNewNamePairs.foldLeft(df) {
			case (accDf, (oldName, newName)) => accDf.withColumnRenamed(oldName, newName)
		}
		renameDf
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





	/**
	 * Gets the non-null items from the colnames in the colnamelist and sets those into separate column that is a list containing all those non-null items.
	 *
	 * NOTE: this function was born int he context of the ArtistDf, to take the non-null skills and put them in a list-col alongisde original df.
	 *
	 * @param df            = the current dataframe
	 * @param colsToGetFrom = the cols in the dataframe from which to extract items and place the non-null items into a list in the dataframe.
	 * @param lstColname    = the name of the column that contains the list of all non-null elements of `colsToGetFrom`
	 */
	def gatherNonNullsToListCol(df: DataFrame, colsToGetFrom: Seq[NameOfCol], lstColname: NameOfCol): DataFrame = {
		/*val fromColnames: Seq[NameOfCol] = List(Mathematician, Engineer, Architect, Botanist, Chemist, Geologist, Doctor, Physicist, Painter, Sculptor, Musician, Dancer, Singer, Actor, Designer, Inventor, Producer, Director, Writer, Linguist).enumNames*/
		val fromCols: Seq[Column] = colsToGetFrom.map(col(_))

		val GATHER_NAME: NameOfCol = "Group"
		//val SKILL_COL_NAME: NameOfCol = "ListOfSkills"

		(df.withColumn(GATHER_NAME, array(fromCols: _*))
			.withColumn(lstColname, array_remove(col(GATHER_NAME), "null"))
			.drop(GATHER_NAME)
			.drop(colsToGetFrom: _*)) // removing the null-containing columns
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


	/**
	 *
	 * This function casts the given column to the specified type, where the specified types are given by the user as DataType and String types.
	 * NOTE: must pass in type T just like you pass in the other dataType, and strDataTypes (list); type T is the scala equivalent type of the passed DataType
	 *
	 * @param originalDf
	 * @param colnameToCast
	 * @param dataType
	 * @param strDataTypes
	 * @tparam T
	 * @return
	 */
	def columnCastResultIs[T: TypeTag](originalDf: DataFrame, colnameToCast: String, dataType: DataType, strDataTypes: Seq[String]): Boolean = {

		// TODO how to assert that dataType is equivalent to scala type T?
		require(strDataTypes.length == 2) // e.g. list("string", "String") or list("byte", "Byte") etc

		val dfCastedByDataType: DataFrame = originalDf.withColumn(colnameToCast, col(colnameToCast).cast(dataType))

		val dfListCastedByStringType: Seq[DataFrame] = strDataTypes.map(tpe =>
			originalDf.withColumn(colnameToCast, col(colnameToCast).cast(tpe))
		)

		val allCastedDfs: Seq[DataFrame] = dfCastedByDataType +: dfListCastedByStringType

		import implicits._

		def verifyCasting(df: DataFrame): Boolean = (df.schema(colnameToCast).dataType == dataType) &&
			df.select(colnameToCast).collectCol[T].isInstanceOf[Seq[T]]

		allCastedDfs.forall(verifyCasting(_))

		/*val cp = new Checkpoint
		//cp {allCastedDfs.map(df => df.schema(colnameToCast).dataType shouldEqual dataType) }
		/*val res: Seq[Unit] = */ allCastedDfs.map(df =>
			cp {
				df.schema(colnameToCast).dataType shouldEqual dataType
			}
		)
		allCastedDfs.map(df =>
			cp {
				df.select(colnameToCast).collectCol[T] shouldBe a[Seq[T]]
			}
		)
		cp.reportAll()*/
	}


	/**
	 * SOURCE = https://hyp.is/kHU__NcUEe6POQOR86ELCA/sparkbyexamples.com/spark/spark-flatten-nested-struct-column/
	 *
	 * Flattens a nested schema
	 *
	 * @param schema
	 * @param prefix
	 * @return
	 */
	def flattenStructSchema(schema: StructType, prefix: String = null): Array[Column] = {
		schema.fields.flatMap(f => {

			// Get colname of this field
			val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

			// Get data type of this field - if structtype, then must get its top name and set it as prefix so can rename the col with '.' (signify flattening)
			f.dataType match {
				case st: StructType => {
					println(s"st.getClass = ${st.getClass.getSimpleName} | colname simple = $columnName")
					flattenStructSchema(st, columnName)
				}
				case other => {
					println(s"other.getClass = ${other.getClass.getSimpleName} | colname here to see why replace . with _ = $columnName")
					Array(col(columnName).as(columnName.replace(".", "_")))
				}
			}
		})
	}


	/*def imposeSchema(df: DataFrame, targetSchema: StructType): DataFrame = {
		// NOTE: can use this technique then in step 2 do copy nullable trick:

		// TODO why does this fail for date type?
		// NOTE: using sqlContext isntead of spark object keeps the nullability (a.k.a all properties of the schema)
		val df_correctSchema = df.sqlContext.createDataFrame(df.rdd, targetSchema)
		//val df_final: DataFrame = df.select(exprs: _*)
		df_correctSchema
	}*/

	def imposeSchema(df: DataFrame, targetSchema: StructType): DataFrame = {

		/**
		 * SOURCE:
		 * 	- https://stackoverflow.com/a/38353335
		 * 	- https://hyp.is/D_aVYuenEe6suksPwDVcEw/medium.com/@pradipsudo/explore-nullable-property-of-columns-in-a-spark-data-frame-1d1b7b042adb
		 */

		// WARNING this expressions step doesn't work when any element is of ArrayType, gives conversion error ... ahd to just copy the targetSchema when creating the second df.
		// Step 1: create the expressions
		/*def makeExprs(df: DataFrame, st: StructType): Array[Column] = {
			def helper(acc: Array[Column], fields: Array[StructField]): Array[Column] = {

				if (fields.isEmpty) acc
				else {
					fields.head match {
						case StructField(fname, ftpe, fnlb, fmt) => {
							val newitem = if (df.schema.names.contains(fname)) col(fname).cast(ftpe) else lit(null).cast(ftpe).alias(fname)
							helper(acc :+ newitem, fields.tail)
						}
						//case inner: StructType => acc ++ makeExprs(inner) // TODO this won't keep nesting. Make acc be of type Seq[T] where T <: List[Column] ???
					}
				}
			}

			helper(Array.empty[Column], st.fields)
		}

		val exprs: Array[Column] = makeExprs(df, targetSchema)
		val resultDf1: DataFrame = df.select(exprs: _*)*/

		// Step 2: set nullability to be the same as in the target schema
		val resultDf2: DataFrame = df.sqlContext.createDataFrame(df.rdd, schema = targetSchema)
		/*val resultDf2: DataFrame = resultDf1.sqlContext.createDataFrame(resultDf1.rdd, schema =
			StructType(resultDf1.schema.zip(targetSchema).map { case (rf, tf) => rf.copy(nullable = tf.nullable) })
		)*/
		resultDf2
	}



	// TODO left off here
	/*def makeCaseClassObjFromRow[C: TypeTag](row: Row): C = {

		// step 1: get types of the parameters of the case class
		// step 2: extract the elements from the row (these are the class args) using the types of the args (getAs[T])
		// step 3: use 'instantiateClass' function from generalmainutils to instantiate the class, after passing the parameters.
	}*/




	object implicits {

		implicit class RowOps(row: Row) {
			def mapRowStr: Row = Row(row.toSeq.map(_.toString):_* )
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

			def collectSeqCol[T: TypeTag]: Seq[Seq[T]] = {
				require(df.columns.length == 1)

				df.collect().toSeq.map(row => row.getSeq[T](0))
			}

			def collectMapCol[K: TypeTag, V: TypeTag]: Seq[collection.Map[K, V]] = {
				require(df.columns.length == 1)

				df.collect().toSeq.map(row => row.getMap[K, V](0))
			}
			/**
			 * When E is EnumEntry then cannot cast the dataframe String to EnumEntry so must do this the manual way
			 */

			import enumeratum._

			import utilities.EnumUtils.implicits._
			import scala.reflect.runtime._
			import scala.tools.reflect.ToolBox
			import utilities.EnumUtils.Helpers._

			/**
			 * Treats EnumEntry like Enum[EnumEntry] so can convert String => EnumEntry using the Enum's withName() function.
			 * @param tt
			 * @tparam Y
			 * @return
			 */
			def collectEnumCol/*[E <: Enum[Y]]*/ [Y <: EnumEntry](implicit tt: TypeTag[Y]): Seq[Y] = {

				require(df.columns.length == 1)

				val cm: universe.Mirror = universe.runtimeMirror(getClass.getClassLoader)
				val tb: ToolBox[universe.type] = cm.mkToolBox()

				// Step 1: first select the column from the data frame as Seq[String] to be able to cast it later.
				val enumStrCol: Seq[EnumString] = df.collect.toSeq.map(row => row.getAs[String](0))

				// Step 2: parse the strings to enums
				enumStrCol.map { (estr: EnumString) =>
					//println(s"code string = $funcEnumStrToCode")
					//println(s"code string(e) = ${funcEnumStrToCode(estr)}")
					val result: Y = funcCodeToResultY[Y](tb)(funcEnumStrToCode[Y](estr))
					//println(s"code string(e) - evaluated = ${result}")
					result
				}
			}

			// TODO do the same for Case Classes

			def collectSeqEnumCol[Y <: EnumEntry](implicit tt: TypeTag[Y]): Seq[Seq[Y]] = {
				require(df.columns.length == 1)

				val cm: universe.Mirror = universe.runtimeMirror(getClass.getClassLoader)
				val tb: ToolBox[universe.type] = cm.mkToolBox()

				// Step 1: first select the colum to be Seq[String] to cast the strings to enums later
				val enumSeqStrCol: Seq[Seq[EnumString]] = df.collect().toSeq.map(row => row.getSeq[String](0))

				// Step 2: parse the strings to enums
				enumSeqStrCol.map { (seq: Seq[EnumString]) => seq.map { (estr: EnumString) =>
					val result: Y = funcCodeToResultY[Y](tb)(funcEnumStrToCode[Y](estr))
					result
				}}
			}


			def collectAll: Seq[Row] = {
				require(df.columns.length >= 1)
				df.collect().toSeq
			}

			// Converts rows to string
			def collectAllStr: Seq[String] = {
				require(df.columns.length >= 1)
				df.collect().toSeq.map(_.toString)
			}


			/**
			 * Attaches leftdf to right df even when cols are not the same, ignores cols that are the same
			 *
			 * SOURCES:
			 * 	- https://copyprogramming.com/howto/how-to-concatenate-append-multiple-spark-dataframes-column-wise-in-pyspark
			 * 	- https://hadoopist.wordpress.com/2016/05/24/generate-unique-ids-for-each-rows-in-a-spark-dataframe/
			 *
			 * @param rightDf
			 * @return
			 */
			def appendDf(rightDf: DataFrame): DataFrame = {
				val mdf1 = df.withColumn("row_id", monotonically_increasing_id())
				val mdf2 = rightDf.withColumn("row_id", monotonically_increasing_id())

				mdf1.join(mdf2, "row_id").drop("row_id") // result
			}
			// TODO handle when cols are the same? (like unionbyname)


			/**
			 * Applies the targetSchema to this df, outputting another df with the scpeified schema
			 * @param targetSchema
			 * @return
			 */
			def imposeSchema(targetSchema: StructType): DataFrame = {

				DFUtils.imposeSchema(df, targetSchema)
			}
		}


		// Converts Seq[Row] -> Seq[String] for easier comparison in testing else comparing rows from a dataframe to rows that are made on  the fly does not work ... why?
		implicit class SeqRowOps(seq: Seq[Row]) {
			def rowsAsString: Seq[String] = seq.map(_.toString)
		}

	}
}
