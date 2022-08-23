package com.sparkscalaexamples.SQLTutorial

/**
 *
 */
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.catalyst.plans._

import org.apache.spark.sql.types.{DataType, StringType, IntegerType, BooleanType, DoubleType, StructField, StructType}
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._


import util.DataFrameCheckUtils._

import SparkJoins._




object L16_JoinTypes extends App {


	val spark: SparkSession = SparkSession.builder()
		.master("local[1]")
		.appName("SparkByExamples.com")
		.getOrCreate()
	// for console
	//val spark: SparkSession = SparkSession.builder().master("local[1]").appName("SparkByExamples.com").getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")
	// TODO meaning?

	import spark.sqlContext.implicits._

	// Creating the employee data --------------------------------------------------------------------------
	val empData = Seq((1,"Smith",-1,"2018","10","M",3000),
		(2,"Rose",1,"2010","20","M",4000),
		(3,"Williams",1,"2010","10","M",1000),
		(4,"Jones",2,"2005","10","F",2000),
		(5,"Brown",2,"2010","40","",-1),
		(6,"Brown",2,"2010","50","",-1)
	)


	val empColNameTypePairs: Seq[(String, DataType)] = Seq(("emp_id", IntegerType), ("name", StringType),
		("superior_emp_id", IntegerType),	("year_joined", StringType), ("emp_dept_id", StringType),
		("gender", StringType), ("salary", IntegerType)
	)
	val empColnames: Seq[String] = empColNameTypePairs.unzip._1
		//Seq("emp_id","name","superior_emp_id","year_joined",	"emp_dept_id","gender","salary")

	val empDF_strCol: DataFrame = empData.toDF(empColnames:_*)
	empDF_strCol.show(truncate = false)


	val empRows: Seq[Row] = empData.map( tupleRow => Row( tupleRow.productIterator.toList:_* ))
	val empSchema: StructType = StructType(
		empColNameTypePairs.map{ case (title, tpe) => StructField(name = title, dataType = tpe, nullable = true)}
	)
	val empDF_strCol_FromSchema: DataFrame = spark.createDataFrame(empRows, empSchema)

	assert(typeOfColumn(empDF_strCol, "emp_dept_id") == StringType &&
		typeOfColumn(empDF_strCol_FromSchema, "emp_dept_id") == StringType,
		"Test: in _empDF, both column types must be StringType"
	)

	// Now convert the emp_dept_id to be integer type:
	val empDF_intCol: DataFrame = empDF_strCol.withColumn("emp_dept_id", col("emp_dept_id").cast(IntegerType))

	// Test that conversion of column type from string -> int worked
	assert(typeOfColumn(empDF_strCol, "emp_dept_id") == StringType &&  //was
		typeOfColumn(empDF_intCol, "emp_dept_id") == IntegerType, // is now
		"Test: in empDF, emp-dept-id is converted to IntegerType"
	)

	// TODO check what getColAs function returns -- need to make sure it doesn't accept unsuitable conversions.
	/*assert(typeOfColumn(empDF_strCol, "name") == StringType && getColAs[Int](empDF_strCol, "name").forall(_ == null),
		"Test: Changing coltype to unsuitable target type yields null list")

	assert(typeOfColumn(empDF_strCol, "name")  == StringType &&
		getColAs[Int](empDF_strCol, "emp_dept_id") == List(10,	20, 10, 10, 40, 50),
		"Test: Changing coltype to suitable target type yields desired int list")*/



	// leftdf should have numbers in "emp_dept_id" that are NOT found in the right df
	val empDataExtra = Seq(
		(7, "Layla", 3, "2030", "70", "F", 5000),
		(8, "Lobelia", 3, "2030", "80", "F", 5500),
		(9, "Linda", 4, "2030", "90", "F", 5050),
		(10, "Lisbeth", 4, "2030", "100", "F", 5005),
		(11, "Llewelyn", 4, "2030", "60", "F", 5555)
	)
	val empRowsExtra: DataFrame = empDataExtra.toDF(empColnames:_*)

	val empDFExtra_strCol: DataFrame = empDF_strCol.union(empRowsExtra)

	// Creating the department data --------------------------------------------------------------------------

	val deptData: Seq[(String, Int)] = Seq(("Finance",10),
		("Marketing",20),
		("Sales",30),
		("IT",40)
	)

	val deptColNameTypePairs: Seq[(String, DataType)] = Seq(("dept_name", StringType), ("dept_id", IntegerType))
	val deptColnames: Seq[String] = deptColNameTypePairs.map(_._1)
	val deptDF = deptData.toDF(deptColnames:_*)
	deptDF.show(truncate = false)


	val deptRows: Seq[Row] = deptData.map( tupleRow => Row( tupleRow.productIterator.toList:_* ))
	/*val deptSchema = StructType(Seq(StructField(name="dept_name", dataType=StringType), StructField(name="dept_id",
		dataType=IntegerType)))*/
	val deptSchema: StructType = StructType(
		deptColNameTypePairs.map{ case (title, tpe) => StructField(name = title, dataType = tpe, nullable = true)}
	)
	val deptDF_fromSchema: DataFrame = spark.createDataFrame(deptRows, deptSchema)


	assert(typeOfColumn(deptDF, "dept_id") == IntegerType &&
		typeOfColumn(deptDF_fromSchema, "dept_id") == IntegerType
	)



	// rightdf should have numbers in the "dept_id" that are NOT found in the left df
	val deptDataExtra = Seq(
		("Finance", 20),
		("Finance", 120),
		("Marketing", 130),
		("Sales", 140),
		("IT", 150),
		("ForexTrading", 140),
		("Investments", 130)
	)

	val deptRowsExtra: DataFrame = deptDataExtra.toDF(deptColnames:_*)

	val deptDFExtra: DataFrame = deptDF.union(deptRowsExtra)



	// TESTING
	//  Inner join Tests- use to match dataframes on KEY columns, and where KEYS don't match, the rows get dropped
	//  from both datasets

	// Testing if even if empdf has a string col, can conversion to int col and thsus comparison to deptdf, still
	// take place?
	val ij_convertStrColToInt = SparkJoins.InnerJoinSpecs[String, Int, Int](empDF_strCol, deptDF, "emp_dept_id",
		StringType,  "dept_id", IntegerType)
	ij_convertStrColToInt.testColumnAggregationForInnerJoin
	ij_convertStrColToInt.testColumnTypesForInnerJoin
	ij_convertStrColToInt.testIntersectedColumnsForInnerJoin

	val ij_keepColAsInt = InnerJoinSpecs[Int, Int, Int](empDF_intCol, deptDF, "emp_dept_id",
		IntegerType, "dept_id", IntegerType)
	ij_keepColAsInt.testColumnAggregationForInnerJoin
	ij_keepColAsInt.testColumnTypesForInnerJoin
	ij_keepColAsInt.testIntersectedColumnsForInnerJoin


	val ij_convertColToStr = SparkJoins.InnerJoinSpecs[String, Int, String](empDF_strCol, deptDF, "emp_dept_id",
		StringType, "dept_id", IntegerType)
	ij_convertColToStr.testColumnAggregationForInnerJoin
	ij_convertColToStr.testColumnTypesForInnerJoin
	ij_convertColToStr.testIntersectedColumnsForInnerJoin


	// --------------------

	// TESTING: outer join tests --- Outer join returns all rows from both dataframes, and where join expression
	//  doesn’t match it returns null on the respective record columns.

	val oj = SparkJoins.OuterJoinSpecs[String, Int, Int](empDFExtra_strCol, deptDF, "emp_dept_id", StringType,
		"dept_id", IntegerType)
	oj.testSamnessOfAllKindsOfOuterJoins


	oj.testIntersectedColumnsForOuterJoin
	oj.testColumnTypesForOuterJoin
	oj.testMismatchedRowsForOuterJoin

	oj.testDifferingRecordsHaveNullsInOuterJoin
	oj.testMatchingRecordsDontHaveNullsInOuterJoin
	oj.testOuterJoinKeepsAllLeftAndRightRecordsRegardlessOfMatch

	// ---------------------------------


	// TESTING  Left outer join returns all rows from the left dataframe / dataset regardless of
	//  the match found on the right data set; shows the null row componenets only where the left df doesn't match
	//  the right df (and drops records from right df where match wasn't found)
	// SIMPLE: just keeps the intersect + left differences, no right differences.
	val loj = SparkJoins.LeftOuterJoinSpecs[String, Int, Int](empDFExtra_strCol, deptDF, "emp_dept_id", StringType,
		"dept_id", IntegerType)

	loj.testSamnessOfAllKindsOfLeftOuterJoins

	loj.testIntersectedColumnsForLeftOuterJoin
	loj.testColumnTypesForLeftOuterJoin
	loj.testMismatchedRowsForLeftOuterJoin

	loj.testDifferingRecordsHaveNullsInLeftOuterJoin
	loj.testMatchingRecordsDontHaveNullsInLeftOuterJoin
	loj.testLeftOuterJoinKeepsAllLeftRecordsAndDropsDifferingRightRecords

	// TESTING: right outer joins --- Right Outer join is opposite of left join, here it returns all rows
	//  from the right DataFrame/Dataset regardless of match found on the left dataset.
	//  When join expression doesn’t  match, it assigns null for that record and drops records from left where match not found.
	val roj = SparkJoins.RightOuterJoinSpecs[String, Int, Int](empDFExtra_strCol, deptDF, "emp_dept_id", StringType,
		"dept_id", IntegerType)
	roj.testSamnessOfAllKindsOfRightOuterJoins

	roj.testIntersectedColumnsForRightOuterJoin
	roj.testColumnTypesForRightOuterJoin
	roj.testMismatchedRowsForRightOuterJoin

	roj.testDifferingRecordsHaveNullsInRightOuterJoin
	roj.testMatchingRecordsDontHaveNullsInRightOuterJoin
	roj.testRightOuterJoinKeepsAllRightRecordsAndDropsDifferingLeftRecords



	// TESTING: left semi joins
	// Left semi join is just like inner join, but drops the columns from the right dataframe, while keeping all the
	// columns from the left dataframe. Also, it only returns the left df's columns for which the records match.
	// NOTE: "leftsemi" == "semi"
	val lsj = SparkJoins.LeftSemiJoinSpecs[String, Int, Int](empDFExtra_strCol, deptDF, "emp_dept_id", StringType,
		"dept_id", IntegerType)

	lsj.testIntersectedColumnsForLeftSemiJoin
	lsj.testColumnTypesForLeftSemiJoin

	lsj.testLeftSemiJoinLacksRightDFColumns

	lsj.testNoMismatchedRowsInLeftSemiJoin.methodLeftSemiDoesNotEqualDiffsFromLeftVsRightDF
	lsj.testNoMismatchedRowsInLeftSemiJoin.methodLeftSemiDiffsAreEmpty
	lsj.testNoMismatchedRowsInLeftSemiJoin.methodAllRowsHaveNoNullsSinceOnlyLeftDFColsAreKept
	lsj.testNoMismatchedRowsInLeftSemiJoin.methodLeftSemiColIntersectingRightDFColIsLikeLeftDFCol
	lsj.testNoMismatchedRowsInLeftSemiJoin.methodLeftSemiIsDisjointFromDiffsOfLeftvsRightDFs



	val laj = SparkJoins.LeftAntiJoinSpecs[String, Int, Int](empDFExtra_strCol, deptDF, "empt_dept_id", StringType,
		"dept_id", IntegerType)

	laj.testColumnTypesForLeftAntiJoin
	laj.testIntersectedColumnsForLeftAntiJoin

	laj.testLeftAntiDropsRightDFColumns.methodLeftAntiLacksRightDFCols
	laj.testLeftAntiDropsRightDFColumns.methodLeftAntiColsEqualLeftDFCols

	laj.testLeftAntiJoinKeepsOnlyMismatchedRows.methodKeepingOnlyMismatchesFromLeftNotRight
	laj.testLeftAntiJoinKeepsOnlyMismatchedRows.methodNonEmptyLeftDiffs
	laj.testLeftAntiJoinKeepsOnlyMismatchedRows.methodLeftAntiPlusLeftSemiIsLeftOuter
	laj.testLeftAntiJoinKeepsOnlyMismatchedRows.methodAllRowsHaveNoNullsSinceOnlyLeftDFColsAreKept
	laj.testLeftAntiJoinKeepsOnlyMismatchedRows.methodLeftAntiMatchesDiffsFromLeftToRightDFs
	laj.testLeftAntiJoinKeepsOnlyMismatchedRows.methodRecordsFromLeftAntiHaveNothingInCommonWithRightDFRecords


	//TESTING: Left anti join -  Left-anti join is exact opposite of left semi join - it returns only the columns from the left dataframe for
	// non-matched records. Also, like leftSemiJoin, leftAntiJoin does not keep columns from the right df.
/*
	// Left-anti join is exact opposite of left semi join - it returns only the columns from the left dataframe for
	// non-matched records
	// NOTE: "leftanti" == "anti"
	val leftAntiJoin = empDF_intCol.join(deptDF, empDF_intCol("emp_dept_id") === deptDF("dept_id"), "leftanti")
	leftAntiJoin.show

	assert(leftAntiJoin.collect.zip(edRows).forall{
		case (leftAntiJoinRow, outerJoinRow) => leftAntiJoinRow.toSeq.toSet.subsetOf(outerJoinRow.toSeq.toSet)
	}, "Test: left anti join mismatch rows only show the non-matched rows from the left df, and doesn't fill it with" +
		" nulls to correspond to the unmatched columns in the right df, unlike the outer join")*/
}











//deptDF_.select("dept_id").collectAsList().get(0).getAs[Int](0)
//deptDF_.select("dept_id").collectAsList().toList.map(row => row.getAs[Int](0))
// TODO left off here
//deptdfcopy.withColumn("dept_id", col("dept_id").cast())
// TODO
/*def getTypedColVals(df: DataFrame, arg: Tuple2[String, DataType] ): Seq[DataType] = {
	val name: String = arg._1
	val dataType: DataType = arg._2.asInstanceOf[DataType]

	// todo erase this below one after making this abstract
	val name = "dept_id"
	val dfCopy = df.alias("copy")

	val mp = Map("IntegerType" -> "Integer", "StringType" -> "String", "BooleanType" -> "Boolean", "DoubleType"
		-> "Double")
	/*val mp2 = Map("IntegerType" -> Integer.valueOf(0), "StringType" -> String.valueOf(" "), "DoubleType" -> Double
		.valueOf(1.2), "BooleanType" -> )*/

	// TODO left off here
	//import scala.reflect.runtime.universe._
	//typeOf[Integer].getClass

	val keyType = dfCopy.schema.fields.filter(strct => strct.name == "dept_id").head.dataType.toString
	val castType = mp.get(keyType).get

	//df.withColumn("dept_id",col("dept_id").cast(castType))
	//dfCopy.withColumn(name, col(name).cast("Int"))
	//df.select(name).collectAsList().toList.map(row => row.getAs[dataType](0))
	dfCopy.select(name).collectAsList().toList.map(r => r(0)).asInstanceOf[List[Int]]


	// TODO left off here
	import scala.reflect.runtime.universe._

	val className = "java.lang.Integer" // (new Integer(0)).getClass.getName
	val any: Any = 10

	val mirror = runtimeMirror(getClass.getClassLoader)
	val classSymbol = mirror.staticClass(className)
	val typ = classSymbol.toType
	val idMethodSymbol = typ.decl(TermName("id")).asMethod
	val nameMethodSymbol = typ.decl(TermName("name")).asMethod
	val instanceMirror = mirror.reflect(any)
	val idMethodMirror = instanceMirror.reflectMethod(idMethodSymbol)
	val nameMethodMirror = instanceMirror.reflectMethod(nameMethodSymbol)

	instanceMirror.reflectClass(classSymbol)

	// TESTING
	val name = "dept_id"
	dfCopy.select(name).as[Int].collect.toList
	dfCopy.select(name).map(r => r.getInt(0))
	//df.select(col("label").cast(DoubleType)).map { case Row(label) => label.getClass.getName }.show(false)
	dfCopy.select(col(name).cast(IntegerType)).map { case Row(lab) => lab.getClass.getName}.show()

	import scala.reflect.api._
	import scala.reflect.runtime.universe._
	stringToTypeName("Integer")
	TypeName("String")

	// deptDF_.select("dept_id").collectAsList().toList.map(row => row.getAs[Integer](0))

	deptDF_.schema.filter(_.dataType.isInstanceOf[IntegerType]) // get integer type col names
}*/
//deptDF_.schema.fields.head.dataType
