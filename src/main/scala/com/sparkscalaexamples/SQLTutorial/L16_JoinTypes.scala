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

	val _empDF: DataFrame = empData.toDF(empColnames:_*)
	_empDF.show(truncate = false)

	val empRows: Seq[Row] = empData.map( tupleRow => Row( tupleRow.productIterator.toList:_* ))
	val empSchema: StructType = StructType(
		empColNameTypePairs.map{ case (title, tpe) => StructField(name = title, dataType = tpe, nullable = true)}
	)
	val _empDF2: DataFrame = spark.createDataFrame(empRows, empSchema)

	assert(colType(_empDF, "emp_dept_id") == StringType && colType(_empDF2, "emp_dept_id") == StringType,
		"Both column types must be StringType"
	)

	// Now convert the emp_dept_id to be integer type:
	val empDF: DataFrame = _empDF.withColumn("emp_dept_id", col("emp_dept_id").cast(IntegerType))

	// Test that conversion of column type from string -> int worked
	assert(colType(_empDF, "emp_dept_id") == StringType &&  //was
		colType(empDF, "emp_dept_id") == IntegerType, // is now
		"EmpDf emp-dept-id must now be IntegerType"
	)

	assert(convert[Int](_empDF, "name").forall(_ == null), "Changing coltype to unsuitable target type yields null " +
		"list")
	assert(convert[Int](_empDF, "emp_dept_id") == List(10, 20, 10, 10, 40, 50),
		"Changing coltype to suitable target type yields desired int list")



	// Creating the department data --------------------------------------------------------------------------

	val deptData: Seq[(String, Int)] = Seq(("Finance",10),
		("Marketing",20),
		("Sales",30),
		("IT",40)
	)

	val deptColNameTypePairs: Seq[(String, DataType)] = Seq(("dept_name", StringType), ("dept_id", IntegerType))
	val deptColnames: Seq[String] = deptColNameTypePairs.map(_._1)
	val _deptDF = deptData.toDF(deptColnames:_*)
	_deptDF.show(truncate = false)


	val deptRows: Seq[Row] = deptData.map( tupleRow => Row( tupleRow.productIterator.toList:_* ))
	/*val deptSchema = StructType(Seq(StructField(name="dept_name", dataType=StringType), StructField(name="dept_id",
		dataType=IntegerType)))*/
	val deptSchema: StructType = StructType(
		deptColNameTypePairs.map{ case (title, tpe) => StructField(name = title, dataType = tpe, nullable = true)}
	)
	val deptDF: DataFrame = spark.createDataFrame(deptRows, deptSchema)



	// Inner join - use to match dataframes on KEY columns and where KEYS don't match, the rows get dropped from
	// both datasets
	val _innerJoin = _empDF.join(
		right = _deptDF,
		joinExprs = _empDF("emp_dept_id") === _deptDF("dept_id"),
		joinType = "inner"
	)
	_innerJoin.show()

	assert(_innerJoin.columns.toList == (_empDF.columns.toList ++ _deptDF.columns.toList),
		"Test: colnames of inner join are aggregation of the joined dataframes"
	)
	assert(colType(_empDF, "emp_dept_id") == StringType &&
		colType(_deptDF, "dept_id") == IntegerType &&
		colType(_innerJoin, "emp_dept_id") == StringType,
		"The df from result of inner join has col data type same as that of col type of the colname given to match " +
			"on (StringType)"
	)
	val innerJoin: DataFrame = empDF.join(right = deptDF,
		joinExprs = empDF("emp_dept_id") === deptDF("dept_id"),
		joinType = "inner"
	)
	innerJoin.show()


	val ecol = convert[Int](empDF, "emp_dept_id")
	val dcol = convert[Int](deptDF, "dept_id")
	val commonIDElems = ecol.toSet.intersect(dcol.toSet)
	val icol = convert[Int](innerJoin, "emp_dept_id")

	assert(icol.toSet == commonIDElems &&
		commonIDElems.subsetOf(icol.toSet), "Inner join is result of matching on the given column")

	assert(colType(empDF, "emp_dept_id") == IntegerType &&
		colType(deptDF, "dept_id") == IntegerType &&
		colType(innerJoin, "emp_dept_id") == IntegerType,
		"The df from inner join has col data type same as that of col type of the colname given to match on " +
			"(IntegerType)"
	)


	// Full outer join = returns all rows from both datasets, and where join expressions don't match it returns null
	// on the respective record columns

	val outerJoin: DataFrame = empDF.join(right = deptDF,
		joinExprs = empDF("emp_dept_id") === deptDF("dept_id"),
		joinType = "outer"
	)
	outerJoin.show()

	val fullJoin = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "full")
	fullJoin.show()
	val fullOuterJoin = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "fullouter")
	fullOuterJoin.show()

	val (oc, fc, foc) = (outerJoin.collect, fullJoin.collect, fullOuterJoin.collect)

	assert(oc.sameElements(fc) && fc.sameElements(foc), "Test: all outer joins are the same")

	val (edRows, deRows) = getMismatchRows[Int](empDF, deptDF, "emp_dept_id", "dept_id")

	val edMismatch: List[Row] = ecol.toSet.diff(dcol.toSet)
		.toList
		.flatMap(diffElem =>
			outerJoin.where(outerJoin.col("emp_dept_id") === diffElem).collect.toList
		)

	val deMismatch: List[Row] = dcol.toSet.diff(ecol.toSet)
		.toList
		.flatMap(diffElem =>
			outerJoin.where(outerJoin.col("dept_id") === diffElem).collect.toList
		)
	assert(edRows == edMismatch, "Test: non-matching rows of first df with respect to second df")
	assert(deRows == deMismatch, "Test: non-matching rows of second df with respect to first df")

	assert(edRows.map(row => row.toSeq.takeRight(deptDF.columns.length).forall(_ == null)).forall(_ == true),
		"Test: Last elements in the row that don't match are always null (first df relative to second)"
	)
	assert(deRows.map(row => row.toSeq.take(empDF.columns.length).forall(_ == null)).forall(_ == true),
		"Test: first elements in the row that don't match are always null (second df relative to first"
	)


	// Left outer join returns all rows from the left dataframe / dataset regardless of
	// the match found on the right data set; shows the null row componenets only where the left df doesn't match
	// the right df (and drops records from right df where match wasn't found)
	val leftOuterJoin: DataFrame = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "left")
	leftOuterJoin.show(truncate = false)
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
