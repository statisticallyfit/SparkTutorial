package com.sparkdataframes.OnlineTutorials.Course_sparkbyexamples.SQLTutorial.L16_Joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}

import util.DataFrameCheckUtils._

// NOTE: need to use "JavaConversions" not "JavaConverters" so that the createDataFrame from sequence of rows will work.
import scala.collection.JavaConversions._



/**
 *
 */
object JoinsData {


	val spark: SparkSession = SparkSession.builder()
		.master("local[1]")
		.appName("SparkByExamples.com")
		.getOrCreate()
	// for console
	//val spark: SparkSession = SparkSession.builder().master("local[1]").appName("SparkByExamples.com").getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")
	// TODO meaning?

	//import spark.sqlContext.implicits._
	import spark.implicits._


	// Creating the employee data --------------------------------------------------------------------------
	val empData = Seq((1, "Smith", -1, "2018", "10", "M", 3000),
		(2, "Rose", 1, "2010", "20", "M", 4000),
		(3, "Williams", 1, "2010", "10", "M", 1000),
		(4, "Jones", 2, "2005", "10", "F", 2000),
		(5, "Brown", 2, "2010", "40", "", -1),
		(6, "Brown", 2, "2010", "50", "", -1)
	)


	val empColNameTypePairs: Seq[(String, DataType)] = Seq(("emp_id", IntegerType), ("name", StringType),
		("superior_emp_id", IntegerType), ("year_joined", StringType), ("emp_dept_id", StringType),
		("gender", StringType), ("salary", IntegerType)
	)
	val empColnames: Seq[String] = empColNameTypePairs.unzip._1
	//Seq("emp_id","name","superior_emp_id","year_joined",	"emp_dept_id","gender","salary")

	val empDF_strCol: DataFrame = empData.toDF(empColnames: _*)
	empDF_strCol.show(truncate = false)


	val empRows: Seq[Row] = empData.map(tupleRow => Row(tupleRow.productIterator.toList: _*))
	val empSchema: StructType = StructType(
		empColNameTypePairs.map { case (title, tpe) => StructField(name = title, dataType = tpe, nullable = true) }
	)
	val empDF_strCol_FromSchema: DataFrame = spark.createDataFrame(empRows, empSchema)

	assert(typeOfColumn(empDF_strCol, "emp_dept_id") == StringType &&
		typeOfColumn(empDF_strCol_FromSchema, "emp_dept_id") == StringType,
		"Test: in _empDF, both column types must be StringType"
	)

	// Now convert the emp_dept_id to be integer type:
	val empDF_intCol: DataFrame = empDF_strCol.withColumn("emp_dept_id", col("emp_dept_id").cast(IntegerType))

	// Test that conversion of column type from string -> int worked
	assert(typeOfColumn(empDF_strCol, "emp_dept_id") == StringType && //was
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
	val empRowsExtra: DataFrame = empDataExtra.toDF(empColnames: _*)

	val empDFExtra_strCol: DataFrame = empDF_strCol.union(empRowsExtra)

	// Creating the department data --------------------------------------------------------------------------

	val deptData: Seq[(String, Int)] = Seq(("Finance", 10),
		("Marketing", 20),
		("Sales", 30),
		("IT", 40)
	)

	val deptColNameTypePairs: Seq[(String, DataType)] = Seq(("dept_name", StringType), ("dept_id", IntegerType))
	val deptColnames: Seq[String] = deptColNameTypePairs.map(_._1)
	val deptDF = deptData.toDF(deptColnames: _*)
	deptDF.show(truncate = false)


	val deptRows: Seq[Row] = deptData.map(tupleRow => Row(tupleRow.productIterator.toList: _*))
	/*val deptSchema = StructType(Seq(StructField(name="dept_name", dataType=StringType), StructField(name="dept_id",
		dataType=IntegerType)))*/
	val deptSchema: StructType = StructType(
		deptColNameTypePairs.map { case (title, tpe) => StructField(name = title, dataType = tpe, nullable = true) }
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

	val deptRowsExtra: DataFrame = deptDataExtra.toDF(deptColnames: _*)

	val deptDFExtra: DataFrame = deptDF.union(deptRowsExtra)


	// --------------------------
	// Constant values for the tests:


	final val leftColname = "emp_dept_id"
	final val rightColname = "dept_id"
}
