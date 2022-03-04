package com.sparkscalaexamples.SQLTutorial

/**
 *
 */
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.catalyst.plans._

import org.apache.spark.sql.types.{DataType, StringType, IntegerType, BooleanType, DoubleType, StructField, StructType}
import scala.collection.JavaConversions._

object L16_JoinTypes extends App {


	val spark: SparkSession = SparkSession.builder()
		.master("local[1]")
		.appName("SparkByExamples.com")
		.getOrCreate()
	// for console
	//val spark: SparkSession = SparkSession.builder().master("local[1]").appName("SparkByExamples.com").getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")
	// TODO meaning?


	// Creating the employee data
	val empData = Seq((1,"Smith",-1,"2018","10","M",3000),
		(2,"Rose",1,"2010","20","M",4000),
		(3,"Williams",1,"2010","10","M",1000),
		(4,"Jones",2,"2005","10","F",2000),
		(5,"Brown",2,"2010","40","",-1),
		(6,"Brown",2,"2010","50","",-1)
	)
	val empColumns = Seq("emp_id","name","superior_emp_id","year_joined",
		"emp_dept_id","gender","salary")


	import spark.sqlContext.implicits._

	val empDF: DataFrame = empData.toDF(empColumns:_*)
	empDF.show(truncate = false)

	// Creating the department data

	val deptData = Seq(("Finance",10),
		("Marketing",20),
		("Sales",30),
		("IT",40)
	)

	val deptColTypes: Seq[(String, DataType)] = Seq(("dept_name", StringType), ("dept_id", IntegerType))
	val deptColumns: Seq[String] = deptColTypes.map(_._1)
	val deptDF_ = deptData.toDF(deptColumns:_*)
	deptDF_.show(truncate = false)


	val deptRows: Seq[Row] = deptData.map( tupleRow => Row( tupleRow.productIterator.toList:_* ))
	/*val deptSchema = StructType(Seq(StructField(name="dept_name", dataType=StringType), StructField(name="dept_id",
		dataType=IntegerType)))*/
	val deptSchema: StructType = StructType(
		deptColTypes.map{ case (title, tpe) => StructField(name = title, dataType = tpe, nullable = true)}
	)
	val deptDF: DataFrame = spark.createDataFrame(deptRows, deptSchema)



	// Inner join - use to match dataframes on KEY columns and where KEYS don't match, the rows get dropped from
	// both datasets
	val innerJoin = empDF.join(
		right = deptDF_,
		joinExprs = empDF("emp_dept_id") === deptDF_("dept_id"),
		joinType = "inner"
	)
	innerJoin.show()

	assert(innerJoin.columns.toList == (empDF.columns.toList ++ deptDF_.columns.toList),
		"Test: colnames of inner join are aggregation of the joined dataframes"
	)


	// Full outer join = returns all rows from both datasets, and where join expressions don't match it returns null
	// on the respective record columns

	val lst: List[Row] = innerJoin.collectAsList().toList
	val mat: Seq[Seq[Any]] = lst.map(_.toSeq) // convert each row to seq
	val colValues: Seq[Seq[Any]] = mat.transpose  // values by column

	val res: Array[Row] = empDF.filter(empDF.col("emp_dept_id").contains(40)).collect()
	res.isEmpty // TODO way 1 = this is how to check that the column contains a value

	// TODO way 2 = this is how to select values from a column
	val colvals: List[Any] = empDF.select("emp_dept_id").collect().map(r => r(0)).toList
	colvals.contains(50) // NOTE cannot check since types don't match

	deptDF_.select("dept_id").collectAsList().get(0).getAs[Int](0)
	// TODO left off here

	// TODO
	def getTypedColVals(df: DataFrame, arg: Tuple2[String, DataType] ): Seq[DataType] = {
		val (name, tpe): (String, DataType) = arg
		???
	}
}
