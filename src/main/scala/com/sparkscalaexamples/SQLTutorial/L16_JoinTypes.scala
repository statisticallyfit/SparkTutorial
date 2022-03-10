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


object L16_JoinTypes extends App {


	val spark: SparkSession = SparkSession.builder()
		.master("local[1]")
		.appName("SparkByExamples.com")
		.getOrCreate()
	// for console
	//val spark: SparkSession = SparkSession.builder().master("local[1]").appName("SparkByExamples.com").getOrCreate()

	import scala.tools.reflect.ToolBox
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




	// NOTE: FINALLY DID IT
	def getTypedCol[A: TypeTag](df: DataFrame, name: String): List[A] = {
		// assert that we are converting same types (IntegerType -> Int, not to Double for instance)
		val dfDataType: String = df.schema.filter(struct => struct.name == name).head.dataType.toString
		val targetDataType: String = typeOf[A].toString
		assert(dfDataType.contains(targetDataType), "WILL NOT convert different types")

		df.select(name).collectAsList().toList.map(r => r(0)).asInstanceOf[List[A]]
	}
	/*def convert[A](df: DataFrame, name: String): List[A] = {
		df.select(name).as[A].collect.toList
	}*/
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
