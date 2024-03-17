package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, Dataset, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize }
import org.apache.spark.sql.types._
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


//import com.SparkSessionForTests
import com.SparkDocumentationByTesting.CustomMatchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import org.scalatest.Assertions._
import utilities.SparkSessionWrapper


/**
 *
 */
class ColCastSpecs  extends AnyFunSpec with Matchers /*with CustomMatchers*/ with SparkSessionWrapper {
	val sess = sparkSessionWrapper


	describe("Casting column to a type ..."){

		describe("cast() argument - can be a String, or DataType") {


			it("cast to StringType"){

				columnCastResultIs[String](originalDf = dfCast, colnameToCast = "NameString", dataType = StringType, strDataTypes = Seq("String", "string")) should be (true)
			}

			it("cast to ByteType"){

				columnCastResultIs[Byte](dfCast, "age", ByteType, Seq("byte", "Byte")) should be (true)
			}

			it("cast to IntegerType, ShortType, LongType"){

				columnCastResultIs[Integer](dfCast, "numberOfSponsors", IntegerType, Seq("integer", "Integer"))

				columnCastResultIs[Int](dfCast, "numberOfSponsors", IntegerType, Seq("int", "Integer")) should be (true)

				columnCastResultIs[Short](dfCast, "numberOfSponsors", ShortType, Seq("short", "Short")) should be (true)

				columnCastResultIs[Long](dfCast, "numberOfSponsors", LongType, Seq("long", "Long")) should be (true)
			}

			it("cast to DoubleType, FloatType, DecimalType") {

				columnCastResultIs[Double](dfCast, "salary", DoubleType, Seq("double", "Double")) should be (true)

				columnCastResultIs[Float](dfCast, "salary", FloatType, Seq("float", "Float")) should be (true)

				//HELP not possible here because Decimal is AbstractDataType which is > DataType
				// columnCastResultIs[Decimal](dfCast, "salary", DecimalType, Seq("decimal", "Decimal")) //should be (true)
			}

			it("cast to CharType") {

				// TODO what happens when inputting length > 1 for CharType?

				// HELP the resulting type from the cast is String not Char so this doesn't work.
				//columnCastResultIs[Char](dfCast, "lastNameInitial", CharType(1), Seq("char", "Char")) should be (true)
			}

			// supported types are: string, boolean, byte, short, int, long, float, double, decimal, date, timestamp.
			// arraytype, maptype, chartype
			// TODO here: daytimeintervaltype, yearmonthintervaltype, calendarintervaltype
			// HELP  cast to ArrayTYpe, MapType HOW?

			it("cast to BooleanType") {

				columnCastResultIs[Boolean](dfCast, "isGraduated", BooleanType, Seq("boolean", "Boolean")) should be (true)
			}


			it("cast to DateType") {

				// NOTE no scala type equivalent so must still use the spark types

				columnCastResultIs[DateType](dfCast, "jobStartDate", DateType, Seq("date", "Date")) should be (true)
			}

			it("cast to TimestampType"){
				// NOTE no scala type equivalent so must still use the spark types

				columnCastResultIs[TimestampType](dfCast, "jobStartTime", TimestampType, Seq("timestamp", "Timestamp")) should be (true)
			}



			// SOURCE = https://sparkbyexamples.com/spark/convert-delimiter-separated-string-to-array-column-in-spark/#:~:text=Spark%20SQL%20provides%20split(),e.t.c%2C%20and%20converting%20into%20ArrayType.
			describe("cast to ArrayType(string) - this assumes the column to be casted is splittable into an array"){

				// Given a comma-delimited column, must convert that to array[string]

				it("using split()"){

					val resultDf: DataFrame = dfCast.withColumn("NameArray", split(col("NameString"), pattern = ","))

					val resultDfExplicitCastNoNeed: DataFrame = dfCast.withColumn("NameArray", split(col("NameString"), pattern = ",").cast("array<string>"))

					//resultDf should equalDataFrame(resultDfExplicitCastNoNeed)
					// TODO not true because of the nullable thing - why?
					//resultDf.schema("NameArray") shouldEqual resultDfExplicitCastNoNeed.schema("NameArray")
					// (resultDf.schema == resultDfExplicitCastNoNeed.schema) should be (true)

					resultDf.select("NameArray").collectSeqCol[String] shouldEqual Seq(
						List("James", " A", " Smith"),
						List("Michael", " Rose", " Jones"),
						List("Robert", "K", "Williams"),
						List("Maria", "Anne", "Jones")
					)

					resultDf.schema(
						"NameArray").dataType shouldEqual ArrayType(StringType, false) // TODO why false?
				}

				it("using UDF"){

					val strToArray: UserDefinedFunction = udf((value : String) => value.split(','))
					val resultDf: DataFrame = dfCast.withColumn("NameArray", strToArray(col("NameString")))

					resultDf.select("NameArray").collectSeqCol[String] shouldEqual Seq(
						List("James", " A", " Smith"),
						List("Michael", " Rose", " Jones"),
						List("Robert", "K", "Williams"),
						List("Maria", "Anne", "Jones")
					)

					resultDf.schema("NameArray").dataType shouldEqual ArrayType(StringType, true)
				}

				it("using SQL expression and split()"){

					// TODO is there a way to use withColumn() instead of select() inside sql string query?

					dfCast.createOrReplaceTempView("CastExample")

					val resultDf: DataFrame = sess.sql("SELECT *, SPLIT(NameString, ',') AS NameArray FROM CastExample")

					resultDf.select("NameArray").collectSeqCol[String] shouldEqual Seq(
						List("James", " A", " Smith"),
						List("Michael", " Rose", " Jones"),
						List("Robert", "K", "Williams"),
						List("Maria", "Anne", "Jones")
					)

					// TODO why is this result nullable = false while the others above nullable = true?
					resultDf.schema("NameArray").dataType shouldEqual ArrayType(StringType, false)
				}

			}
		}





		describe("casting - using selectExpr()") {

		}




		describe("casting - using sql string") {

			dfCast.createOrReplaceTempView("CastExample")

			it("method 1 - using the cast word"){
				val resultDf: DataFrame = sess.sql("SELECT *, cast(numberOfSponsors as Integer) AS numberOfSponsors2 from CastExample")

				resultDf.schema("numberOfSponsors2").dataType shouldEqual IntegerType
			}

			it("method 2 - using the casting functions directly"){

				// WARNING: INTEGER() command here does not work - why? even though it works using the spark-code way

				val sqlString: String = """
				  |SELECT
				  |SPLIT(NameString, ',') AS NameArray,
				  |cast(lastNameInitial AS CHAR(1)),
				  |INT(age),
				  |DATE(jobStartDate), TIMESTAMP(jobStartTime),
				  |cast(numberOfSponsors AS SHORT),
				  |BOOLEAN(isGraduated),
				  |STRING(gender),
				  |DOUBLE(salary)
				  |FROM CastExample
				  |""".stripMargin

				val resultDf: DataFrame = sess.sql(sqlString)

				resultDf.schema shouldEqual checkCastingSchema
			}
		}


		// --------------------------------
		// NOTE: now describing the methods through which the casting is deliverd - withcol, select, etc



		describe("casting - using withColumn() results in an in-place cast"){

			it("... when NOT renaming the column"){
				val castedDf: DataFrame = (dfCast.withColumn("age", col("age").cast(StringType))
					.withColumn("isGraduated", col("isGraduated").cast(BooleanType))
					.withColumn("jobStartDate", col("jobStartDate").cast(DateType)))

				castedDf.columns.length should equal (dfCast.columns.length)
			}
			it("... but not when renaming the column"){
				val castedDf: DataFrame = dfCast.withColumn("NameArray",split(col("NameString"), ","))

				castedDf.columns.length should equal (dfCast.columns.length + 1)
			}
		}
		describe("casting - using select() with renaming can look like an in-place cast"){

			// SOURCE = https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/CastColumnType.scala#L54
			it("using a map-case statement") {

				val castedDf: DataFrame = dfCast.select(dfCast.columns.map {
					/*cstr => cstr match {*/
					case column@"NameString" => split(col("NameString"), ",").as("NameArray")
					case column@"lastNameInitial" => col("lastNameInitial").cast(CharType(1)) // HELP result cast is string not char
					case column@"age" => col("age").cast(IntegerType) //.as(oldname)
					case column@"jobStartDate" => col("jobStartDate").cast(DateType).as("jobStartDate")
					case column@"jobStartTime" => col("jobStartTime").cast(TimestampType)
					case column@"numberOfSponsors" => col("numberOfSponsors").cast(ShortType)
					case column@"isGraduated" => col("isGraduated").cast("Boolean")
					case column@"gender" => col("gender").cast(StringType)
					case column@"salary" => col("salary").cast(DoubleType)
				}: _*)

				castedDf.schema shouldEqual checkCastingSchema
			}
		}



		describe("casting - can flatten a nested structure") {

			// WARNING - do not change this, it is from RenameColSpecs

			val renameDf: DataFrame = dfNested_1.select(
				col("name").cast(StringType), // to flatten
				col("name.firstname").as("FirstName"),
				col("name.middlename").alias("MiddleName"),
				col("name.lastname").name("LastName"),
				col("dob"),
				col("gender"),
				col("salary")
			)
			val flattenedSchema: StructType = new StructType()
				.add("name", StringType)
				.add("FirstName", StringType)
				.add("MiddleName", StringType)
				.add("LastName", StringType)
				.add("dob", StringType)
				.add("gender", StringType)
				.add("salary", IntegerType)

			renameDf.schema should equal(flattenedSchema)
		}


		describe("casting - can maintain a nested structure"){

			// WARNING - do not change this, it is from RenameColSpecs

			// NOTE this does not work for non-nested columsn
			val renameDf: DataFrame = (dfNested_1
				.withColumn("Name", col("name").cast(innerRenameSchema)) // NOTE: this renames 'name' to 'Name' in-place.
				.withColumnRenamed("dob", "DateOfBirth"))

			renameDf.schema shouldEqual nestedRenamedSchema_1
		}




		// SOURCE: https://github.com/sparkbyexamples/spark-examples/blob/master/spark-sql-examples/src/main/scala/com/sparkbyexamples/spark/dataframe/StructTypeUsage.scala#L103
		describe("casting - using when() and cast() to impose a condition on a column") {

			it("using when().cast() imposes no permanent change to the column being casted, it is just to be able to write the type constraints in the when() condition"){

				val dfNestedStrSalary = dfNested_1.withColumn("salary", col("salary").cast("String"))

				val whenCastDf: DataFrame = dfNestedStrSalary.withColumn("SalaryLevel",

					when(col("salary").cast(IntegerType) < 2000, "Low")
						.when(col("salary").cast(IntegerType) < 4000, "Medium")
						.otherwise("High")
				)

				// Showing that the cast on the when() did no change to the df
				dfNestedStrSalary.schema("salary").dataType shouldEqual StringType //starting out
				whenCastDf.schema("salary").dataType shouldNot equal (IntegerType)
				whenCastDf.schema("salary").dataType shouldEqual StringType
			}
		}
	}
}
