package com.SparkDocumentationByTesting.specs.AboutDataFrames



import org.apache.spark.sql.{Column, ColumnName, DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
import org.apache.spark.sql.types._
import utilities.GeneralMainUtils._
import com.data.util.EnumHub._
import utilities.EnumUtils.implicits._
import utilities.DFUtils
import DFUtils.TypeAbstractions._
import DFUtils.implicits._
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.jdk.CollectionConverters._

//import com.SparkSessionForTests
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper // intercept
import com.SparkDocumentationByTesting.CustomMatchers
import org.scalatest.Assertion

import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._
import com.data.util.DataHub.ManualDataFrames.fromSparkByExamples._
import TradeDf._
import AnimalDf._
import ArtistDf._
import Artist._



/**
 *
 */
class SchemaSpecs extends AnyFunSpec with Matchers  with SparkSessionWrapper {


	import com.SparkDocumentationByTesting.state.SpecState._
	import sparkSessionWrapper.implicits._

	val sess = sparkSessionWrapper



	describe("Schema - has properties"){

		it("has fields"){
			animalDf.schema.fields shouldBe a [Array[StructField]]

			animalDf.schema.fields should contain allElementsOf(Array(
				StructField("Animal", StringType, true),
				StructField("Amount", IntegerType, true),
				StructField("Country", StringType, true),
				StructField("Climate", StringType, true))
			)

			animalDf.schema.fields should equal (animalSchema.fields)
		}

		it("has field names"){

			val names1: Seq[String] = animalDf.schema.fields.map(_.name).toList
			val names2: Seq[String] = animalDf.schema.fieldNames.toList
			val names3: Seq[NameOfCol] = colnamesAnimal
			val names4: Seq[String] = animalSchema.names.toList

			val theNames: Seq[Seq[NameOfCol]] = List(names1, names2, names3, names4).map(_.asInstanceOf[Seq[NameOfCol]])

			theNames.map(nlst => nlst shouldBe a [ Seq[NameOfCol]])

			theNames.map(nlst => nlst should contain allElementsOf colnamesAnimal )

		}

		it("has data types"){

			/*val types1: Array[DataType] = animalDf.schema.fields.map(_.dataType)
			//val types2: Array[String] = animalDf.schema.typeName
			val types3: List[DataType] = coltypesAnimal
			val types4: Array[DataType] = animalSchema.fields.map(_.dataType)*/
			val types1: Seq[DataType] = animalDf.schema.fields.map(_.dataType).toList
			//val types2: Array[String] = animalDf.schema.typeName
			val types3: Seq[DataType] = coltypesAnimal
			val types4: Seq[DataType] = animalSchema.fields.map(_.dataType).toList

			val theTypes: Seq[Seq[DataType]] = List(types1, types3, types4) //.map(_.asInstanceOf[Seq[DataType]])

			theTypes.map(tlst => tlst shouldBe a[Seq[DataType]])

			theTypes.map(tlst => tlst should contain allElementsOf coltypesAnimal)
		}
	}


	// SOURCE = https://sparkbyexamples.com/spark/spark-get-datatype-column-names-dataframe/
	describe("Schema - can be queried ..."){

		it("for the name"){

		}

		it("for the dataType"){

		}
	}



	// SOURCE = https://sparkbyexamples.com/spark/spark-sql-structtype-on-dataframe/
	describe("Schema - can be created to be used in context of a dataframe ... ") {

		val nestedData: Seq[Row] = Seq(

			Row(Row("James ", "", "Smith"), List("Cricket", "Movies"), Map("hair" -> "black", "eye" -> "brown")),
			Row(Row("Michael ", "Rose", ""), List("Tennis"), Map("hair" -> "brown", "eye" -> "black")),
			Row(Row("Robert ", "", "Williams"), List("Cooking", "Football"), Map("hair" -> "red", "eye" -> "gray")),
			Row(Row("Maria ", "Anne", "Jones"), null, Map("hair" -> "blond", "eye" -> "red")),
			Row(Row("Jen", "Mary", "Brown"), List("Blogging"), Map("white" -> "black", "eye" -> "black"))
		)


		it("using StructType and list of StructFields") {

			val nestedListSchema: StructType = StructType(List(
				StructField("name", StructType(List(
					StructField("firstname", StringType),
					StructField("middlename", StringType),
					StructField("lastname", StringType)
				))),
				StructField("hobbies", ArrayType(StringType)),
				StructField("properties", MapType(StringType, StringType))
			))
			val df: DataFrame = sess.createDataFrame(sess.sparkContext.parallelize(nestedData), nestedListSchema)


			df.schema shouldEqual nestedListSchema


			// Checking the nested struct came out ok
			df.schema("name").dataType shouldBe a[StructType]
			df.select("name").collect().toSeq shouldEqual Seq(
				Row(Row("James ", "", "Smith")),
				Row(Row("Michael ", "Rose", "")),
				Row(Row("Robert ", "", "Williams")),
				Row(Row("Maria ", "Anne", "Jones")),
				Row(Row("Jen", "Mary", "Brown"))
			)
			df.select("name").collectCol[Row] shouldEqual Seq(
				Row("James ", "", "Smith"),
				Row("Michael ", "Rose", ""),
				Row("Robert ", "", "Williams"),
				Row("Maria ", "Anne", "Jones"),
				Row("Jen", "Mary", "Brown")
			)

			// Checking the map structure came out ok
			df.schema("properties").dataType shouldEqual MapType(StringType, StringType)
			df.select("properties").collectMapCol[String, String] shouldBe a[Map[String, String]]
		}


		it("using StructType with add()") {

			val nestedAddSchema: StructType = new StructType()
				.add("name", new StructType()
					.add("firstname", StringType)
					.add("middlename", StringType)
					.add("lastname", StringType))
				.add("hobbies", ArrayType(StringType))
				.add("properties", MapType(StringType, StringType))


			val df: DataFrame = sess.createDataFrame(nestedData.asJava, nestedAddSchema)

			df.schema shouldEqual nestedAddSchema

			df.schema("name").dataType shouldBe a[StructType]
			df.schema("hobbies").dataType shouldEqual ArrayType(StringType)
			df.schema("properties").dataType shouldEqual MapType(StringType, StringType)
		}
	}


	// SOURCE = https://hyp.is/I0JVUNYLEe6I6WthXtJSyg/sparkbyexamples.com/spark/spark-sql-structtype-on-dataframe/
	describe("Schema - can change the struct of the dataframe"){


		it("using the function struct()"){

		}
	}


	// SOURCE = https://sparkbyexamples.com/spark/spark-sql-structtype-on-dataframe/
	describe("Schema - can be inter-changed with JSON"){

		val nestedListSchema: StructType = StructType(List(
			StructField("name", StructType(List(
				StructField("firstname", StringType),
				StructField("middlename", StringType),
				StructField("lastname", StringType)
			))),
			StructField("hobbies", ArrayType(StringType)),
			StructField("properties", MapType(StringType, StringType))
		))


		describe("Schema -> JSON: meta-level") {

			it("using schema.prettyJson() function") {

				/*
				// SOURCE = https://stackoverflow.com/questions/6879427/scala-write-string-to-file-in-one-statement
				import java.io.PrintWriter

				// Specify file to write out to , in order to store the result
				val PATH = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/test/scala/com/SparkDocumentationByTesting/specs/AboutDataFrames/output.txt"
				val f = new PrintWriter(PATH) {
					write(nestedListSchema.prettyJson); close
				}*/

				nestedListSchema.prettyJson shouldEqual
					"""
					  |{
					  |  "type" : "struct",
					  |  "fields" : [ {
					  |    "name" : "name",
					  |    "type" : {
					  |      "type" : "struct",
					  |      "fields" : [ {
					  |        "name" : "firstname",
					  |        "type" : "string",
					  |        "nullable" : true,
					  |        "metadata" : { }
					  |      }, {
					  |        "name" : "middlename",
					  |        "type" : "string",
					  |        "nullable" : true,
					  |        "metadata" : { }
					  |      }, {
					  |        "name" : "lastname",
					  |        "type" : "string",
					  |        "nullable" : true,
					  |        "metadata" : { }
					  |      } ]
					  |    },
					  |    "nullable" : true,
					  |    "metadata" : { }
					  |  }, {
					  |    "name" : "hobbies",
					  |    "type" : {
					  |      "type" : "array",
					  |      "elementType" : "string",
					  |      "containsNull" : true
					  |    },
					  |    "nullable" : true,
					  |    "metadata" : { }
					  |  }, {
					  |    "name" : "properties",
					  |    "type" : {
					  |      "type" : "map",
					  |      "keyType" : "string",
					  |      "valueType" : "string",
					  |      "valueContainsNull" : true
					  |    },
					  |    "nullable" : true,
					  |    "metadata" : { }
					  |  } ]
					  |}
					  |""".stripMargin

			}
		}
		describe ("JSON -> Schema: meta-level"){


			// NOTE SOLUTION SOLUTION for converting json-string -> schema spark  = https://hyp.is/MNrDZNZbEe6jsOepUZosCQ/sparkbyexamples.com/spark/spark-sql-dataframe-data-types/

			

			// TODO not sure of a canonical way yet ... look further into these two resources - the second one is for canonical json schema not for our prupose here
			// 1) https://gist.github.com/PawaritL/a9d50a7b80f93013bd739255dd206a70
			// 2) https://github.com/zalando-incubator/spark-json-schema/blob/master/src/test/scala/org/zalando/spark/jsonschema/SchemaConverterTest.scala
			// 3) NOTE tried this method here but did not work = https://kb.databricks.com/scala/create-df-from-json-string-python-dictionary
			// output result of nestedListSchema.prettyJson to file and applied method in link 3 to a dataframe but resulting schema was not idndetical as starting schema.

			// 4) how to use this schema string and convert it to StructType? https://hyp.is/0zx8PtZVEe6DPe_Go4Grdw/sparkbyexamples.com/spark/spark-most-used-json-functions-with-examples/


			// TODO 1  - given the above json string, parse the 'fields' to get the fields as scala sequence of {name:..}, ... {name...} etc
			// TODO 2 - OR just parse straight json-string -> spark schema
		}

		describe("Schema -> JSON: data-level"){

			it("using `to_json` function`"){

				val resultDf: DataFrame = (dfNested.groupBy("name", "dob", "gender")
					.agg(collect_list(struct(col("name"), col("dob").as("dateofbirth"), col("gender"), col("salary"))).alias("jsonCol"))
					.withColumn("jsonCol", to_json(col("jsonCol"))))

				resultDf.columns shouldEqual Seq("name", "dob", "gender", "jsonCol")

				resultDf.select("jsonCol").collectCol[String].mkString(",\n") shouldEqual
					"""
					  |[{"name":{"firstname":"James ","middlename":"","lastname":"Smith"},"dateofbirth":"36636","gender":"M","salary":3000}],
					  |[{"name":{"firstname":"Maria ","middlename":"Anne","lastname":"Jones"},"dateofbirth":"39192","gender":"F","salary":4000}],
					  |[{"name":{"firstname":"Jen","middlename":"Mary","lastname":"Brown"},"dateofbirth":"","gender":"F","salary":-1}],
					  |[{"name":{"firstname":"Michael ","middlename":"Rose","lastname":""},"dateofbirth":"40288","gender":"M","salary":4000}],
					  |[{"name":{"firstname":"Robert ","middlename":"","lastname":"Williams"},"dateofbirth":"42114","gender":"M","salary":4000}]
					  |""".stripMargin
			}

			it("using `get_json_object` function"){

				// NOTE: groupby contains columns you can index into later on at get_json_object call
				val resultDf: DataFrame = (dfNested
					.groupBy("name", "dob")
					.agg(collect_list(struct(col("name"), col("dob"), col("gender"), col("salary"))).alias("jsonCol"))
					.toJSON
					.select(
						get_json_object(col("value"), "$.name").as("name"),
						get_json_object(col("value"), "$.dob").as("dateofbirth"),
						get_json_object(col("value"), "$.jsonCol").as("jsonCol")
					))

				resultDf.columns shouldEqual Seq("name", "dateofbirth", "jsonCol")

				resultDf.select("jsonCol").collectCol[String].mkString(",\n") shouldEqual
					"""
					  |{"name":{"firstname":"Jen","middlename":"Mary","lastname":"Brown"},"dob":"","gender":"F","salary":-1}],
					  |[{"name":{"firstname":"Robert ","middlename":"","lastname":"Williams"},"dob":"42114","gender":"M","salary":4000}],
					  |[{"name":{"firstname":"Maria ","middlename":"Anne","lastname":"Jones"},"dob":"39192","gender":"F","salary":4000}],
					  |[{"name":{"firstname":"Michael ","middlename":"Rose","lastname":""},"dob":"40288","gender":"M","salary":4000}],
					  |[{"name":{"firstname":"James ","middlename":"","lastname":"Smith"},"dob":"36636","gender":"M","salary":3000}]
					  |""".stripMargin


			}

		}

		it("converting JSON to schema StructType, again"){

		}

	}

	// TODO search "withField", "dropField" = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L617-L636

}
