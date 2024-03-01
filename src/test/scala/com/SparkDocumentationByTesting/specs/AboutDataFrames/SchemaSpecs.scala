package com.SparkDocumentationByTesting.specs.AboutDataFrames



import org.apache.spark.sql.{Column, ColumnName, DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction


import utilities.GeneralMainUtils._
import com.data.util.EnumHub._
import utilities.EnumUtils.implicits._
import utilities.DFUtils
import DFUtils._
import DFUtils.TypeAbstractions._
import DFUtils.implicits._
import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._
import com.data.util.DataHub.ManualDataFrames.fromSparkByExamples._
import TradeDf._
import AnimalDf._
import ArtistDf._
import Artist._



import scala.jdk.CollectionConverters._

//import com.SparkSessionForTests
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper // intercept
import com.SparkDocumentationByTesting.CustomMatchers
import org.scalatest.Assertion




/**
 *
 */
class SchemaSpecs extends AnyFunSpec with Matchers  with SparkSessionWrapper {


	import com.SparkDocumentationByTesting.state.SpecState._
	import sparkSessionWrapper.implicits._

	val sess = sparkSessionWrapper


	// --------------------------------------------------------------------------------------------------------------


	describe("Schema - has properties"){

		it("has fields"){
			animalDf.schema.fields shouldBe a [Array[StructField]]

			animalDf.schema.fields should contain allElementsOf(Array(
				StructField(Animal.name, StringType, true),
				StructField("Amount", IntegerType, true),
				StructField(World.name, StringType, true),
				StructField(Climate.name, StringType, true))
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

		it("has nullable property"){

			artistDf.schema(Architect.name).nullable shouldEqual true
		}

		it("has metadata"){
			// TODO how to check - artistDf.schema(Architect.name).metadata shouldEqual {}
		}


		// SOURCE = https://sparkbyexamples.com/spark/spark-get-datatype-column-names-dataframe/
		describe("Schema - can be queried ...") {

			it("for the name") {

				artistDf.schema(Human.name).name should equal(Human.name)
			}

			it("for the dataType") {

				tradeDf.schema(Transaction.name).dataType should equal(StringType)
			}

			// SOURCE = https://hyp.is/4gdmDNaMEe6j9mu1jnL4wA/sparkbyexamples.com/spark/spark-sql-structtype-on-dataframe/
			it("to check if a field exists in the schema"){

				artistDf.schema.fieldNames.contains(World.name) should equal (false)

				artistDf.schema.contains(
					StructField("YearPublished", IntegerType)
				) shouldEqual true
			}
		}
	}

	// --------------------------------------------------------------------------------------------------------------


	describe("Nested schema can be flattened"){

		it("using .* operator"){

			val flattenedCols: Seq[NameOfCol] = Seq("name.firstname", "name.middlename", "name.lastname", "address_curr_state", "address_curr_city", "address_prev_state", "address_prev_city")

			val flattenedDf: DataFrame = (dfNested_2.select(col("name.*"),
				col("address.current.*"),
				col("address.previous.*")).toDF(flattenedCols:_*))

			flattenedDf.columns should equal (flattenedCols)

			flattenedDf.schema shouldEqual StructType(List(
				StructField("name.firstname",StringType,true),
				StructField("name.middlename",StringType,true),
				StructField("name.lastname",StringType,true),
				StructField("address_curr_state",StringType,true),
				StructField("address_curr_city",StringType,true),
				StructField("address_prev_state",StringType,true),
				StructField("address_prev_city",StringType,true)
			))

		}

		it("using recursion"){

			val flattenedColsByRecursion: Array[Column] = flattenStructSchema(schemaNested_2)

			val flattenedDf: DataFrame = dfNested_2.select(flattenedColsByRecursion:_*)

			flattenedDf.schema shouldEqual StructType(List(
				StructField("name_firstname", StringType, true),
				StructField("name_middlename", StringType, true),
				StructField("name_lastname", StringType, true),
				StructField("address_current_state", StringType, true),
				StructField("address_current_city", StringType, true),
				StructField("address_previous_state", StringType, true),
				StructField("address_previous_city", StringType, true)
			))
		}

		it("using recursion with explode()"){

			// TODO this is more than for exam , do later = https://medium.com/@nalin.rs/apache-spark-data-transformation-flattening-structs-exploding-arrays-0c4db948acce
		}
	}



	// --------------------------------------------------------------------------------------------------------------

	// SOURCE = https://sparkbyexamples.com/spark/spark-sql-structtype-on-dataframe/
	describe("Schema - can be used in context of a dataframe ... ") {

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

			// checking the list structure came out ok
			df.schema("hobbies").dataType shouldEqual ArrayType(StringType)
			df.select("hobbies").collectSeqCol[String] shouldBe a [Seq[Seq[String]]]

			// Checking the map structure came out ok
			df.schema("properties").dataType shouldEqual MapType(StringType, StringType)
			df.select("properties").collectMapCol[String, String] shouldBe a [Seq[Map[String, String]]]
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


	// --------------------------------------------------------------------------------------------------------------

	// SOURCE = https://hyp.is/I0JVUNYLEe6I6WthXtJSyg/sparkbyexamples.com/spark/spark-sql-structtype-on-dataframe/
	describe("Schema - can change the struct of the dataframe"){


		it("using the function struct()"){

			val newStructDf: DataFrame = (dfNested_1.withColumn("OtherInfo", struct(
				col("dob").cast(DateType).as("dateofbirth"),
				col("gender"),
				col("salary"),
				when(col("salary").cast(IntegerType) < 2000, lit("Low"))
					.when(col("salary").cast(IntegerType) < 4000, lit("Medium"))
					.otherwise("High")
					.alias("SalaryGrade")
			)).drop("gender", "salary", "dob"))

			val checkSchema: StructType = StructType(List(
				StructField("name", StructType(List(
					StructField("firstname", StringType),
					StructField("middlename", StringType),
					StructField("lastname", StringType)
				))),
				StructField("OtherInfo", StructType(List(
					StructField("dateofbirth", DateType),
					StructField("gender", StringType),
					StructField("salary", IntegerType),
					StructField("SalaryGrade", StringType, nullable = false) // TODO why false in the result above?
				)), nullable = false)
			))

			newStructDf.columns shouldEqual Seq("name", "OtherInfo")
			newStructDf.schema shouldEqual checkSchema
		}
	}


	// --------------------------------------------------------------------------------------------------------------

	describe("Schema - is convertible to and from Scala Case Classes"){

		it("schema -> case class: using Encoders"){

			// HELP compiler complains no typetag available for these classes ...
//			// Minimal working example to get the point across.
//			case class Gem(private val gem: String = "gem") // no changing the arg
//			case class Metal(private val metal: String = "preciousMetal")
//			// WARNING either doesn't make the args show up in the tree string - why?
//			//case class Commodity(commodityEither: Either[Gem, Metal])
//			case class Commodity(g: Option[Gem], m: Option[Metal])
//
//			/*class PriceInstr(pricing: String)
//			case class Stock(stock: String) extends PriceInstr
//			case class Bond(bond: String) extends PriceInstr*/
//			case class FinancialInstrument(/*priceInstr: PriceInstr, */ commodity: Commodity)
//			case class Instrument(finInstr: FinancialInstrument)
//
//			import org.apache.spark.sql.catalyst.ScalaReflection
//			import org.apache.spark.sql.Encoder
//			val schemaResult: StructType = implicitly[Encoder[Instrument]].schema
//				//ScalaReflection.schemaFor[Instrument].dataType.asInstanceOf[StructType]
//
//
//
//			schemaResult.treeString shouldEqual
//				"""
//				  |root
//				  | |-- finInstr: struct (nullable = true)
//				  | |    |-- commodity: struct (nullable = true)
//				  | |    |    |-- g: struct (nullable = true)
//				  | |    |    |    |-- gem: string (nullable = true)
//				  | |    |    |-- m: struct (nullable = true)
//				  | |    |    |    |-- metal: string (nullable = true)
//				  |""".stripMargin
//
//
//			schemaResult should equal (
//				StructType(List(
//					StructField("finInstr",
//						StructType(
//							List(StructField("commodity",
//							StructType(List(
//								StructField("g",
//									StructType(List(StructField("gem",StringType,true))),
//									true),
//								StructField("m",
//									StructType(List(StructField("metal",StringType,true))),
//									true)
//							)),true))),
//							true
//						)
//				))
//			)
//
//
//
//			// NOTE: using Encoders can name class that have no string arg so can use Gold() instead of Gold(name: String)
//			import org.apache.spark.sql.Encoders
//			val schemaEncoderResult: StructType = Encoders.product[Instrument].schema
//
//			schemaEncoderResult.treeString should equal (
//				"""
//				  |root
//				  | |-- finInstr: struct (nullable = true)
//				  | |    |-- commodity: struct (nullable = true)
//				  | |    |    |-- gem: struct (nullable = true)
//				  | |    |    |    |-- gem: string (nullable = true)
//				  | |    |    |-- metal: struct (nullable = true)
//				  | |    |    |    |-- metal: string (nullable = true)
//				  |""".stripMargin
//			)




			// TODO help To develop pthis fruther and add it to the abvoe example
			//  as i want to , must create encoder for an arbitrary class like this blog post recommends: https://www.dataversity.net/case-study-deriving-spark-encoders-and-schemas-using-implicits/
			/*
			case class Company(name: String)
			case class FinancialInstrument(inst: String)
			case class Amount(int: Int)
			case class Transaction(buyOrSell: String)
			case class Location(world: String)

			case class Trade(company: Company, financialInstrument: FinancialInstrument, amount: Amount, transaction: Transaction, location: Location)

			import org.apache.spark.sql.catalyst.ScalaReflection
			val sch = ScalaReflection.schemaFor[Trade].dataType.asInstanceOf[StructType]*/


			// TODO
			// TODO figure this out - HELP not working no encoder available issue
			/*case class Company(name: String)

			case class Instrument(finInstr: FinancialInstrument)

			//trait FinInstr
			class FinInstr(s: String)
			case class Stock(private val stock: String = "stock") extends FinInstr

			trait Commdty extends FinInstr

			case class Commodity(commodity: Commdty) extends Commdty

			case class CrudeOil(oil: String) extends Commdty

			trait Metal extends Commdty

			case class PreciousMetal(metal: Metal) extends Metal

			case class Gold(gold: String = "gold") extends Metal

			trait Gem extends Commdty

			case class Ruby(ruby: String = "ruby") extends Gem

			case class Diamond(diamond: String = "diamond") extends Gem

			case class FinancialInstrument(fin: Option[FinInstr], comm: Option[Commodity])

			case class Amount(quantity: Int)

			case class Transaction(buyOrSell: String)

			case class Location(world: String)

			val fi = FinancialInstrument(None, Some(Commodity(PreciousMetal(Gold()))))

			case class Trade(company: Company, financialInstrument: FinancialInstrument, amount: Amount, transaction: Transaction, location: Location)*/

		}


		it("case class -> schema: ???"){
			// TODO
		}
	}

	// --------------------------------------------------------------------------------------------------------------

	// TODO create schema from DDL string as 'describe' =https://hyp.is/AWWhBNbrEe6suWeYXzjDcw/sparkbyexamples.com/spark/spark-sql-structtype-on-dataframe/
	describe("Schema - can be created from DDL String"){

	}

	// --------------------------------------------------------------------------------------------------------------


	/**
	 * TODO after finishing JsonParsingSpecs, decide if this test below should be moved to the JsonParsingSpecs (since it is about json conversion not just about schema ....)
	 */

	// SOURCE = https://sparkbyexamples.com/spark/spark-sql-structtype-on-dataframe/
	describe("Schema - is convertible to and from JSON"){

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

				// TODO fix minor bracket mismatchs here fix:

				/*nestedListSchema.prettyJson shouldEqual
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
					  |""".stripMargin*/

			}
		}
		describe ("JSON -> Schema: meta-level"){

			// SOURCE: SOLUTION for converting json-string -> schema spark  = https://hyp.is/MNrDZNZbEe6jsOepUZosCQ/sparkbyexamples.com/spark/spark-sql-dataframe-data-types/
			it("using DataType.fromJson function on JSON string"){

				val dTpe: DataType = DataType.fromJson(nestedListSchema.prettyJson)
				val schTpe: StructType = dTpe.asInstanceOf[StructType] // push to parent type

				schTpe shouldBe a [StructType]
				schTpe shouldEqual nestedListSchema
				DataType.equalsStructurally(schTpe, nestedListSchema) should equal (true)
			}
			it("using DataType.fromJson function on JSON file"){
				/// TODO see here https://hyp.is/eSAzKtaNEe6XB2_AuQNbhg/sparkbyexamples.com/spark/spark-sql-structtype-on-dataframe/
			}

			/**
			 * RESOURCES for effort of json -> schema conversion. Efforts that didn't work / half leads:
			 * 1) https://gist.github.com/PawaritL/a9d50a7b80f93013bd739255dd206a70
			 * 2) https://github.com/zalando-incubator/spark-json-schema/blob/master/src/test/scala/org/zalando/spark/jsonschema/SchemaConverterTest.scala
			 * 3) https://kb.databricks.com/scala/create-df-from-json-string-python-dictionary
			 * NOTE tried this method here but did not work: output result of nestedListSchema.prettyJson to file and applied method in link 3 to a dataframe but resulting schema was not idndetical as starting schema.
			 * 4) how to use this schema string and convert it to StructType? https://hyp.is/0zx8PtZVEe6DPe_Go4Grdw/sparkbyexamples.com/spark/spark-most-used-json-functions-with-examples/
			 *
			 * // TODO 1  - given the above json string, parse the 'fields' to get the fields as scala sequence of {name:..}, ... {name...} etc
			 * // TODO 2 - OR just parse straight json-string -> spark schema
			 */
		}

		describe("Json -> Schema: data-level") { // TODO HOW???
			}

		describe("Schema -> JSON: data-level"){

			it("using `to_json` function`"){

				val resultDf: DataFrame = (dfNested_1.groupBy("name", "dob", "gender")
					.agg(collect_list(struct(col("name"), col("dob").as("dateofbirth"), col("gender"), col("salary"))).alias("jsonCol"))
					.withColumn("jsonCol", to_json(col("jsonCol"))))

				resultDf.columns shouldEqual Seq("name", "dob", "gender", "jsonCol")

				resultDf.select("jsonCol").collectCol[String] should contain allElementsOf Seq(
					"[{\"name\":{\"firstname\":\"James \",\"middlename\":\"\",\"lastname\":\"Smith\"},\"dateofbirth\":\"36636\",\"gender\":\"M\",\"salary\":3000}]",
					"[{\"name\":{\"firstname\":\"Michael \",\"middlename\":\"Rose\",\"lastname\":\"\"},\"dateofbirth\":\"40288\",\"gender\":\"M\",\"salary\":4000}]",
					"[{\"name\":{\"firstname\":\"Robert \",\"middlename\":\"\",\"lastname\":\"Williams\"},\"dateofbirth\":\"42114\",\"gender\":\"M\",\"salary\":4000}]",
					"[{\"name\":{\"firstname\":\"Maria \",\"middlename\":\"Anne\",\"lastname\":\"Jones\"},\"dateofbirth\":\"39192\",\"gender\":\"F\",\"salary\":4000}]",
					"[{\"name\":{\"firstname\":\"Jen\",\"middlename\":\"Mary\",\"lastname\":\"Brown\"},\"dateofbirth\":\"\",\"gender\":\"F\",\"salary\":-1}]"
				)

			}

			// SOURCE = https://stackoverflow.com/questions/51109238/scala-spark-convert-a-struct-type-column-to-json-data
			it("using `get_json_object` function"){

				// NOTE: groupby contains columns you can index into later on at get_json_object call
				val resultDf: DataFrame = (dfNested_1
					.groupBy("name", "dob")
					.agg(collect_list(struct(col("name"), col("dob"), col("gender"), col("salary"))).alias("jsonCol"))
					.toJSON
					.select(
						get_json_object(col("value"), "$.name").as("name"),
						get_json_object(col("value"), "$.dob").as("dateofbirth"),
						get_json_object(col("value"), "$.jsonCol").as("jsonCol")
					))

				resultDf.columns shouldEqual Seq("name", "dateofbirth", "jsonCol")

				resultDf.select("jsonCol").collectCol[String] should contain allElementsOf Seq(
					"[{\"name\":{\"firstname\":\"James \",\"middlename\":\"\",\"lastname\":\"Smith\"},\"dob\":\"36636\",\"gender\":\"M\",\"salary\":3000}]",
					"[{\"name\":{\"firstname\":\"Michael \",\"middlename\":\"Rose\",\"lastname\":\"\"},\"dob\":\"40288\",\"gender\":\"M\",\"salary\":4000}]",
					"[{\"name\":{\"firstname\":\"Robert \",\"middlename\":\"\",\"lastname\":\"Williams\"},\"dob\":\"42114\",\"gender\":\"M\",\"salary\":4000}]",
					"[{\"name\":{\"firstname\":\"Maria \",\"middlename\":\"Anne\",\"lastname\":\"Jones\"},\"dob\":\"39192\",\"gender\":\"F\",\"salary\":4000}]",
					"[{\"name\":{\"firstname\":\"Jen\",\"middlename\":\"Mary\",\"lastname\":\"Brown\"},\"dob\":\"\",\"gender\":\"F\",\"salary\":-1}]"
				)


			}

		}

	}


	// --------------------------------------------------------------------------------------------------------------


	// TODO search "withField", "dropField" = https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L617-L636

}
