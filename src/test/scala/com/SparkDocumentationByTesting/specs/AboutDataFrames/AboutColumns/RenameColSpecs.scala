package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutColumns

import org.apache.spark.sql.{DataFrame, Row, SparkSession, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => sqlSize}
import org.apache.spark.sql.types._

import utilities.GeneralUtils._
import com.data.util.EnumHub._
import utilities.EnumUtils.implicits._
import utilities.DFUtils
import DFUtils.TypeAbstractions._
import DFUtils.implicits._

//import com.SparkSessionForTests
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper // intercept
import com.SparkDocumentationByTesting.CustomMatchers

import com.data.util.DataHub.ImportedDataFrames.fromBillChambersBook._
import com.data.util.DataHub.ManualDataFrames.fromEnums._
import TradeDf._
import AnimalDf._
import ArtistDf._
import Artist._

/**
 *
 */
class RenameColSpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {


	import sparkSessionWrapper.implicits._

	import com.SparkDocumentationByTesting.state.SpecState._
	import AnimalState._



	describe("Renaming columns ..."){

		/**
		 * SOURCE: spark-test-repo
		 * 	- https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala#L126-L131
		 */
		it("using as() word"){

			val oldColName = Animal.name
			val newColName = "The Animal Column"

			animalDf.select(col(oldColName)).columns shouldEqual Seq(oldColName)

			val animalRenamedDf: DataFrame = animalDf.select(col(oldColName).as(newColName))
			animalRenamedDf.columns shouldEqual Seq(newColName)
			animalRenamedDf.columns.length shouldEqual 1
		}

		it("using alias() keyword"){
			val oldColName = Climate.name
			val newColName = "The Climate Column"

			animalDf.select(col(oldColName)).columns shouldEqual Seq(oldColName)

			val animalRenamedDf: DataFrame = animalDf.select(col(oldColName).alias(newColName))
			animalRenamedDf.columns shouldEqual Seq(newColName)
			animalRenamedDf.columns.length shouldEqual 1
		}

		it("using name() keyword") {
			val oldColName = World.name
			val newColName = "The Country Column"

			animalDf.select(col(oldColName)).columns shouldEqual Seq(oldColName)

			val animalRenamedDf: DataFrame = animalDf.select(col(oldColName).name(newColName))
			animalRenamedDf.columns shouldEqual Seq(newColName)
			animalRenamedDf.columns.length shouldEqual 1
		}
		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
		 */
		it("using expr() followed by as() keyword"){

			val renameDf: DataFrame = animalDf.select(expr("Animal as TheAnimals"))

			renameDf.columns.length should equal (1)
			renameDf.columns.head shouldEqual "TheAnimals"

			// TODO how to use select to rename col but keep all cols in the same order?
			//val dfAllCols: DataFrame = animalDf.select(col("*"), )
		}

		// TODO - rename with any way of calling the column ($, col, "" etcc)  + using alias(), as(), withColumn etc
		// resultDf.select($"Animal".alias())
		// resultDf.select($"Animal".as())
		/**
		 * SOURCE:
		 * 	- BillChambers_Chp5
		 */
		it("using expr() followed by alias()"){

			val animalRenameDf = animalDf.select(
				expr("Animal as TheAnimals_1").alias("TheAnimals_2")
			)

			animalRenameDf.columns.length should be (1)
			animalRenameDf.columns should equal (Seq("TheAnimals_2"))
		}

		it("using withColumn()"){

			val colsInOrder: Seq[Column] = List("Firm", Instrument.FinancialInstrument.name, "AmountTraded", "Transaction", "Location").map(col(_))

			//Company, Instrument.FinancialInstrument, "Amount", Transaction, World)
			val tradeRenameDf: DataFrame = (tradeDf.withColumn("Firm", col("Company"))
				.withColumn("Location", col("World"))
				.withColumn("BuyOrSell", col("Transaction"))
				.withColumn("AmountTraded", col("Amount"))
				.select(colsInOrder: _*)) // have to choose appropriate order and not maintain duplicate cols

			//Company, Instrument.FinancialInstrument, "Amount", Transaction, World

			tradeRenameDf.columns.map(col(_)) shouldEqual colsInOrder
			tradeRenameDf.columns.length shouldEqual tradeDf.columns.length
		}

		// TODO show later, trivial
		/*it("using withColumn() and as() -- is useless, withcolumn() wins out"){

		}*/

		describe("using withColumnRenamed()"){

			it("withColumnRenamed() takes only String argument, never Column type argument"){

				val colsInOrder: Seq[Column] = (Seq("FamousArtist", "FamousWork") ++ artistDf.columns.tail.tail).map(col(_))

				val artistRenamedDf: DataFrame = artistDf
					.withColumnRenamed(Human.name, "FamousArtist")
					.withColumnRenamed("TitleOfWork", "FamousWork")
					.select(colsInOrder:_*) // moving in proper order

				artistRenamedDf.columns.length shouldEqual artistDf.columns.length
				artistRenamedDf.columns shouldEqual Seq("FamousArtist", "FamousWork")
			}

			it("can rename multiple columns simultaneously by passing a Map - then don't have to drop duplicate columns"){

				val artistRenamedDf: DataFrame = artistDf.withColumnsRenamed(Map(
					Human.name -> "FamousArtist",
					"TitleOfWork" -> "FamousWork",
					Art.Literature.Genre.name -> "GenreOfWork",
					Artist.Musician.name -> "IsMusician",
					Art.name -> "DomainOfArt"
				))

				artistRenamedDf.columns shouldEqual Seq("FamousArtist", "DomainOfArt", "GenreOfWork", ArtPeriod.name, "FamousWork", "YearPublished", "PlaceOfBirth", "PlaceOfDeath", Painter.name, Sculptor.name, "IsMusician", Dancer.name, Singer.name, Writer.name, Architect.name, Actor.name)

				artistRenamedDf.columns.length shouldEqual artistDf.columns.length
			}

		}


		// SOURCE = https://sparkbyexamples.com/spark/rename-a-column-on-spark-dataframes/
		describe("using col() function - to rename all/multiple columns"){

			val newColumns = Seq("FamousArtist", "DomainOfArt", "GenreOfWork", ArtPeriod.name, "FamousWork", "YearPublished", "PlaceOfBirth", "PlaceOfDeath", Painter.name, Sculptor.name, "IsMusician", Dancer.name, Singer.name, Writer.name, Architect.name, Actor.name)
			val oldColumns = artistDf.columns

			val colsList = oldColumns.zip(newColumns).map{case (oldName, newName) => {col(oldName).as(newName)} }

			val artistRenameDf: DataFrame = artistDf.select(colsList:_*)

			artistDf.columns shouldEqual oldColumns
			artistRenameDf.columns shouldEqual newColumns
			artistDf.columns.length shouldEqual artistRenameDf.columns.length
		}

	}


	/**
	 * SOURCE: spark-by-examples
	 * 	- website: https://sparkbyexamples.com/spark/rename-a-column-on-spark-dataframes/
	 * 	- code: https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/RenameColDataFrame.scala
	 */
	describe("Renaming nested columns"){

		import com.data.util.DataHub.ManualDataFrames.fromSparkByExamples._

		it("using StructType - to rename nested column"){
			// Step 1 - create new schema stating the new names
			val renameSchema: StructType = new StructType()
				.add("FirstName", StringType)
				.add("MiddleName", StringType)
				.add("LastName", StringType)

			val renameDf: DataFrame = dfNested.select(col("name").cast(renameSchema),
				col("dob"),
				col("gender"),
				col("salary"))

			val checkSchema: StructType = new StructType()
				.add("name", renameSchema)
				.add("dob", StringType)
				.add("gender", StringType)
				.add("salary", IntegerType)

			renameDf.schema shouldEqual checkSchema
		}

		it("using select(), col(), as()/alias()/name() - to rename nested elements by flattening the nested structure"){

			val renameDf: DataFrame = dfNested.select(
				col("name.firstname").as("FirstName"),
				col("name.middlename").alias("MiddleName"),
				col("name.lastname").name("LastName")
			)
			val checkSchema: StructType = new StructType()
				.add("FirstName", StringType)
				.add("MiddleName", StringType)
				.add("LastName", StringType)
				.add("dob", StringType)
				.add("gender", StringType)
				.add("salary", StringType)

			renameDf.schema shouldEqual checkSchema
		}

		it("using withColumn() - to rename nested column"){

			val renameDf: DataFrame = dfNested.withColumn("FirstName", col("name.firstname"))
				.withColumn("MiddleName", col("name.middlename"))
				.withColumn("LastName", col("name.lastname"))
				.drop("name")

			val checkSchema: StructType = new StructType()
				.add("FirstName", StringType)
				.add("MiddleName", StringType)
				.add("LastName", StringType)
				.add("dob", StringType)
				.add("gender", StringType)
				.add("salary", StringType)

			renameDf.schema shouldEqual checkSchema
		}
	}

}
