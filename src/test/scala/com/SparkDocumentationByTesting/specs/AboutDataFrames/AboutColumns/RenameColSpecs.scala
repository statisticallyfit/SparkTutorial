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

			it("withColumnRenamed() takes only String argument, never Column type argument." +
				"- withColumnRenamed() renames columns in-place unlike withColumn()"){

				val colsInOrder: Seq[EnumString] = ("FamousArtist", Art, Art.Literature.Genre, ArtPeriod, "FamousWork", "YearPublished", "PlaceOfBirth", "PlaceOfDeath", Painter, Sculptor, Musician, Dancer, Singer, Writer, Architect, Actor).tupleToNameList //.map(col(_))

				val artistRenamedDf: DataFrame = (artistDf
					.withColumnRenamed(Human.name, "FamousArtist")
					.withColumnRenamed("TitleOfWork", "FamousWork") )
					// .select(colsInOrder:_*)) // moving in proper order

				artistRenamedDf.columns.length shouldEqual artistDf.columns.length
				artistRenamedDf.columns shouldEqual colsInOrder //.map(_.toString)
			}

			it("withColumnsRenamed() passed a Map() object can rename multiple columns simultaneously and in-place"){

				val mapOfNewColnames: Map[NameOfCol, NameOfCol] = Map(
					Human.name -> "FamousArtist",
					"TitleOfWork" -> "FamousWork",
					Art.Literature.Genre.name -> "GenreOfWork",
					Artist.Musician.name -> "IsMusician",
					Art.name -> "DomainOfArt",
					Artist.Painter.name -> "IsPainter"
				)

				val artistRenamedDf: DataFrame = artistDf.withColumnsRenamed(mapOfNewColnames)

				artistRenamedDf.columns shouldEqual Seq("FamousArtist", "DomainOfArt", "GenreOfWork", ArtPeriod.name, "FamousWork", "YearPublished", "PlaceOfBirth", "PlaceOfDeath", "IsPainter", Sculptor.name, "IsMusician", Dancer.name, Singer.name, Writer.name, Architect.name, Actor.name)

				artistRenamedDf.columns.length shouldEqual artistDf.columns.length
			}

			// SOURCE: https://sparkbyexamples.com/spark/spark-rename-multiple-columns/
			it("foldLeft(), Map(), withColumnRenamed() can rename multiple columns simultaneously and in-place"){

				val mapOfNewColnames: Map[NameOfCol, NameOfCol] = Map(
					Human.name -> "FamousArtist",
					"TitleOfWork" -> "FamousWork",
					Art.Literature.Genre.name -> "GenreOfWork",
					Artist.Musician.name -> "IsMusician",
					Art.name -> "DomainOfArt",
					Artist.Painter.name -> "IsPainter"
				)
				val newEntireColnames: Seq[NameOfCol] = Seq("FamousArtist", "DomainOfArt", "GenreOfWork", ArtPeriod.name, "FamousWork", "YearPublished", "PlaceOfBirth", "PlaceOfDeath", "IsPainter", Sculptor.name, "IsMusician", Dancer.name, Singer.name, Writer.name, Architect.name, Actor.name)

				val renameDf: DataFrame = mapOfNewColnames.foldLeft(artistDf) {
					case (accDf, (oldName, newName)) => accDf.withColumnRenamed(oldName, newName)
				}

				// TODO why doesn't this work???
				//renameDf.columns containsSlice (mapOfNewColnames.values.toSeq) should be (true)
				// instead:
				mapOfNewColnames.values.toSeq.toSet.subsetOf(renameDf.columns.toSet) should be (true)
				renameDf.columns shouldEqual newEntireColnames

				// -------
				val newColNames: Seq[NameOfCol] = newEntireColnames
				val renameIndexDf: DataFrame = newColNames.foldLeft(artistDf) {
					case (accDf, newName) => {
						val i = newColNames.indexOf(newName)
						val oldName = accDf.columns(i)
						accDf.withColumnRenamed(oldName, newName)
					}
				}
				renameIndexDf.columns shouldEqual newColNames
			}

			it("for loop to rename columns dynamically"){
				val oldColnames: Seq[NameOfCol] = artistDf.columns
				val newColnames: Seq[NameOfCol] = oldColnames.map(name => s"NEW_$name")

				var accDf = artistDf
				for(i <- oldColnames.indices) {
					accDf = accDf.withColumnRenamed(existingName = oldColnames(i), newName = newColnames(i))
				}
				accDf.columns.sameElements( newColnames )
			}
		}




		// SOURCE = https://sparkbyexamples.com/spark/rename-a-column-on-spark-dataframes/
		describe("using col() function - to rename all or multiple columns") {

			val newColumns = Seq("FamousArtist", "DomainOfArt", "GenreOfWork", ArtPeriod.name, "FamousWork", "YearPublished", "PlaceOfBirth", "PlaceOfDeath", Painter.name, Sculptor.name, "IsMusician", Dancer.name, Singer.name, Writer.name, Architect.name, Actor.name)
			val oldColumns = artistDf.columns

			val colsList: Array[Column] = oldColumns.zip(newColumns).map { case (oldName, newName) => {
				col(oldName).as(newName)
			}}

			val artistRenameDf: DataFrame = artistDf.select(colsList: _*)

			artistDf.columns shouldEqual oldColumns
			artistRenameDf.columns shouldEqual newColumns
			artistDf.columns.length shouldEqual artistRenameDf.columns.length
		}



		describe("using toDF() function - to rename all columns"){

			import com.data.util.DataHub.ManualDataFrames.fromSparkByExamples._

			val newCols: Seq[String] = Seq("Names3", "Birthdate", "Gender", "Income")
			val dfRenamed: DataFrame = dfNested.toDF(newCols:_*)

			it("toDF() must be passed ALL columns not just a subset"){
				dfRenamed.columns shouldEqual newCols

				val iae = intercept[IllegalArgumentException] {dfNested.toDF("One") }
				iae.getMessage should include ("The number of columns doesn't match.")
			}

			it("toDF(), if given nested columns, throws error"){

				// First checking how the column names are nested and how the call of .columns does not show nesting
				DFUtils.getNestedSchemaNames(dfNested.schema) shouldEqual Seq("name", "firstname", "middlename", "lastname", "dob", "gender", "salary")

				dfNested.columns shouldEqual Seq("name", "dob", "gender", "salary")


				// ERROR type mismatch
				// dfNested.toDF("ONE", Seq("f","m","l"), "TWO", "THREE", "FOUR")
				val iae: IllegalArgumentException = intercept[IllegalArgumentException]{
					dfNested.toDF("NewName", "NewFirstname","NewMiddlename","NewLastname", "NewBirthday", "NewGender", "NewSalary")
				}
				iae.getMessage should include ("The number of columns doesn't match.")

			}
			it("providing colnames to toDF() when there are nested columns leaves the nested columns unchanged"){

				val dfRename: DataFrame = dfNested.toDF("NewName", "NewBirthday", "NewGender", "NewSalary")

				// Renaming flat way
				val nestedUnchangedSchema: StructType = (new StructType()
					.add("NewName", new StructType()
						.add("firstname", StringType)
						.add("middlename", StringType)
						.add("lastname", StringType))
					.add("NewBirthday", StringType)
					.add("NewGender", StringType)
					.add("NewSalary", IntegerType))

				dfRename.schema shouldEqual nestedUnchangedSchema
			}
		}
	}


	/**
	 * SOURCE: spark-by-examples
	 * 	- website: https://sparkbyexamples.com/spark/rename-a-column-on-spark-dataframes/
	 * 	- code: https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/RenameColDataFrame.scala
	 */
	describe("Renaming nested columns"){

		import com.data.util.DataHub.ManualDataFrames.fromSparkByExamples._

		describe("using cast() to StructType - to rename nested column while maintaining the nesting"){
			// Step 1 - create new schema stating the new names
			val innerRenameSchema: StructType = (new StructType()
				.add("FirstName", StringType)
				.add("MiddleName", StringType)
				.add("LastName", StringType))

			val nestedRenamedSchema: StructType = (new StructType()
				.add("Name",
					innerRenameSchema)
				.add("DateOfBirth", StringType)
				.add("gender", StringType)
				.add("salary", IntegerType))


			it("using select() and cast() "){

				val renameDf: DataFrame = (dfNested.select(col("name").as("Name").cast(innerRenameSchema),
					col("dob").as("DateOfBirth"),
					col("gender"),
					col("salary")))

				renameDf.schema shouldEqual nestedRenamedSchema
			}

			it("using withColumn() and cast() - to rename columns in-place"){

				// NOTE this does not work for non-nested columsn
				val renameDf: DataFrame = (dfNested
					.withColumn("Name", col("name").cast(innerRenameSchema))
					.withColumnRenamed("dob", "DateOfBirth"))

				renameDf.schema shouldEqual nestedRenamedSchema
			}

			// SOURCE: https://sparkbyexamples.com/spark/spark-rename-multiple-columns/
			it("using foldLeft(), and withColumn() + cast() (for maintaining nested cols) or withColumnRenamed() (for non-nested cols) to rename columns in-place"){

				val oldNamesPairNewFields: Seq[(String, StructField)] = dfNested.columns.zip(nestedRenamedSchema.fields)

				val renameDf: DataFrame = oldNamesPairNewFields.foldLeft(dfNested) {

					// NOTE cannot use withColumn in non-nested cases, doesn't rename in-place when not nested
					case (accDf, (oldName, structField)) => structField.dataType  match {
						case _:StructType => accDf.withColumn(structField.name, col(oldName).cast(structField.dataType))
						case _ => accDf.withColumnRenamed(existingName = oldName, newName = structField.name) // no need to cast
					}
				}

				renameDf.schema shouldEqual nestedRenamedSchema

				DFUtils.renameNestedDfByFold(dfNested, nestedRenamedSchema).schema shouldEqual nestedRenamedSchema
				//DFUtils.renameNestedDfByFold(artistDf, )
			}
		}

		it("using select(), col(), as()/alias()/name() - to rename nested elements by flattening the nested structure"){

			val renameDf: DataFrame = dfNested.select(
				col("name").cast(StringType),
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

			renameDf.schema should equal ( flattenedSchema )
		}


		it("using withColumn() - to rename while flattening nested column"){

			val colsInOrder: Seq[String] = Seq("FirstName", "MiddleName", "LastName", "dob", "gender", "salary")

			val renameDf: DataFrame = (dfNested
				.withColumn("FirstName", col("name.firstname"))
				.withColumn("MiddleName", col("name.middlename"))
				.withColumn("LastName", col("name.lastname")))
				//.select(colsInOrder.map(col(_)):_*)
				//.drop("name") // must select cols in order now
				//.select()

			val checkSchema: StructType = (new StructType()
				.add("name", new StructType()
					.add("firstname", StringType)
					.add("middlename", StringType)
					.add("lastname", StringType)
				)
				.add("dob", StringType)
				.add("gender", StringType)
				.add("salary", IntegerType)
				.add("FirstName", StringType)
				.add("MiddleName", StringType)
				.add("LastName", StringType))

			renameDf.schema shouldEqual checkSchema
		}
		// HELP: how to rename nested columns? Not working:
		// https://www.sparkcodehub.com/spark-dataframe-column-alias
		// https://medium.com/@uzzaman.ahmed/what-is-withcolumnrenamed-used-for-in-a-spark-sql-7bda0c465195#:~:text=To%20Rename%20Nested%20Columns%20in,with%20the%20withColumnRenamed()%20method.
	}

}
