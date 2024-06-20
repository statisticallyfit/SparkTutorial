package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutArrayColumns

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
//import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions._
import utilities.DFUtils
import utilities.DFUtils.TypeAbstractions._
import utilities.DFUtils.implicits._
import utilities.DataHub.ManualDataFrames.ArrayDf._
import utilities.DataHub.ManualDataFrames.fromEnums.TradeDf._
import utilities.DataHub.ManualDataFrames.fromEnums._
import utilities.EnumHub.Animal.Bear._
import utilities.EnumHub.Animal.Bird.Eagle._
import utilities.EnumHub.Animal.Bird._
import utilities.EnumHub.Animal.Camelid._
import utilities.EnumHub.Animal.Canine.WildCanine._
import utilities.EnumHub.Animal.Canine._
import utilities.EnumHub.Animal.Cat.WildCat._
import utilities.EnumHub.Animal.Cat._
import utilities.EnumHub.Animal.Deer._
import utilities.EnumHub.Animal.Equine.Horse._
import utilities.EnumHub.Animal.Equine._
import utilities.EnumHub.Animal.Insect._
import utilities.EnumHub.Animal.Monkey.Ape._
import utilities.EnumHub.Animal.Monkey._
import utilities.EnumHub.Animal.Reptile._
import utilities.EnumHub.Animal.Rodent.Squirrel._
import utilities.EnumHub.Animal.Rodent._
import utilities.EnumHub.Animal.SeaCreature._
import utilities.EnumHub.Animal.WeaselMustelid._
import utilities.EnumHub.Animal._
import utilities.EnumHub._
import utilities.EnumUtils.implicits._
import utilities.GeneralMainUtils.Helpers._
import utilities.GeneralMainUtils.implicits._

import scala.Double.NaN
// TODO update with new animals made

import utilities.EnumHub.Instrument._
import utilities.EnumHub.World.SouthAmerica._
import utilities.EnumHub.World._

//import com.SparkSessionForTests
import com.SparkDocumentationByTesting.CustomMatchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._
import utilities.SparkSessionWrapper


/**
 *
 */
class ArraySortSpecs extends AnyFunSpec with Matchers with CustomMatchers with SparkSessionWrapper {

	/*import AnimalState._
	import FlightState._
	import TradeState._*/

	import sparkSessionWrapper.implicits._

	/**
	 * SOURCE:
	 * 	- https://towardsdatascience.com/the-definitive-way-to-sort-arrays-in-spark-1224f5529961
	 */

	describe("Array Sorting: multiple examples using sort_array, array_sort, sortBy, & co. all in the spirit of sorting a dataframe") {


		import com.SparkDocumentationByTesting.state.ArraySpecState.SQLArrayComparisonTypeFunctionState._


		describe("sorting using: sort_array") {


			it("way 1: sort_array() for simple case: Sorts the input array in ascending or descending order." +
				"NaN is greater than any non-NaN elements for double/float type. " +
				"Null elements will be placed at the beginning of the returned array in ascending order or at the end of the returned array in descending order.") {


				val sortArrayAscDf: DataFrame = arrayNullGroupDf.select(col("col1"), sort_array(col("ArrayCol2"), asc = true))

				val expectedSortArrayAscRows: Seq[Row] = Seq(
					("x", Array(1, 1.3, 2, 2, 4, 6, 7.6, 8, 9, NaN, NaN, null, null).map(getSimpleString)),
					("z", Array(0.3, 1.1, 1.2, 4, 5.8, 7, 7.5, 8.8, NaN, NaN, null, null, null).map(getSimpleString)),
					("a", Array(2, 3, 3, 3.4, 4, 5, 8.1, NaN, NaN, null, null).map(getSimpleString))
				).toRows(sortArrayAscDf.schema)

				sortArrayAscDf.collectAll shouldEqual expectedSortArrayAscRows


				// ---

				val sortArrayDescDf: DataFrame = arrayNullGroupDf.select(col("col1"), sort_array(col("ArrayCol2"), asc = false))

				val actualRows: Seq[Row] = sortArrayDescDf.collect().toSeq

				val expectedRows: Seq[Row] = Seq(
					("x", Array(null, null, NaN, NaN, 9, 8, 7.6, 6, 4, 2, 2, 1.3, 1).map(getSimpleString)),
					("z", Array(null, null, null, NaN, NaN, 8.8, 7.5, 7, 5.8, 4, 1.2, 1.1, 0.3).map(getSimpleString)),
					("a", Array(null, null, NaN, NaN, 8.1, 5, 4, 3.4, 3, 3, 2).map(getSimpleString))
				).toRows(sortArrayDescDf.schema)

				actualRows shouldEqual expectedRows
			}


			/**
			 * SOURCES:
			 * 	- https://stackoverflow.com/questions/73259833/sort-array-of-structs
			 */

			it("sort_array: case for data that has multiple fields, so sorting by manipulating the fields via transform()") {


				import utilities.DataHub.ManualDataFrames.ArrayDf._


				val sortArrayDf: DataFrame = (personDf
					.withColumn("rearrangeMidFirst", transform(col("yourArray"), elem => struct(
						elem.getField("middleInitialThrice"),
						elem.getField("id"),
						elem.getField("name"),
						elem.getField("age"),
						elem.getField("addressNumber"))))
					.withColumn("sortedByMiddle", sort_array(col("rearrangeMidFirst"))))


				// Test the order of mid,names in the result
				val actualSortArraySeq: Seq[SortByMidStruct[PersonMidIdNameAgeStruct]] = (sortArrayDf
					.as[SortByMidStruct[PersonMidIdNameAgeStruct]]
					.collect().toSeq /*
				.map((rms: SortByMidStruct[PersonMidIdNameAgeStruct]) => rms.sortedByMiddle.map(person => (person.middleInitialThrice, person.id, person.name, person.age)))*/)

				actualSortArraySeq shouldEqual expectedSortArraySeq
			}
		}


		// TESTING: array_sort of sorting on keys using comparator
		/**
		 * SOURCES:
		 * 	- https://hyp.is/lVKHevUrEe6Hpq_i_GK6Sg/towardsdatascience.com/the-definitive-way-to-sort-arrays-in-spark-1224f5529961
		 * 	- https://juejin.cn/s/spark%20sql%20sort%20array%20of%20struct
		 */
		it("sorting using: array_sort() + comparator passed to udf") {

			val funcMiddleInitialComparator: (PersonStruct, PersonStruct) => Int = (p1, p2) => if (p1.middleInitialThrice < p2.middleInitialThrice) -1 else if (p1.middleInitialThrice == p2.middleInitialThrice) 0 else 1
			val funcNameComparator: (PersonStruct, PersonStruct) => Int = (p1, p2) => if (p1.name < p2.name) -1 else if (p1.name == p2.name) 0 else 1
			val funcIDComparator: (PersonStruct, PersonStruct) => Int = (p1, p2) => if (p1.id < p2.id) -1 else if (p1.id == p2.id) 0 else 1
			val funcAgeComparator: (PersonStruct, PersonStruct) => Int = (p1, p2) => if (p1.age < p2.age) -1 else if (p1.age == p2.age) 0 else 1
			val funcAddressComparator: (PersonStruct, PersonStruct) => Int = (p1, p2) => if (p1.addressNumber < p2.addressNumber) -1 else if (p1.addressNumber == p2.addressNumber) 0 else 1


			val udfMiddleInitialComparator: UserDefinedFunction = udf(funcMiddleInitialComparator(_: PersonStruct, _: PersonStruct): Int)
			val udfNameComparator: UserDefinedFunction = udf(funcNameComparator(_: PersonStruct, _: PersonStruct): Int)
			val udfIDComparator: UserDefinedFunction = udf(funcIDComparator(_: PersonStruct, _: PersonStruct): Int)
			val udfAgeComparator: UserDefinedFunction = udf(funcAgeComparator(_: PersonStruct, _: PersonStruct): Int)
			val udfAddressComparator: UserDefinedFunction = udf(funcAddressComparator(_: PersonStruct, _: PersonStruct): Int)


			val arraySortComparatorUdfDf: DataFrame = (personRDD.toDF()
				.withColumn("sortedByMiddle", array_sort(col("yourArray"), comparator = (p1, p2) => udfMiddleInitialComparator(p1, p2)))
				.withColumn("sortedByName", array_sort(col("sortedByMiddle"), comparator = (p1, p2) => udfNameComparator(p1, p2)))
				.withColumn("sortedByAge", array_sort(col("sortedByName"), comparator = (p1, p2) => udfAgeComparator(p1, p2)))
				.withColumn("sortedByID", array_sort(col("sortedByAge"), comparator = (p1, p2) => udfIDComparator(p1, p2)))
				.withColumn("sortedByAddress", array_sort(col("sortedByID"), comparator = (p1, p2) => udfAddressComparator(p1, p2)))
				)


			// NOTE: collecting the items as Row objects will result in error for innermost struct, classcasexception Seq[Nothing] so easiest to convert to dataset then get the rwos as objects.

			val actualUdfMidSortSeq: Seq[SortByMidStruct[PersonStruct]] = (arraySortComparatorUdfDf.as[SortByMidStruct[PersonStruct]].collect().toSeq)
			val actualUdfSortMidThenNameSeq: Seq[SortByNameStruct[PersonStruct]] = arraySortComparatorUdfDf.as[SortByNameStruct[PersonStruct]].collect().toSeq
			val actualUdfSortMidThenNameAge: Seq[SortByAgeStruct[PersonStruct]] = arraySortComparatorUdfDf.as[SortByAgeStruct[PersonStruct]].collect().toSeq
			val actualUdfSortMidThenNameAgeID: Seq[SortByIDStruct[PersonStruct]] = arraySortComparatorUdfDf.as[SortByIDStruct[PersonStruct]].collect().toSeq

			expectedUdfComparatorMidSortSeq shouldEqual actualUdfMidSortSeq

			// Checking order of names after sorting by middle initial:
			expectedNames_afterMid shouldEqual actualUdfMidSortSeq.map(smi => smi.sortedByMiddle.map(ps => ps.name))

			// Checking order of middle initial after sorting by middle initial
			expectedMiddles_afterMid shouldEqual actualUdfMidSortSeq.map(smi => smi.sortedByMiddle.map(ps => ps.middleInitialThrice))

			// Checking order of names after sorting by mid, then name
			expectedNames_afterMidThenName shouldEqual actualUdfSortMidThenNameSeq.map(smni => smni.sortedByName.map(ps => ps.name))
			// Checking order of names after sorting by mid, then name, then id
			expectedNames_afterMidThenNameAge shouldEqual actualUdfSortMidThenNameAge.map(sma => sma.sortedByAge.map(ps => ps.name))
			// Checking order of names after sorting by mid, name, id, age
			expectedNames_afterMidThenNameAgeID shouldEqual actualUdfSortMidThenNameAgeID.map(smida => smida.sortedByID.map(ps => ps.name))

			// TODO why does the dataset contain all colnames while after collect() only the smni arg is available?
		}





		// TESTING: sorting using transform, array_sort, map_from_entries

		describe("sorting using: array_sort + transform + map_from_entries") {


			/**
			 * WAY 1: sql string code
			 *
			 * SOURCES:
			 * 	- https://hyp.is/OM9XcvT4Ee6oIhtawMKnnQ/archive.ph/2021.05.23-062738/https://towardsdatascience.com/did-you-know-this-in-spark-sql-a7398bfcc41e
			 */
			it("way 1: using sql string code") {

				val actualDf: DataFrame = personDf.withColumn("sortedByMiddle", expr(
					"array_sort(yourArray,	(left, right) -> case when left.middleInitialThrice < right.middleInitialThrice then -1 when left.middleInitialThrice > right.middleInitialThrice then 1 else 0 end)"))

				expectedUdfComparatorMidSortSeq shouldEqual actualDf.as[SortByMidStruct[PersonStruct]]
			}

			/**
			 * WAY 2: spark code
			 *
			 * SOURCES:
			 * Converting to Map:
			 * 	- https://sparkbyexamples.com/spark/spark-sql-map-functions/#map-from-entries
			 * 	- https://sparkbyexamples.com/spark/spark-how-to-convert-structtype-to-a-maptype/
			 *      Sorting by keys from a map type:
			 * 	- https://stackoverflow.com/questions/72652903/return-map-values-sorted-by-keys?rq=3
			 * 	- https://stackoverflow.com/questions/65929879/sort-by-key-in-map-type-column-for-each-row-in-spark-dataframe#:~:text=You%20can%20first%20get%20the,two%20arrays%20using%20map_from_arrays%20function.
			 */
			it("way 2: using spark code") {

				// WARNING: prerequisite to have unique map keys

				// Rearranging the struct to be nested so it can be converted to map (from two pairs)
				val twoFieldsDf: DataFrame = (personUniqueMidDf
					.withColumn("twoFields", transform(
						col("yourArray"),
						elem => struct(elem.getField("middleInitialThrice"),
							struct(
								elem.getField("id"),
								elem.getField("name"),
								elem.getField("addressNumber"),
								elem.getField("age")
							)
						)
					)))

				// Creating map from the nested struct
				val mapFieldsDf: DataFrame = (twoFieldsDf
					.withColumn("mapEntries", map_from_entries(col("twoFields"))))

				// Sorting by map keys
				val sortedNestedStructDf: DataFrame = (mapFieldsDf
					.withColumn("sortedValues", transform(
						array_sort(map_keys(col("mapEntries"))),
						k => struct(k.as("middleInitialThrice"), col("mapEntries")(k).as("rest"))
					)))

				// Rearranging the struct to be flattened and in the same order as PersonStruct
				val sortedStructDf: DataFrame = sortedNestedStructDf.withColumn("sortedStructs", transform(col("sortedValues"),
					stc => struct(stc.getField("rest").getField("id"),
						stc.getField("rest").getField("name"),
						stc.getField("middleInitialThrice"),
						stc.getField("rest").getField("addressNumber"),
						stc.getField("rest").getField("age")
					)))

				val actualArraySortTransformMapMidSort: Dataset[SortByMidStruct[PersonStruct]] = (sortedStructDf
					.withColumnRenamed("sortedStructs", "sortedByMiddle")
					.as[SortByMidStruct[PersonStruct]])

				expectedArraySortTransformMapMidSort shouldEqual actualArraySortTransformMapMidSort.collect.toSeq
			}
		}


		// TESTING: explode + sort on columns
		it("sorting using: explode + map_from_entries() + sort() + groupBy") {


			import utilities.DataHub.ManualDataFrames.ArrayDf._

			/**
			 * SOURCES:
			 * 	- sort on column = https://medium.com/@sfranks/i-had-trouble-finding-a-nice-example-of-how-to-have-an-udf-with-an-arbitrary-number-of-function-9d9bd30d0cfc
			 * 	- sort each col = https://sparkbyexamples.com/spark/spark-sort-column-in-descending-order/
			 * 	- https://sparkbyexamples.com/spark/spark-how-to-sort-dataframe-column-explained/
			 *
			 */

			val explodeSortDf_1: DataFrame = (personDf
				.withColumn("twoFields", transform(col("yourArray"), elem =>
					struct(
						elem.getField("middleInitialThrice"),
						elem.getField("id")))
				)
				.withColumn("explodeElem", explode(col("twoFields")))
				//.withColumn("mapEntries", map_from_entries(col("twoFields")))
				.withColumn("toMap", map_from_entries(array(col("explodeElem"))))
				.select(col("groupingKey"), col("explodeElem"), explode(col("toMap"))))


			val explodeSortDf_2a: DataFrame = (explodeSortDf_1
				.sort(col("groupingKey").asc, col("key").asc, col("value").asc) // sorting on the property
				.groupBy("groupingKey").agg(collect_list(col("explodeElem")).as("sortedByMiddle")) // grouping to make array of structs again
				)

			val explodeSortDf_2b: DataFrame = (explodeSortDf_1
				.sort(col("explodeElem.middleInitialThrice").asc, col("explodeElem.id").asc)
				.groupBy("groupingKey").agg(collect_list(col("explodeElem")).as("sortedByMiddle"))
				)

			explodeSortDf_2a.collect.toSeq shouldEqual explodeSortDf_2b.collect.toSeq

			// ---------------------------

			val actualExplodeSortTups: Array[Seq[(String, Int)]] = explodeSortDf_2a.as[SortByMidStruct[PersonMidIDStruct]].collect().map(dpm => dpm.sortedByMiddle.map(ps => (ps.middleInitialThrice, ps.id)))

			actualExplodeSortTups shouldEqual expectedExplodeSortTups

			// --------------------------

			val explodeSortDf_3: Dataset[Row] = (personDf
				.withColumn("explodeElems", explode(col("yourArray")))
				.sort(col("explodeElems.middleInitialThrice").asc,
					col("explodeElems.id").asc,
					col("explodeElems.age").asc,
					col("explodeElems.addressNumber").asc,
					col("explodeElems.name").asc)
				.drop(col("yourArray"))
				.groupBy("groupingKey")
				.agg(collect_list(col("explodeElems")).as("sorted"))
				.sort(col("groupingKey").asc)
				/*.as[SortStruct[PersonMidFirstStruct]]*/)

			val actualExplodeMultiSortTups: Seq[Seq[PersonStruct]] = (explodeSortDf_3
				.as[SortStruct[PersonStruct]]
				.collect.toSeq
				.map(dgs => dgs.sorted.sortBy(ps => (ps.middleInitialThrice, ps.id, ps.age, ps.addressNumber, ps.name)))
				)


			actualExplodeMultiSortTups shouldEqual expectedExplodeMultiSortTups
		}







		// TESTING: (explode) + grouping, array_sort + on property
		/**
		 * SOURCES:
		 * 	- https://hyp.is/cm1Z7vBCEe6jf0Piuu3GLg/www.geeksforgeeks.org/sorting-an-array-of-a-complex-data-type-in-spark/
		 */
		it("sorting using: explode + grouping + array_sort on property") {

			val explodeArraySortDf_1: DataFrame = (personDf
				.withColumn("explodeElems", explode(col("yourArray")))
				.groupBy("groupingKey")
				.agg(array_sort(collect_list(struct(
					col("explodeElems.middleInitialThrice"),
					col("explodeElems.name"),
					col("explodeElems.id")

				))).as("sortedByMiddle")))

			val actualExplodeArraySortTups: Seq[Seq[(String, String, Int)]] = (explodeArraySortDf_1
				.as[SortByMidStruct[PersonMidNameIDStruct]]
				.collect().toSeq
				.map(strct => strct.sortedByMiddle.map(p => (p.middleInitialThrice, p.name, p.id))))

			expectedExplodeArraySortTups shouldEqual actualExplodeArraySortTups
		}





		// TESTING:  udf + sortby on property + seq[Row] way

		/**
		 * SOURCES:
		 * 	- https://medium.com/@sfranks/i-had-trouble-finding-a-nice-example-of-how-to-have-an-udf-with-an-arbitrary-number-of-function-9d9bd30d0cfc
		 * 	- https://hyp.is/ykf9tPHREe6drgNjikNkyQ/newbedev.com/how-to-sort-array-of-struct-type-in-spark-dataframe-by-particular-column
		 * 	- https://stackoverflow.com/questions/49671354/how-to-sort-array-of-struct-type-in-spark-dataframe-by-particular-field
		 * 	- https://stackoverflow.com/questions/59999974/scala-spark-udf-filter-array-of-struct
		 * 	- https://stackoverflow.com/questions/47507767/sort-array-of-structs-in-spark-dataframe?rq=3
		 * 	- https://stackoverflow.com/questions/59901941/spark-udf-to-custom-sort-array-of-structs?rq=3
		 * 	- https://stackoverflow.com/questions/38739124/how-to-sort-arrayrow-by-given-column-index-in-scala?rq=3
		 */

		describe("sorting using: udf + sortBy on property + Seq[Row] as argument to udf, converting from Row to Class as intermediate step in udf") {


			// SOURCE: https://stackoverflow.com/questions/59901941/spark-udf-to-custom-sort-array-of-structs?rq=3
			it("(stackoverflow example)") {


				// WARNING: never include the data declaration here because the udf will give error "No TypeTag available for Score"
				// SOURCE = https://intellipaat.com/community/18751/scala-spark-app-with-no-typetag-available-error-in-def-main-style-app
				// case class Score(id: Int, num: Int)

				// -----------------------------------------
				// Way 1: creating this data set and grouping by id

				val inputDf: DataFrame = Seq((1, 2, 1), (1, 3, -3), (1, 4, 2)).toDF("id1", "id2", "num")

				val tempDf: DataFrame = (inputDf
					.groupBy(col("id1"))
					.agg((collect_set(struct(col("id2"), col("num")))).as("scoreList")))

				// -----------------------------------------
				// Way 2: creating this data set and showing manually how to input data so result looks like the result after grouping (tempDf)
				val data: Seq[Row] = Seq(
					Row(1, Array(Row(2, 1), Row(3, -3), Row(4, 2))),
				)
				val innerSchema: StructType = new StructType().add("id2", IntegerType).add("num", IntegerType)
				val fullSchema: StructType = new StructType().add("id1", IntegerType).add("scoreList", ArrayType(innerSchema))

				val df: DataFrame = sparkSessionWrapper.createDataFrame(sparkSessionWrapper.sparkContext.parallelize(data), fullSchema)

				// ------------------------------------------

				// TODO temp and df are interchangeable

				val funcGivenSeqRowSortToScores: Seq[Row] => Score = (lst: Seq[Row]) => {
					lst.map { case Row(n: Int, age: Int) => Score(n, age) }.minBy(_.num)
				}

				val udfSortScoreList: UserDefinedFunction = udf(funcGivenSeqRowSortToScores(_: Seq[Row]): Score)

				val resultSeqRowUdfDf: DataFrame = (df
					.select(col("id1"), udfSortScoreList(col("scoreList")).as("result"))
					.select(col("id1"), col("result.*")))

				resultSeqRowUdfDf.collectAll shouldEqual Seq(Row(1, Row(3, -3)))

			}


			it("(person example)") {

				val funcGivenSeqRowSortPersons: Seq[Row] => Seq[PersonStruct] = (lst: Seq[Row]) => {
					lst.map { case Row(id: Int, name: String, mid: String, addr: String, age: Int) => PersonStruct(id, name, mid, addr, age) }
						.sortBy((p: PersonStruct) => (p.middleInitialThrice, p.name, p.id))
				}
				val udfSortPersons: UserDefinedFunction = udf(funcGivenSeqRowSortPersons(_: Seq[Row]): Seq[PersonStruct])

				val resultUdfSortByPropertyUsingSeqRowToClass: Seq[SortStruct[PersonStruct]] = (personDf
					.select(col("groupingKey"), udfSortPersons(col("yourArray")).as("sorted"))
					.as[SortStruct[PersonStruct]]
					.collect().toSeq)

				expectedUdfSortByPropertyUsingSeqRowToClass shouldEqual resultUdfSortByPropertyUsingSeqRowToClass
				expectedUdfSortByPropertyUsingSeqRowToClass shouldEqual expectedUdfSortByPropertyUsingSeqAccessRowToClass

			}
		}






		// TESTING: udf + sortBy on property + class/record/dataset
		/**
		 * SOURCES: (Filtered feature)
		 * 	- https://stackoverflow.com/questions/59999974/scala-spark-udf-filter-array-of-struct
		 */
		// NOTE: this method keeps the groupingKey even without explicitly including it
		it("sorting using: class/dataset + udf + sortBy on property") {


			def funcGivenSeqRowAccessThenSortPersons(seq: Seq[Row]): Seq[PersonStruct] = (seq
				.sortBy((row: Row) => (row.getAs[String]("middleInitialThrice"), row.getAs[String]("name"), row.getAs[Int]("id")))
				.map(row => PersonStruct(
					row.getInt(0),
					row.getString(1),
					row.getString(2),
					row.getString(3),
					row.getInt(4)))
				)

			val udfSortPersons: UserDefinedFunction = udf(funcGivenSeqRowAccessThenSortPersons(_: Seq[Row]): Seq[PersonStruct])

			val resultUdfSortByPropertyUsingSeqAccessRowToClass: Seq[SortStruct[PersonStruct]] = (personDf
				.withColumn("sorted", udfSortPersons(col("yourArray")))
				.drop("yourArray")
				.as[SortStruct[PersonStruct]]
				.collect().toSeq)

			expectedUdfSortByPropertyUsingSeqAccessRowToClass shouldEqual resultUdfSortByPropertyUsingSeqAccessRowToClass

		}



		// TESTING: using sort + class/object/dataset/rdd property way

		/**
		 * SOURCES: (YourStruct)
		 * 	- https://stackoverflow.com/questions/54954732/spark-scala-filter-array-of-structs-without-explode
		 * 	- https://stackoverflow.com/questions/28543510/spark-sort-records-in-groups
		 * 	- https://stackoverflow.com/questions/62218496/how-to-convert-a-dataframe-map-column-to-a-struct-column/62218822#62218822
		 */
		it("sorting using: class/dataset + sortBy on property") {

			import sparkSessionWrapper.sqlContext.implicits._

			val resultSortByPropertyOnClass: Dataset[(String, Seq[PersonStruct])] = (personDs
				.map((record: Record) => (record.groupingKey, record.yourArray.sortBy((p: PersonStruct) => p.middleInitialThrice))))

			expectedSortByPropertyOnClass shouldEqual resultSortByPropertyOnClass
		}



		// TESTING: class/dataset + sortBy on property + groupByKey, mapGroups
		/**
		 * SOURCES: (Record)
		 * 	- https://hyp.is/HKPEUvLhEe6w6uuMg43GDQ/newbedev.com/how-to-sort-array-of-struct-type-in-spark-dataframe-by-particular-column
		 */
		it("sorting using: class/dataset + groupByKey, mapGroups + sortBy on property") {

			val resultGroupByKeySortByPropertyOnClass_1: Dataset[(String, Seq[(Int, String, String, String, Int)])] = personRecDs.groupByKey(_.groupingKey).mapGroups((groupKey: String, objs: Iterator[RecordRaw]) => (groupKey, objs.toSeq.flatMap(_.yourArray.sortBy(_._3))))

			val resultGroupByKeySortByPropertyOnClass_2: Dataset[(String, Seq[PersonStruct])] = personDs.groupByKey(_.groupingKey).mapGroups((groupKey: String, objs: Iterator[Record]) => (groupKey, objs.toSeq.flatMap(_.yourArray.sortBy(_.middleInitialThrice))))

			resultGroupByKeySortByPropertyOnClass_1.map(tup => tup._2.map(p => PersonStruct(p._1, p._2, p._3, p._4, p._5))) shouldEqual resultGroupByKeySortByPropertyOnClass_2
		}

	}
}
