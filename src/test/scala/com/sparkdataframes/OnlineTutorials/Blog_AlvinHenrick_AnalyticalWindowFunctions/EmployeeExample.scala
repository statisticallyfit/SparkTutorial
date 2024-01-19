package com.sparkdataframes.OnlineTutorials.Blog_AlvinHenrick_AnalyticalWindowFunctions

import utilities.SparkSessionWrapper

import org.apache.spark.sql.functions.{first, first_value}
import org.apache.spark.sql.{Column, ColumnName, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, column, expr, row_number, rank, dense_rank, percent_rank, ntile, cume_dist, lag, lead, min, max, avg, sum, count}
// rangeBetween, rowsBetween

import org.apache.spark.sql.expressions.{Window, WindowSpec}

import com.github.mrpowers.spark.fast.tests.DataFrameComparer


/**
 * Source = https://alvinhenrick.com/2017/05/16/apache-spark-analytical-window-functions/
 */
object EmployeeExample extends App with SparkSessionWrapper with DataFrameComparer {


	//val spark: SparkSession = SparkSession.builder().master("local[1]").appName("alvinhenrickexample").getOrCreate()
	import sparkSessionWrapper.implicits._


	// Create dataframe
	val empDf: DataFrame = sparkSessionWrapper.createDataFrame(Seq(
		(7369, "Smith", "Clerk", 7902, "17-Dec-80", 800, 20, 10),
		(7499, "Allen", "Salesman", 7698, "20-Feb-81", 1600, 300, 30),
		(7521, "Ward", "Salesman", 7698, "22-Feb-81", 1250, 500, 30),
		(7566, "Jones", "Manager", 7839, "2-Apr-81", 2850, 0, 20),
		(7654, "Marin", "Salesman", 7698, "28-Sep-81", 1250, 1400, 30),
		(7698, "Blake", "Manager", 7839, "1-May-81", 2850, 0, 30),
		(7782, "Clark", "Manager", 7839, "9-Jun-81", 2850, 0, 10),
		(7788, "Scott", "Analyst", 7566, "19-Apr-87", 3000, 0, 20),
		(7839, "King", "President", 0, "17-Nov-81", 5000, 0, 10),
		(7844, "Turner", "Salesman", 7698, "8-Sep-81", 1500, 0, 30),
		(7876, "Adams", "Clerk", 7788, "23-May-87", 800, 0, 20),
		// Extra rows here to ensure large enough partitions
		(7342, "Johanna", "Manager", 8923, "14-Apr-99", 2343, 1, 11),
		(5554, "Patrick", "Manager", 8923, "12-May-99", 2343, 2, 5),
		(2234, "Stacey", "Manager", 8923, "5-May-01", 3454, 4, 8),
		(2343, "Jude", "Analyst", 8923, "5-Sept-22", 6788, 3, 5),
		(5676, "Margaret", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
		(5676, "William", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
		(5676, "Bridget", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
		(5676, "Kieroff", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
		(5676, "Quinlin", "Analyst", 8923, "3-Nov-23", 6787, 7, 2),
		(6787, "Sarah", "Analyst", 8923, "17-Jun-78", 6788, 1, 2),
		(2342, "David", "President", 8923, "23-Jan-89", 500, 11, 1),
		(2345, "Christian", "Clerk", 8923, "31-Jul-98", 2343, 12, 10),
		(3456, "John", "Salesman", 8923, "21-Dec-00", 2343, 21, 21),
		(7898, "Lizbeth", "President", 8923, "11-Oct-11", 2343, 22, 34),

	)).toDF("EmpNum", "EmpName", "Job", "Mgr", "Hiredate", "Salary", "Comm", "DeptNum")

	val LEN: Int = empDf.count().toInt

	// Create the window partition - partition by department, order by salary descending
	val windowPartOrdSpec: WindowSpec = Window.partitionBy($"Job").orderBy($"Salary".desc)

	//val viewCols: Seq[ColumnName] = List($"EmpName", $"Job", $"Salary")
	val viewCols: Seq[String] = List("EmpName", "Job", "Salary")
	val dropCols: Seq[String] = List("EmpNum", "Mgr", "Hiredate", "Comm", "DeptNum")


	// NOTE: to do sql query way must create temporary view
	empDf.createOrReplaceTempView("empDf")



	/**
	 * Row number
	 */
	val rowNumSqlDf: DataFrame = sparkSessionWrapper.sql("SELECT EmpName, Job, Salary, ROW_NUMBER() OVER (partition by Job ORDER BY Salary desc) as RowNumberResultSql FROM empDf;")
	rowNumSqlDf.show(LEN)

	val rowNumDf: DataFrame = empDf.withColumn(colName = "RowNumberResult", col = row_number().over(windowPartOrdSpec))
	rowNumDf.show(LEN)

	assertSmallDataFrameEquality(rowNumSqlDf, rowNumDf)


	/**
	 * Rank
	 */

	val rankSqlDf: DataFrame = sparkSessionWrapper.sql("SELECT EmpName,Job,Salary,RANK() OVER (partition by Job ORDER BY Salary desc) as RankResultSql FROM empDf;")
	rankSqlDf.show(LEN)

	val rankDf: DataFrame = empDf.withColumn(colName = "RankResult", col = rank().over(windowPartOrdSpec))
	rankDf.show(LEN)

	val rankDf2: DataFrame = empDf
		.select($"EmpName", $"DeptNum", $"Job", $"Salary",
			rank().over(windowPartOrdSpec).as("RankResult2") // NOTE: select takes list * of Columns
		)
	rankDf2.show(LEN)

	assertSmallDataFrameEquality(rankSqlDf, rankDf)
	assertSmallDataFrameEquality(rankDf, rankDf2)


	/**
	 * Dense rank salary within each department
	 */
	//SELECT EmpName,Job,Salary,RANK() OVER (partition by Job ORDER BY Salary desc) as rank FROM empDf;")
	val denseRankSqlDf: DataFrame = sparkSessionWrapper.sql("SELECT EmpName,Job, Salary, DENSE_RANK() OVER (partition by Job ORDER BY Salary desc) as DenseRankResultSql FROM empDf;")
	denseRankSqlDf.show(LEN)



	// NOTE: doesn't work to _* view cols and then add another col afterward
	/*val denseRankDf: DataFrame = empDf.select(viewCols:_*,
		dense_rank().over(windowEmpSpec) as "DenseRankResult"
	)*/
	val denseRankDf: DataFrame = empDf
		.withColumn("DenseRankResult", col = dense_rank().over(windowPartOrdSpec))
		.drop(dropCols:_*)
	denseRankDf.show(LEN)

	assertSmallDataFrameEquality(denseRankSqlDf, denseRankDf)


	/**
	 * Aggregation functions
	 */
	val colnamesAggBase: List[String] = List("SumResult", "AvgResult", "MinResult", "MaxResult")
	val colnamesAggSimple: Seq[Column] = (viewCols ++ colnamesAggBase).map(col(_))
	val colnamesAggSql: Seq[Column] = (viewCols ++ colnamesAggBase.map(_ + "Sql")).map(col(_))

	// Source for sql query format = https://stackoverflow.com/questions/1896102/applying-multiple-window-functions-on-same-partition
	val aggSqlDf: DataFrame = sparkSessionWrapper.sql(
		"""SELECT EmpName,Job,Salary,
		  |sum(Salary) OVER w AS SumResultSql,
		  |avg(Salary) OVER w AS AvgResultSql,
		  |min(Salary) OVER w AS MinResultSql,
		  |max(Salary) OVER w AS MaxResultSql
		  |FROM empDf
		  |WINDOW w AS (PARTITION BY Job ORDER BY Salary desc);""".stripMargin)

	val aggSqlDistinctDf: DataFrame = aggSqlDf
		.drop(col("EmpName"))
		//.select(("Job" +: colnamesAggBase.map(_ + "Sql")).map(col(_)):_*)
		.dropDuplicates("Job") // weird have to specify the colname here else it keeps replicates of each job type, but for aggDf below it works without colname given

	aggSqlDistinctDf.show(LEN)



	val windowPartSpec: WindowSpec = Window.partitionBy("Job")

	val aggDf: DataFrame = empDf.withColumns(Map(
		"SumResult" -> sum(col("Salary")).over(windowPartSpec),
		"AvgResult" -> avg(col("Salary")).over(windowPartSpec),
		"MinResult" -> min(col("Salary")).over(windowPartSpec),
		"MaxResult" -> max(col("Salary")).over(windowPartSpec),
	)).select(colnamesAggSimple: _*)


	// To drop duplicates must first include only the column with duplicates and the contrasting columns (ex: Just Job not Salary and not EmpName)
	val aggDistinctDf: DataFrame = aggDf.drop(col("EmpName")).dropDuplicates("Job")
	aggDistinctDf.show

	/// HELP why are these not the same??
	//assertSmallDataFrameEquality(aggSqlDistinctDf, aggDistinctDf)


	/**
	 * LEAD function = Lead function allows us to compare current row with subsequent rows within each partition depending on the second argument (offset) which is by default set to 1 i.e. next row but you can change that parameter 2 to compare against every other row.
	 * Source = https://hyp.is/qcayiJwxEe6xaGco8YERzA/alvinhenrick.com/2017/05/16/apache-spark-analytical-window-functions/
	 */
	val leadSqlDf: DataFrame = sparkSessionWrapper.sql("SELECT EmpName, Job, Salary, lead(Salary) OVER (PARTITION BY Job ORDER BY Salary desc) AS Subsequent_LeadResultSql FROM empDf;")

	val leadDf: DataFrame = empDf
		.withColumn("Subsequent_LeadResult", col = lead($"Salary", offset = 1).over(windowPartOrdSpec))
		.drop(dropCols:_*)
	// Another way
	//val leadDf2: DataFrame = empDf.select(List("EmpName", "Job" ,"Salary").map(col(_)), lead("Salary", offset = 1).over(windowPartOrdSpec).as("LeadResult"))
	val leadDf2: DataFrame = empDf
		.select($"*", lead("Salary", 1).over(windowPartOrdSpec).as("Subsequent_LeadResult"))
		.drop(dropCols:_*)
	leadDf2.show(LEN)

	assertSmallDataFrameEquality(leadSqlDf, leadDf)
	assertSmallDataFrameEquality(leadDf, leadDf2)

	/**
	 *  LAG = Lag function allows us to compare current row with preceding rows within each partition depending on the second argument (offset) which is by default set to 1 i.e. previous row but you can change that parameter 2 to compare against every other preceding row.The 3rd parameter is default value to be returned when no preceding values exists or null.
	 *  Source = https://hyp.is/13MChJxHEe6avke4isb29w/alvinhenrick.com/2017/05/16/apache-spark-analytical-window-functions/
	 */
	val lagSqlDf: DataFrame = sparkSessionWrapper.sql("SELECT EmpName,Job,Salary,lag(Salary) OVER (PARTITION BY Job ORDER BY Salary DESC) AS Preceding_LagResultSql FROM empDf;")
	lagSqlDf.show(LEN)

	val lagDf: DataFrame = empDf
		.withColumn(colName = "Preceding_LagResult", col = lag("Salary", 1).over(windowPartOrdSpec))
		.drop(dropCols:_*)

	val lagDf2: DataFrame = empDf
		.select($"*", lag("Salary", 1).over(windowPartOrdSpec).as("Preceding_LagResult2"))
		.drop(dropCols:_*)

	assertSmallDataFrameEquality(lagSqlDf, lagDf)
	assertSmallDataFrameEquality(lagDf, lagDf2)


	/**
	 * FIRST = First value within each partition .
	 * i.e. highest salary (we are using order by descending) within each department can be compared against every member within each department.
	 */
	val firstSqlDf: DataFrame = sparkSessionWrapper.sql("SELECT EmpName,Job,Salary, first(Salary) OVER (PARTITION BY Job ORDER BY Salary DESC) AS FirstResultSql FROM empDf;")
	firstSqlDf.show(LEN)

	val firstDf: DataFrame = empDf
		.withColumn(colName = "FirstResult", col = first_value($"Salary").over(windowPartOrdSpec))
		.drop(dropCols:_*)
	val firstSimpleDf: DataFrame = firstDf.dropDuplicates("Job")
	firstSimpleDf.show

	val firstDf2: DataFrame = empDf
		.select($"*", first($"Salary").over(windowPartOrdSpec).as("FirstResult2"))
		.drop(dropCols:_*)

	assertSmallDataFrameEquality(firstSqlDf, firstDf)
	assertSmallDataFrameEquality(firstDf, firstDf2)


	// TODO research how to partition by multiple columns
	// EX: Window.partitionBy($"DeptNum", $"Job").orderBy($"Salary".desc)


	// TODO compare GROUP BY to window partitioning and applying the agg function in that partition = https://hyp.is/dk7W-pxpEe6SOPPD4cwYgQ/www.sparkcodehub.com/spark-dataframe-aggregations
}
