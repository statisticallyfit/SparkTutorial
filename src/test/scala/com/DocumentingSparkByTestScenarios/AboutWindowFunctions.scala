package com.DocumentingSparkByTestScenarios


import org.apache.spark.sql.{Column, ColumnName, Row, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{avg, col, column, count, cume_dist, dense_rank, expr, lag, lead, max, min, ntile, percent_rank, rank, row_number, sum} // rangeBetween, rowsBetween

import org.apache.spark.sql.expressions.{Window, WindowSpec}

import com.SparkSessionForTests
import org.scalatest.TestSuite
import scala.reflect.runtime.universe._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should._

import com.github.mrpowers.spark.fast.tests.DataFrameComparer

import org.scalatest.Assertions._ // intercept

/**
 *
 */
class AboutWindowFunctions extends AnyFunSpec with Matchers //with TestSuite
	with CustomMatchers
	with SparkSessionForTests
	with DataFrameComparer {

	import sparkTestsSession.implicits._


	import com.data.util.DataHub.ManualDataFrames.fromAlvinHenrickBlog._


	val LEN: Int = empDf.count().toInt



	describe("Ranking window functions") {

		// Create the window partition - partition by department, order by salary descending
		// NOTE: ranking and analytic functions require partition + order
		val windowRankSpec: WindowSpec = Window.partitionBy($"Job").orderBy($"Salary".desc)



		describe("row()"){


			val rowNumSqlDf: DataFrame = sparkTestsSession.sql("SELECT EmpName, Job, Salary, ROW_NUMBER() OVER (partition by Job ORDER BY Salary desc) as RowNumberResultSql FROM empDf;")

			val rowNumDf: DataFrame = empDf
				.withColumn(colName = "RowNumberResult", col = row_number().over(windowRankSpec))
				.drop(dropCols:_*)


			it("provides a row id to each record within a partition ( so each partition starts a fresh set of ids ). "){
				//assertSmallDataFrameEquality(rowNumSqlDf, rowNumDf)
				rowNumSqlDf should equalDataFrame(rowNumDf)

				val expectedRowNumDf: DataFrame = Seq(
					("Jude", "Analyst", 6788, 1),
					("Sarah", "Analyst", 6788, 2),
					("Margaret", "Analyst", 6788, 3),
					("William", "Analyst", 6788, 4),
					("Bridget", "Analyst", 6788, 5),
					("Kieroff", "Analyst", 6788, 6),
					("Quinlin", "Analyst", 6788, 7),
					("Scott", "Analyst", 6788, 8),

					("Christian", "Clerk", 2343, 1),
					("Smith", "Clerk", 800, 2),
					("Adams", "Clerk", 800, 3),

					("Stacey", "Manager", 3454, 1),
					("Jones", "Manager", 2850, 2),
					("Blake", "Manager", 2850, 3),
					("Clark", "Manager", 2850, 4),
					("Johanna", "Manager", 2343, 5),
					("Patrick", "Manager", 2343, 6),

					("King", "President", 5000, 1),
					("Lizbeth", "President", 2343, 2),
					("David", "President", 500, 3),

					("John", "Salesman", 2343, 1),
					("Allen", "Salesman", 1600, 2),
					("Turner", "Salesman", 1500, 3),
					("Ward", "Salesman", 1250, 4),
					("Marin", "Salesman", 1250, 5)
				).toDF("EmpName", "Job", "Salary", "RowNumberResult")


				rowNumDf should equalDataFrame (expectedRowNumDf)
			}

			it("each partition id starts from the beginning"){

				// show how each partition starts with id == 1 and last one has id == length of partition. BUT HOW to separate out the partitions from the dataframe?
			}

		}

		describe ("rank()"){


			val rankBySqlDf: DataFrame = sparkTestsSession.sql("SELECT EmpName,Job,Salary,RANK() OVER (partition by Job ORDER BY Salary desc) as RankResultSql FROM empDf;")

			val rankByWithColDf: DataFrame = empDf.withColumn(colName = "RankResult", col = rank().over(windowRankSpec)).drop(dropCols:_*)

			// NOTE: select takes list * of Columns
			val rankBySelectDf: DataFrame = empDf
				.select($"EmpName", $"DeptNum", $"Job", $"Salary",
					rank().over(windowRankSpec).as("RankResult2")
				)




			it("provides a rank to the row within a window partition."){


				// way 1 test - with showing result
				it("showing via stating the resulting df"){


					val expectedRankDf: DataFrame = sparkTestsSession.createDataFrame(Seq(
						("Jude", "Analyst", 6788, 1),
						("Sarah", "Analyst", 6788, 1),
						("Margaret", "Analyst", 6788, 3),
						("William", "Analyst", 6788, 3),
						("Bridget", "Analyst", 6788, 3),
						("Kieroff", "Analyst", 6788, 3),
						("Quinlin", "Analyst", 6788, 3),
						("Scott", "Analyst", 6788, 8),

						("Christian", "Clerk", 2343, 1),
						("Smith", "Clerk", 800, 2),
						("Adams", "Clerk", 800, 2),

						("Stacey", "Manager", 3454, 1),
						("Jones", "Manager", 2850, 2),
						("Blake", "Manager", 2850, 2),
						("Clark", "Manager", 2850, 2),
						("Johanna", "Manager", 2343, 5),
						("Patrick", "Manager", 2343, 5),

						("King", "President", 5000, 1),
						("Lizbeth", "President", 2343, 2),
						("David", "President", 500, 3),

						("John", "Salesman", 2343, 1),
						("Allen", "Salesman", 1600, 2),
						("Turner", "Salesman", 1500, 3),
						("Ward", "Salesman", 1250, 4),
						("Marin", "Salesman", 1250, 4)
					))

					rankByWithColDf should equalDataFrame (expectedRankDf)
					rankBySelectDf should equalDataFrame (expectedRankDf)
					rankBySqlDf should equalDataFrame (expectedRankDf)
				}

				it("proving via explicitly showing the rank column"){

					val pairJobSalRank: Seq[(String, Int, Int)] = rankByWithColDf.select($"Job", $"Salary", $"RankResult").collect().toSeq.map(row => row.toSeq match { case Seq(j, s, r) => (j,s,r).asInstanceOf[(String, Int, Int)]})

					val (jobs, salaries, ranks): (Seq[String], Seq[Int], Seq[Int]) = pairJobSalRank.unzip3

					jobs should be (Seq.fill[String](8)("Analyst") ++ Seq.fill[String](3)("Clerk") ++ Seq.fill[String](6)("Manager") ++ Seq.fill[String](3)("President") ++ Seq.fill[String](5)("Salesman"))

					salaries should be (Seq(6788, 6788, 6787, 6787, 6787, 6787, 6787, 3000, 2343, 800, 800, 3454, 2850, 2850, 2850, 2343, 2343, 5000, 2343, 500, 2343, 1600, 1500, 1250, 1250))

					ranks should be (Seq(1, 1, 3, 3, 3, 3, 3, 8, 1, 2, 2, 1, 2, 2, 2, 5, 5, 1, 2, 3, 1, 2, 3, 4, 4) )
				}
			}

			it("leaves gap in the rank when there are ties in the ordered column"){

			}
		}
	}
}
