package com.DocumentingSparkByTestScenarios

import com.SparkSessionForTests
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

/**
 *
 */
object TestData extends SparkSessionForTests {

	//val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("sparkDocumentationByTesting").getOrCreate()


	object ImportedDataFrames {

		val PATH: String = "/development/projects/statisticallyfit/github/learningspark/SparkTutorial"



		val dataDamji: String = ???
		val dataHolden: String = ???
		val dataJeanGeorgesPerrin: String = ???

		object fromBillChambersBook {
			val dataBillChambers: String = "/src/main/scala/com/sparkdataframes/BookTutorials/BillChambers_SparkTheDefinitiveGuide/data"
			val flightDf: DataFrame = sparkTestsSession.read.format("json").load(PATH + dataBillChambers + "/flight-data")
		}


	}

	object ManualDataFrames {

		object fromAlvinHenrickBlog {

			val empDf: DataFrame = sparkTestsSession.createDataFrame(Seq(
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


			//val viewCols: Seq[ColumnName] = List($"EmpName", $"Job", $"Salary")
			val viewCols: Seq[String] = List("EmpName", "Job", "Salary")
			val dropCols: Seq[String] = List("EmpNum", "Mgr", "Hiredate", "Comm", "DeptNum")


			// NOTE: to do sql query way must create temporary view
			empDf.createOrReplaceTempView("empDf")
		}
	}
}
