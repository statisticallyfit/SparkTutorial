package com

/**
 * Source of the code = https://github.com/archena/spark-koans/blob/master/src/test/scala/spark.koans/testSparkContext.scala
 */

import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkContextForTests extends Suite with BeforeAndAfterAll {
	var sc: SparkContext = _

	override def beforeAll() {
		sc = new SparkContext("local", "Testing", System.getenv("SPARK_HOME"))
	}

	override def afterAll() {
		sc.stop()
	}
}