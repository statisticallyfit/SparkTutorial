package com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming.util

import org.apache.spark.sql.ForeachWriter

/**
 * SOURCE = https://github.com/bartosz25/spark-scala-playground/blob/d4dae02098169e9f4241f3597dc4864421237881/src/test/scala/com/waitingforcode/util/NoopForeachWriter.scala
 * @tparam T
 */
class NoopForeachWriter[T] extends ForeachWriter[T] {
	override def open(partitionId: Long, version: Long): Boolean = true

	override def process(value: T): Unit = {}

	override def close(errorOrNull: Throwable): Unit = {}
}