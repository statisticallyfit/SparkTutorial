package com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming.util


import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * SOURCE = https://github.com/bartosz25/spark-scala-playground/blob/d4dae02098169e9f4241f3597dc4864421237881/src/test/scala/com/waitingforcode/util/store/InMemoryKeyedStore.scala#L8
 *
 * In memory data store used in tests assertions.
 */
object InMemoryKeyedStore {

	private val Data = new mutable.HashMap[String, mutable.ListBuffer[String]]()

	def addValue(key: String, value: String) = {
		Data.synchronized {
			val values = Data.getOrElse(key, new mutable.ListBuffer[String]())
			values.append(value)
			Data.put(key, values)
		}
	}

	def getValues(key: String): mutable.ListBuffer[String] = Data.getOrElse(key, ListBuffer.empty)

	def allValues = Data
}