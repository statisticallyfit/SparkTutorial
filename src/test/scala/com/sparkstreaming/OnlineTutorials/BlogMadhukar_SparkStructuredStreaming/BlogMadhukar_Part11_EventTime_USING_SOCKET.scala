package com.sparkstreaming.OnlineTutorials.BlogMadhukar_SparkStructuredStreaming

import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.sql.Timestamp

/**
 * Source = https://blog.madhukaraphatak.com/introduction-to-spark-structured-streaming-part-11
 */
object BlogMadhukar_Part11_EventTime_USING_SOCKET extends App {

	val sparkSession: SparkSession = SparkSession
		.builder()
		.master("local[1]")
		.appName("BlogMadhukar_part11")
		.getOrCreate()
	// REPL
	// val sparkSession: SparkSession = SparkSession.builder().master("local[1]").appName("BlogMadhukar11").getOrCreate()

	import sparkSession.implicits._



	// Step 1: Reading data from the socket

	val socketStreamDS: Dataset[String] = sparkSession.readStream
		.format(source = "socket")
		.option(key = "host", value = "localhost")
		.option(key = "port", value = 50050)
		.load()
		.as[String]

	// Step 2: Extracting Time from Stream (from the stock price event)
	case class Stock(timeGenerated: Timestamp /*Long*/ , stockSymbol: String, stockValue: Double)

	val stockDS: Dataset[Stock] = socketStreamDS.map((value: String) => {
		val columns: Array[String] = value.split(",")
		val timestamp = new Timestamp(columns(0).toLong)

		Stock(timeGenerated = timestamp /*.getTime*/ , stockSymbol = columns(1), stockValue = columns(2).toDouble)
	})


	// Step 3 - Defining window on event time
	// Defining window which aggregates the stock value for the last 10 seconds
	// Like partitioning (stock prices) in groups of 10 seconds
	val windowedCount: DataFrame = stockDS
		.groupBy(window(timeColumn = $"timeGenerated", windowDuration = "10 seconds"))
		.sum(colNames = "stockValue") // TODO meaning of sum


	val query: DataStreamWriter[Row] /*: DataStreamWriter[Row]*/ = windowedCount.writeStream
		.format(source = "console")
		.option(key = "truncate", value = false)
		.outputMode(outputMode = OutputMode.Complete())

	query.start().awaitTermination(timeoutMs = 40000) // 40 ms = 40 s



	// ERROR program crashes before inputs are even given. Error:
	/**
	 *
	 *
	 * Exception in thread "main" org.apache.spark.sql.AnalysisException: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "window(timeGenerated, 10000000, 10000000, 0)" due to data type mismatch: Parameter 1 requires the ("(TIMESTAMP OR TIMESTAMP WITHOUT TIME ZONE)" or "STRUCT<start: TIMESTAMP, end: TIMESTAMP>" or "STRUCT<start: TIMESTAMP_NTZ, end: TIMESTAMP_NTZ>") type, however "timeGenerated" has the type "BIGINT".;
	 * 'Aggregate [window(timeGenerated#10L, 10000000, 10000000, 0)], [window(timeGenerated#10L, 10000000, 10000000, 0) AS window#17, sum(stockValue#12) AS sum(stockValue)#22]
	 * +- SerializeFromObject [knownnotnull(assertnotnull(input[0, com.sparkstreaming.OnlineTutorials.BlogMadhukar_Part11_SparkStructStream_EventTime$Stock, true])).timeGenerated AS timeGenerated#10L, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, com.sparkstreaming.OnlineTutorials.BlogMadhukar_Part11_SparkStructStream_EventTime$Stock, true])).stockSymbol, true, false, true) AS stockSymbol#11, knownnotnull(assertnotnull(input[0, com.sparkstreaming.OnlineTutorials.BlogMadhukar_Part11_SparkStructStream_EventTime$Stock, true])).stockValue AS stockValue#12]
	 * +- MapElements com.sparkstreaming.OnlineTutorials.BlogMadhukar_Part11_SparkStructStream_EventTime$$$Lambda$1408/0x0000000800cbe040@3abdc11d, class java.lang.String, [StructField(value,StringType,true)], obj#9: com.sparkstreaming.OnlineTutorials.BlogMadhukar_Part11_SparkStructStream_EventTime$Stock
	 * +- DeserializeToObject cast(value#0 as string).toString, obj#8: java.lang.String
	 * +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.TextSocketSourceProvider@624be1d4, socket, org.apache.spark.sql.execution.streaming.sources.TextSocketTable@336e17be, [host=localhost, port=50050], [value#0]
	 *
	 */


	// Inputs
	// 1461756862000,"aapl",500.0
	// 1461756867001,"aapl",600.0
	// 1461756872000,"aapl",400.0
	// 1461756867001,"aapl",200.0 // late event for Wed, 27 Apr, 2016, 11:34:27
}
