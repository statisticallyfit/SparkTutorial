package com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime.sparkSocketExtension


/**
 * CODE SOURCE = https://github.com/buildlackey/spark-streaming-group-by-event-time/blob/master/src/main/scala/org/apache/spark/sql/execution/streaming/socket.scala
 */


// CLONE
//
// This file contains a copy of org.apache.spark.sql.execution.streaming.TextSocketSource
// modified to emit more logging information in the methods initialize(), getOffset() and getBatch().
// So that the additional logging does not overly slow down processing we don't print the content of every event
// received over the socket right away. We put those into a buffer, whose contents we can retrieve all
// in one shot via getMsgs().
//
// Note that we use the same package name as the original DataSource because the original classes accessed
// some methods that were only visible in the scope of this package.
//
// Our streaming job will use this modified class if we specify its name as an argument to format() when
// constructing a stream reader, as in:
//
//    sparkSession.readStream.format("org.apache.spark.sql.execution.streaming.TextSocketSourceProvider2")

import com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime.sparkSocketExtension.TextSocketSource2.msgs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders._ //ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.{LogicalRDD, QueryExecution}

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.Socket
//import scala.io.Source
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}
import javax.annotation.concurrent.GuardedBy
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition

// TODO why not imported by author?
// TODO is it Offset from execution OR catalyst OR connector???
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.execution.streaming.sources.TextSocketMicroBatchStream

import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.AnalysisException


// NOTE: Added by @statisticallyfit
private object UtilTextSocketSource2 {


	def helperOfRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[InternalRow] = {

		def helperWithActive[T](block: => T): T = {
			// Use the active session thread local directly to make sure we get the session that is actually
			// set and not the default session. This to prevent that we promote the default session to the
			// active session once we are done.
			/*private*/ val activeThreadSession = new InheritableThreadLocal[SparkSession]
			val old: SparkSession = /*SparkSession.*/ activeThreadSession.get()
			SparkSession.setActiveSession(sparkSession)
			try block finally {
				SparkSession.setActiveSession(old)
			}
		}

		// ------------------------------------------------------------------------------------

		/*sparkSession.withActive*/
		// NOTE: assigning result of this block to a value gives error "forward reference extends over definition of value datasetresult"
		/*val dataSetResult: Dataset[InternalRow] = */
		helperWithActive {

			val qe: QueryExecution = sparkSession.sessionState.executePlan(logicalPlan)
			qe.assertAnalyzed()

			val exRow: ExpressionEncoder[Row] = ExpressionEncoder(qe.analyzed.schema)
			val exIntRow: ExpressionEncoder[InternalRow] = exRow.asInstanceOf[ExpressionEncoder[InternalRow]] // TODO is this right?


			val rddFromQE: RDD[InternalRow] = qe.toRdd
			sparkSession.createDataset(rddFromQE)(exIntRow)

			// new Dataset[Row](qe, ex) // create helper for this too
		}

	}


	def helperInternalCreateDataFrame(selfSparkSession: SparkSession, catalystRows: RDD[InternalRow], schema: StructType, isStreaming: Boolean = false): DataFrame = {

		val logicalPlan = LogicalRDD(
			toAttributes(schema),
			catalystRows,
			isStreaming = isStreaming)(selfSparkSession)

		val dataSet: Dataset[InternalRow] = helperOfRows(selfSparkSession, logicalPlan)
		dataSet.toDF()
	}

}


object TextSocketSource2 {
	val SCHEMA_REGULAR = StructType(StructField("value", StringType) :: Nil)
	val SCHEMA_TIMESTAMP = StructType(StructField("value", StringType) ::
		StructField("timestamp", TimestampType) :: Nil)
	val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)

	val msgs: ListBuffer[String] = mutable.ListBuffer[String]() // CLONE

	def getMsgs(): List[String] = msgs.toList // CLONE
}

/**
 * A source that reads text lines through a TCP socket, designed only for tutorials and debugging.
 * This source will *not* work in production applications due to multiple reasons, including no
 * support for fault recovery and keeping all of the text read in memory forever.
 */
class TextSocketSource2(host: String, port: Int, includeTimestamp: Boolean, sqlContext: SQLContext)
	extends Source with Logging {


	@GuardedBy("this")
	private var socket: Socket = null

	@GuardedBy("this")
	private var readThread: Thread = null

	/**
	 * All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
	 * Stored in a ListBuffer to facilitate removing committed batches.
	 */
	@GuardedBy("this")
	protected val batches: ListBuffer[(String, Timestamp)] = new ListBuffer[(String, Timestamp)]

	@GuardedBy("this")
	protected var currentOffset: LongOffset = new LongOffset(-1)

	@GuardedBy("this")
	protected var lastOffsetCommitted: LongOffset = new LongOffset(-1)

	initialize()




	// NOTE: Added by @statisticallyfit
	import UtilTextSocketSource2._


	private def initialize(): Unit = synchronized {
		socket = new Socket(host, port)
		val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
		readThread = new Thread(s"TextSocketSource($host, $port)") {
			setDaemon(true)

			override def run(): Unit = {
				try {
					while (true) {
						val line: String = reader.readLine()
						TextSocketSource2.msgs += s"socket read at ${new Date().toString} line:" + line
						//System.out.println(s"socket read at ${new Date().toString} line:" + line);     // CLONE
						if (line == null) {
							// End of file reached
							logWarning(s"Stream closed by $host:$port")
							return
						}
						TextSocketSource2.this.synchronized {
							val newData: (String, Timestamp) = (line,
								Timestamp.valueOf(
									TextSocketSource2.DATE_FORMAT.format(Calendar.getInstance().getTime()))
							)
							currentOffset = currentOffset + 1
							batches.append(newData)
						}
					}
				} catch {
					case e: IOException =>
				}
			}
		}
		readThread.start()
	}

	/** Returns the schema of the data from this source */
	override def schema: StructType = if (includeTimestamp) TextSocketSource2.SCHEMA_TIMESTAMP
	else TextSocketSource2.SCHEMA_REGULAR

	override def getOffset: Option[Offset] = synchronized {
		val retval = if (currentOffset.offset == -1) {
			None
		} else {
			Some(currentOffset)
		}
		println(s" at ${new Date().toString} getOffset: " + retval) // CLONE
		retval
	}

	/** Returns the data that is between the offsets (`start`, `end`]. */
	override def getBatch(startOpt: Option[Offset], end: Offset): DataFrame = synchronized {
		println(s" at ${new Date().toString} getBatch start:" + startOpt + ". end: " + end) // CLONE

		// First convert start option-type to offset-type
		val startOffset = startOpt.getOrElse(LongOffset(-1))
		/*val startOrdinal =
			start.flatMap(LongOffset.convert).getOrElse(LongOffset(-1)).offset.toInt + 1

		val endOrdinal = LongOffset.convert(end).getOrElse(LongOffset(-1)).offset.toInt + 1*/


		// NOTE: errors above, changing to below
		// SOURCE: https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/TextSocketMicroBatchStream.scala#L111-L112
		val startOrdinal: Int = startOffset.asInstanceOf[LongOffset].offset.toInt + 1
		val endOrdinal: Int = end.asInstanceOf[LongOffset].offset.toInt + 1

		// Internal buffer only holds the batches after lastOffsetCommitted
		val rawList: ListBuffer[(String, Timestamp)] = synchronized {
			val sliceStart: Int = startOrdinal - lastOffsetCommitted.offset.toInt - 1
			val sliceEnd: Int = endOrdinal - lastOffsetCommitted.offset.toInt - 1
			batches.slice(sliceStart, sliceEnd)
		}

		import sqlContext.sparkSession.implicits._

		/*// TODO put this here not sure if correct
		val numPartitions: Int = rawList.length

		val slices: ListBuffer[ListBuffer[(UTF8String, Long)]] = ListBuffer.fill(numPartitions)(new ListBuffer[(UTF8String, Long)])

		rawList.zipWithIndex.foreach { case ((str: String, timestamp: Timestamp), idx: Int) =>
			slices(idx % numPartitions) += (str, timestamp.getTime)   // .append(r)
		}

		slices.map(TextSocketInputPartition).toSeq.toDF()*/


		/// -----
		//val vs = rawList.map { case (str, timestamp) => InternalRow(UTF8String.fromString(str), timestamp.getTime)}

		val rdd: RDD[InternalRow] = sqlContext.sparkContext
			.parallelize(rawList.toSeq)
			.map {
				case (v, ts) =>
					//println(s" to row at ${new Date().toString} $v")
					InternalRow(UTF8String.fromString(v), ts.getTime)
					//Row(UTF8String.fromString(v), ts.getTime)
			}

		// HELP internal data frame func is private to sql package!! cannot use it som ust make regular dataframe?
		helperInternalCreateDataFrame(sqlContext.sparkSession, rdd, schema, isStreaming = true)
		//sqlContext.createDataFrame(rdd, schema)
	}



	override def commit(end: Offset): Unit = synchronized {
		// source = https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/TextSocketMicroBatchStream.scala#L153
		val newOffset = end.asInstanceOf[LongOffset]
		/*LongOffset.convert(end).getOrElse(
			sys.error(s"TextSocketStream.commit() received an offset ($end) that did not " +
				s"originate with an instance of this class")
		)*/

		val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

		if (offsetDiff < 0) {
			sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
		}

		batches.trimStart(offsetDiff)
		lastOffsetCommitted = newOffset
	}

	/** Stop this source. */
	/*override*/ def stop(): Unit = synchronized {
		if (socket != null) {
			try {
				// Unfortunately, BufferedReader.readLine() cannot be interrupted, so the only way to
				// stop the readThread is to close the socket.
				socket.close()
			} catch {
				case e: IOException =>
			}
			socket = null
		}
	}

	override def toString: String = s"TextSocketSource[host: $host, port: $port]"

	//protected val iter: Iterator[Char] = ???
}





class TextSocketSourceProvider2 extends StreamSourceProvider with DataSourceRegister with Logging {
	private def parseIncludeTimestamp(params: Map[String, String]): Boolean = {
		Try(params.getOrElse("includeTimestamp", "false").toBoolean) match {
			case Success(bool) => bool
			case Failure(_) =>
				throw new AnalysisException(errorClass = "includeTimestamp must be set to either \"true\" or \"false\"", messageParameters = params)
		}
	}

	/** Returns the name and schema of the source that can be used to continually read data. */
	override def sourceSchema(
							sqlContext: SQLContext,
							schema: Option[StructType],
							providerName: String,
							parameters: Map[String, String]): (String, StructType) = {
		logWarning("The socket source should not be used for production applications! " +
			"It does not support recovery.")
		if (!parameters.contains("host")) {
			throw new AnalysisException(errorClass = "Set a host to read from with option(\"host\", ...).", messageParameters = parameters)
		}
		if (!parameters.contains("port")) {
			throw new AnalysisException("Set a port to read from with option(\"port\", ...).", messageParameters = parameters)
		}
		if (schema.nonEmpty) {
			throw new AnalysisException("The socket source does not support a user-specified schema.", messageParameters = parameters)
		}

		val sourceSchema: StructType =
			if (parseIncludeTimestamp(parameters)) {
				TextSocketSource2.SCHEMA_TIMESTAMP
			} else {
				TextSocketSource2.SCHEMA_REGULAR
			}
		("textSocket", sourceSchema)
	}


	override def createSource(
							sqlContext: SQLContext,
							metadataPath: String,
							schema: Option[StructType],
							providerName: String,
							parameters: Map[String, String]): Source = {

		val host: String = parameters("host")
		val port: Int = parameters("port").toInt

		new TextSocketSource2(host, port, parseIncludeTimestamp(parameters), sqlContext)
	}

	/** String that represents the format that this data source provider uses. */
	override def shortName(): String = "socket2" // CLONE
}



case class TextSocketInputPartition(slice: ListBuffer[(UTF8String, Long)]) extends InputPartition
