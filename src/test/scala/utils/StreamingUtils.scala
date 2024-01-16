package utils

import java.sql.Timestamp

//import org.apache.spark.sql.expressions.Window

/**
 *
 */
object StreamingUtils {



	case class IntervalWindow(lt: Timestamp, rt: Timestamp)

	/**
	 * Convert argument in form of "[1970-01-01 02:00:04.0,1970-01-01 02:00:06.0]" into Window object or tuple made of timestamp (left, right)
	 * @param intervalStr
	 */
	def parseWindow(intervalStr: String): IntervalWindow = {
		val timestampLeftRight: Array[Timestamp] = intervalStr
			.replace('[', ' ')
			.replace(']', ' ')
			.trim()
			.split(',')
			.map(str => Timestamp.valueOf(str))

		val (lt, rt): (Timestamp, Timestamp) = (timestampLeftRight(0), timestampLeftRight(1))

		IntervalWindow(lt, rt)
	}



}
