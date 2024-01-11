package util

//import org.apache.spark.sql._
import com.SparkSessionForTests
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.row_number
// rangeBetween, rowsBetween

//import util.DataFrameCheckUtils._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
/**
 *
 */
object DataFrameTestUtils extends SparkSessionForTests{

	import sparkTestsSession.implicits._


	def manualRanker(df: DataFrame, dropCols: Seq[String], viewCols: Seq[String]) = {

		val windowPartOrdSpec: WindowSpec = Window.partitionBy(viewCols(0)).orderBy(viewCols(1))

		val rowNumberDf: DataFrame = df.withColumn("RowNum", row_number().over(windowPartOrdSpec)).drop(dropCols: _*)

		val tupsInOrder = rowNumberDf.select($"*").collect().toSeq.map(row => row.toSeq match {
			case Seq(firstCol, secCol, id) => (firstCol, secCol, id).asInstanceOf[(String, Integer, Integer)]
		})

		def groupMid(lst: List[(String, Integer, Integer)]): Map[Integer, List[(String, Integer, Integer)]] = lst.groupBy { case (first, mid, id) => mid } // .values.map(_.toList)

		val tupsMid: List[List[(String, Integer, Integer)]] = tupsInOrder
			.groupBy { case (first, mid, id) => first }
			.values.map(_.toList).toList
			.map(groupMid(_).values.toList).flatten

		import scala.collection.mutable.ListBuffer

		val buf: ListBuffer[Integer] = ListBuffer()

		def getNewId(lst: List[(String, Integer, Integer)]) = {
			buf += lst.head._3;
			lst.head._3
		}

		tupsMid.map(lst => lst.length match {
			case n if n > 1 => {
				val newId = getNewId(lst)
				lst.map { case (first, mid, id) => (first, mid, newId) }
			}
			case _ => lst

		})

	}


}
