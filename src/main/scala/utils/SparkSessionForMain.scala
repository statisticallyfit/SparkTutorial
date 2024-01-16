package utils


import org.apache.spark.sql.SparkSession

/**
 * Source for code = https://github.com/Bigdataengr/dataframe_unittest/blob/master/src/test/scala/SparkSessionTestWrapper.scala
 *
 * Source 2 = https://hyp.is/c3Jlypa9Ee6g3g8hfzeymQ/mrpowers.medium.com/how-to-cut-the-run-time-of-a-spark-sbt-test-suite-by-40-52d71219773f
 */
trait SparkSessionForMain {

	//	Logger.getLogger("org").setLevel(Level.ERROR)
	//	Logger.getLogger("akka").setLevel(Level.ERROR)

	val sparkMainSession: SparkSession =
		SparkSession
			.builder()
			.master("local[1]")
			.appName("Local Test")
			.config("spark.sql.shuffle.partitions", "1")
			.getOrCreate()

	// REPL
	// val sparkTestsSession: SparkSession = SparkSession.builder().master("local[1]").appName("Local Test").config("spark.sql.shuffle.partitions", "1").getOrCreate()
}
