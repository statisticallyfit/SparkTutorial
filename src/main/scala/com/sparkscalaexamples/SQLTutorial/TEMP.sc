import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.catalyst.plans._

import org.apache.spark.sql.types.{DataType, StringType, IntegerType, BooleanType, DoubleType, StructField, StructType}
import scala.collection.JavaConversions._

import scala.reflect.runtime.universe._


val spark: SparkSession = SparkSession.builder().master("local[1]").appName("SparkByExamples.com").getOrCreate()

import spark.implicits._
val df = Seq((0f, "hello")).toDF("label", "text")

df.show()

df.select(col("label").cast(DoubleType)).map { case Row(label) => label.getClass.getName }.show(false)