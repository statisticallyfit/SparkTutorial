package com.sparkscalaexamples.RDDTutorial


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 *
 */
object L6_Transformations_WordCountExample extends App {

	/**
	 * Narrow Transformation = result of map() and filter() functions and these compute data that live on a single
	 * partition so there is no data movement between partitions to execute narrow transformations
	 * Examples = map(), mapPartition(), flatMap(), filter(), union()
	 * TODO meaning
	 *
	 * Wider Transformation = computes data that live on many partitions, meaning there will be data movements
	 * between partitions to execute wider transformations.
	 * Examples = groupByKey(), reduceByKey(), aggregate(), join(), repartition()
	 * TODO meaning
	 */

	val spark: SparkSession = SparkSession.builder()
		.master("local[3]")
		.appName("SparkByExamples.com")
		.getOrCreate()

	val sc: SparkContext = spark.sparkContext

	val rdd: RDD[String] = sc.textFile("src/main/resources/test.txt")

	Console.println("initial partition count: " + rdd.getNumPartitions)

	val reparRdd: RDD[String] = rdd.repartition(4)
	Console.println("re-partition count: " + reparRdd.getNumPartitions)

	rdd.collect().foreach(println)



	// FlatMap() transformation
	val rddAfterFlatMap: RDD[String] = rdd.flatMap(line => line.split(" "))
	Console.println("rddAfterFlatMap.foreach(println): ")
	rddAfterFlatMap.foreach(println)

	// Map() transformation
	val rddAfterMap: RDD[(String, Int)] = rddAfterFlatMap.map(word => (word, 1))
	Console.println("rddAfterMap.foreach(println)")
	rddAfterMap.foreach(println)

	// Filter() transformation
	val rddAfterFilter: RDD[(String, Int)] = rddAfterMap.filter{ case (word, one) => word.startsWith("a")}
	Console.println("rddAfterFilter.foreach(println)")
	rddAfterFilter.foreach(println)

	// ReduceBy() transformation
	// Merges the values for each key with the function specified.
	val rddAfterReduce: RDD[(String, Int)] = rddAfterMap.reduceByKey(_ + _)
	Console.println("rddAfterReduce.foreach(println)")
	rddAfterReduce.foreach(println)

	// SortByKey() transformation
}
