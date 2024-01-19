package com.sparkstreaming.OnlineTutorials

/**
 *
 */
object TODOFIX_STACKOVERFLOW_RowNumForStreamingDF extends App {


	/*val foreachwriter = new ForeachWriter[SoFarType] {
		override def open(partitionId: Long, version: Long): Boolean = true

		override def process(sofartype: SoFarType): Unit = {
			// sofartype.
			println(s"window row: $sofartype")
			println(sofartype.schema)

			val timeWindow: IntervalWindow = StreamingUtils.parseWindow(sofartype.get(0).toString)
			val cnt: Count = sofartype.getAs[Count]("count(1)") // TODO figure out why not same as results in test


			val rowReprCnt: String = s"${timeWindow.toString} -> ${cnt}"
			InMemoryKeyedStore.addValue(TEST_KEY + FORMAT_STR_CNT, rowReprCnt)
			InMemoryKeyedStore.addValueC(TEST_KEY + FORMAT_REAL_CNT, (timeWindow, cnt))
		}

		override def close(errorOrNull: Throwable): Unit = {}
	}*/

	/*val saveWithWindowFunction = (sourceDf: DataFrame, batchId: Long) => {
		val tempW = Window.partitionBy("new_col").orderBy(lit("A"))

		sourceDf.as
			.withColumn("row_num", row_number().over(tempW)).drop("new_col")
			//.orderBy()
			.groupBy("row_num", "window")
			//.groupBy(window($"timestamp", toWord(TIME_WINDOW)))
			.agg(collect_list("letter").alias("letterlist"))

		//... save the dataframe using: sourceDf.write.save()
	}*/

}
