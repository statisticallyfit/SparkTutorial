package com.sparkstreaming.OnlineTutorials

/**
 *
 */
object TimeConsts {


	final val ONE_SEC: Long = 1
	final val FIVE_SEC: Long = 5
	final val TEN_SEC: Long = 10
	final val FIFTEEN_SEC: Long = 15
	final val TWENTY_SEC: Long = 20
	final val FORTY_SEC: Long = 40
	final val FIFTY_SEC: Long = 50


	final val FIVE_MILLISEC: Long = FIVE_SEC * 1000
	final val TEN_MILLISEC: Long = TEN_SEC * 1000

	final val ONE_SEC_W: String = toWord(ONE_SEC)
	final val FIVE_SEC_W: String = toWord(FIVE_SEC)
	final val TEN_SEC_W: String = toWord(TEN_SEC)
	final val FIFTEEN_SEC_W: String = toWord(FIFTEEN_SEC)
	final val TWENTY_SEC_W: String = toWord(TWENTY_SEC)
	final val FIFTY_SEC_W: String = toWord(FIFTY_SEC)




	val timeNum: Map[Int, Long] = Map(
		1 -> ONE_SEC,
		5 -> FIVE_SEC,
		10 -> TEN_SEC,
		15 -> FIFTEEN_SEC
	)

	val timeWords: Map[Int, String] = Map(
		1 -> "1 second",
		5 -> "5 seconds",
		10 -> "10 seconds",
		15 -> "15 seconds"
	)


	def toWord(n: Long): String = s"$n seconds"
}
