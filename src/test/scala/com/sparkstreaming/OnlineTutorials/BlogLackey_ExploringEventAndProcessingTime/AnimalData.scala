package com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime



import java.sql.Timestamp

/**
 *
 */
object AnimalData {
	case class AnimalView(timeSeen: Timestamp, animal: String, howMany: Integer)

	val animals: Seq[AnimalView] = Seq(
		// window: 16:33:15 - 16:33:20
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:18"), "rat", 1),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:19"), "hog", 2),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:19"), "rat", 2),
		// window: 16:33:20 - 16:33:25
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:22"), "dog", 5),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:21"), "pig", 2),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:22"), "duck", 4),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:22"), "dog", 4),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:24"), "dog", 4),
		// window: 16:33:25 - 16:33:30
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:26"), "mouse", 2),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:27"), "horse", 2),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:27"), "bear", 2),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:29"), "lion", 2),
		// window: 16:33:30 - 16:33:35
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:31"), "tiger", 4),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:31"), "tiger", 4),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:32"), "fox", 4),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:33"), "wolf", 4),
		AnimalView(Timestamp.valueOf("2019-06-18 16:33:34"), "sheep", 4),
	)
	val animalsBatch0: Seq[AnimalView] = animals.take(3)
	val animalsBatch1: Seq[AnimalView] = animals.slice(3, 8)
	val animalsBatch2: Seq[AnimalView] = animals.slice(8, 12)
	val animalsBatch3: Seq[AnimalView] = animals.slice(12, 17)
}
