package com.sparkstreaming.OnlineTutorials.BlogKonieczny_ApacheSparkStructuredStreaming.util


import com.util.GeneralUtils
import com.util.StreamingUtils.IntervalWindow

import java.sql.Timestamp
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.collection.immutable.Seq
import scala.collection.mutable

/**
 * SOURCE = https://github.com/bartosz25/spark-scala-playground/blob/d4dae02098169e9f4241f3597dc4864421237881/src/test/scala/com/waitingforcode/util/store/InMemoryKeyedStore.scala#L8
 *
 * In memory data store used in tests assertions.
 */
object InMemoryKeyedStore {

	type Time = Timestamp
	type Letter = String
	type Count = Long

	type Key = String

	type RowRepr = String
	type RowReprList = ListBuffer[RowRepr]

	// TODO make same for Count param but how to make dynamic?  because
	type WindowPair[A] = (IntervalWindow, A)
	type WindowPairBuffer[A] = ListBuffer[(IntervalWindow, A)]
	type WindowToOccurrencesMap[A] = Map[IntervalWindow, Seq[A]]//ListBuffer[WindowLetterPair]


	private val recordStr = new HashMap[Key, RowReprList]()


	private val recordBufferL = new HashMap[Key, WindowPairBuffer[Letter]]()
	private val recordMap = new HashMap[Key, Map[IntervalWindow, Letter]]
	private val recordMapL = new HashMap[Key, WindowToOccurrencesMap[Letter]]()

	private val recordBufferC = new HashMap[Key, WindowPairBuffer[Count]]()
	private val recordMapC = new HashMap[Key, WindowToOccurrencesMap[Count]]()


	def addValueFinalL(key: Key, buf: WindowPairBuffer[Letter])/*: WindowToOccurrencesMap[A]*/ = {
		val merged: WindowToOccurrencesMap[Letter] = GeneralUtils.mergeValues(buf)

		println(s"\n(STORE): merged all now (letter) = \n${merged.mkString("\n")}")

		recordMapL.put(key, merged)

		println(s"\n(STORE): hashmap of merged all now (letter) = \n${recordMapL(key).mkString("\n")}")
	}

	def addValueFinalC(key: Key, buf: WindowPairBuffer[Count]) /*: WindowToOccurrencesMap[A]*/ = {
		val merged: WindowToOccurrencesMap[Count] = GeneralUtils.mergeValues(buf)
		recordMapC.put(key, merged)
	}

	// TODO - merge the listbuffer elems (letterfreqs) before passing into the hashmap ---> then pass into hashmap as string to store as string ---> can always parse the hashmap string contents (map-str --> map?) but easier to use letterfreqs as the container of real elems and hashmap just as storer-str and getter-str.

	def addValueL(key: Key, value: WindowPair[Letter]): Option[WindowToOccurrencesMap[Letter]] = {

		//println(s"\n(STORE): recordBufferL (letter, BEFORE) = \n${recordBufferL(key).mkString("\n")}")
		//println(s"\n(STORE): recordMap (letter, BEFORE) = \n${recordMap.mkString("\n")}")

		recordBufferL.synchronized {

			val values: ListBuffer[(IntervalWindow, Letter)] = recordBufferL.getOrElse(key, ListBuffer.empty[WindowPair[Letter]])
			values += value //.append(value)

			// Update the listbuffer of records
			recordBufferL.put(key, values)

			// Merging the listbuffer elements by key (groupby)
			val valuesMerged: Map[IntervalWindow, Seq[Letter]] = GeneralUtils.mergeValues(values)


			//recordMapL.put(key, valuesMerged)

			println(s"\n(STORE): valuesMerged (letter) = \n${valuesMerged.mkString("\n")}")
			// Update the map of records
			val opt = recordMapL.put(key, valuesMerged)
			println(s"\n(STORE): recordBufferL (letter, AFTER) = \n${recordBufferL(key).mkString("\n")}")
			println(s"\n(STORE): recordMapL (letter) = \n${recordMapL(key).mkString("\n")}")
			//println(s"(STORE): recordMap (letter) = ${recordMap.mkString("\n")}")
			opt
			// Need to add into the map in the way that does not replace the element-pairs already there
		}
	}

	def addValueC(key: Key, value: WindowPair[Count]): Option[WindowToOccurrencesMap[Count]] = {

		//println(s"\n(STORE): recordBufferL (letter, BEFORE) = \n${recordBufferL(key).mkString("\n")}")
		//println(s"\n(STORE): recordMap (letter, BEFORE) = \n${recordMap.mkString("\n")}")

		recordBufferC.synchronized {

			val values: ListBuffer[(IntervalWindow, Count)] = recordBufferC.getOrElse(key, ListBuffer.empty[WindowPair[Count]])
			values += value //.append(value)

			// Update the listbuffer of records
			recordBufferC.put(key, values)

			// Merging the listbuffer elements by key (groupby)
			val valuesMerged: Map[IntervalWindow, Seq[Count]] = GeneralUtils.mergeValues(values)


			//recordMapL.put(key, valuesMerged)

			println(s"\n(STORE): valuesMerged (count) = \n${valuesMerged.mkString("\n")}")
			// Update the map of records
			val opt = recordMapC.put(key, valuesMerged)
			println(s"\n(STORE): recordBufferC (count, AFTER) = \n${recordBufferC(key).mkString("\n")}")
			println(s"\n(STORE): recordMapC (count) = \n${recordMapC(key).mkString("\n")}")
			//println(s"(STORE): recordMap (count) = ${recordMap.mkString("\n")}")
			opt
			// Need to add into the map in the way that does not replace the element-pairs already there
		}
	}


	def addValue(key: Key, value: RowRepr): Option[RowReprList] = {
		recordStr.synchronized {
			val values: RowReprList = recordStr.getOrElse(key, new ListBuffer[RowRepr]())
			values.append(value)
			recordStr.put(key, values)
		}
	}

	def getElementsGrouped(key: String): Option[WindowToOccurrencesMap[Letter]] = {//ListBuffer[String] = {
		// TODO - do get or else for all the hashmaps to establish which has the key and get the resultvalue

		//def mapToListBuffOfStr[A](mp: Map[IntervalWindow, Seq[A]]): ListBuffer[Letter] = ListBuffer.from(mp.toList).map(_.toString())

		recordMapL.isDefinedAt(key) match {
			case true => Some (recordMapL(key) )// mapToListBuffOfStr(recordMapL(key))
			case false => None
			/*case false => recordMapC.isDefinedAt(key) match {
				case true => Some (recordMapC(key) )// mapToListBuffOfStr(recordMapC(key))
				case false => None // recordStr.getOrElse(key, ListBuffer.empty)
			}*/
		}
	}

	def getElementsGroupedC(key: String): Option[WindowToOccurrencesMap[Count]] = { //ListBuffer[String] = {
		// TODO - do get or else for all the hashmaps to establish which has the key and get the resultvalue

		//def mapToListBuffOfStr[A](mp: Map[IntervalWindow, Seq[A]]): ListBuffer[Letter] = ListBuffer.from(mp.toList).map(_.toString())

		recordMapC.isDefinedAt(key) match {
			case true => Some(recordMapC(key)) // mapToListBuffOfStr(recordMapL(key))
			case false => None
			/*case false => recordMapC.isDefinedAt(key) match {
				case true => Some (recordMapC(key) )// mapToListBuffOfStr(recordMapC(key))
				case false => None // recordStr.getOrElse(key, ListBuffer.empty)
			}*/
		}
	}
	def getElementsAsArrivals_StrFormat(key: String): Option[ListBuffer[String]] = {
		recordStr.isDefinedAt(key) match {
			case true => Some(recordStr(key))
			case false => None
		}
	}

	def getElementsAsArrivals(key: String): Option[ListBuffer[(IntervalWindow, List[Letter])]] = {

		recordBufferL.isDefinedAt(key) match {
			case true => {
				val lst = recordBufferL(key)

				val seed: List[(IntervalWindow, List[Letter])] = List((lst.head._1, List(lst.head._2)))

				val consecMergesByWindow: List[(IntervalWindow, List[Letter])] = lst.tail.foldLeft(seed) { case (acc, (intervalWindow, letter)) => GeneralUtils.mergeTupListAndNewTup(acc, (intervalWindow, letter)) }

				Some(ListBuffer.from(consecMergesByWindow)) // converting to listbuffer ot be consisten
			}
			case false => None
		}

	}

	def getElementsAsArrivalsC(key: String): Option[ListBuffer[(IntervalWindow, Count)]] = {
		//ListBuffer[(IntervalWindow, A)]
		recordBufferC.isDefinedAt(key) match {
			case true => Some(recordBufferC(key))
			case false => None
		}
		/*recordBufferC.isDefinedAt(key) match {
			case true => {
				val lst = recordBufferC(key)

				val seed: List[(IntervalWindow, List[Count])] = List((lst.head._1, List(lst.head._2)))

				val consecMergesByWindow: List[(IntervalWindow, List[Count])] = lst.tail.foldLeft(seed) { case (acc, (intervalWindow, letter)) => GeneralUtils.mergeTupListAndNewTup(acc, (intervalWindow, letter)) }

				Some(ListBuffer.from(consecMergesByWindow)) // converting to listbuffer ot be consisten
			}
			case false => None
		}*/
	}

	def allValues = recordStr
}