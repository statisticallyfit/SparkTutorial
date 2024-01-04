package com.util

import scala.collection.mutable.ListBuffer

/**
 *
 */
object GeneralUtils {


	/**
	 * Takes same keys and merges their values into lists
	 * Ex:
	 * ListBuffer(
	 * WindowInterval(1970-01-01 02:00:01.0,1970-01-01 02:00:02.0),a1),
	 * (WindowInterval(1970-01-01 02:00:02.0,1970-01-01 02:00:03.0),b1),
	 * (WindowInterval(1970-01-01 02:00:03.0,1970-01-01 02:00:04.0),c1),
	 * (WindowInterval(1970-01-01 02:00:04.0,1970-01-01 02:00:05.0),z1),
	 * (WindowInterval(1970-01-01 02:00:01.0,1970-01-01 02:00:02.0),a2),
	 * (WindowInterval(1970-01-01 02:00:02.0,1970-01-01 02:00:03.0),b2),
	 * (WindowInterval(1970-01-01 02:00:03.0,1970-01-01 02:00:04.0),c2) )
	 *
	 * Result:
	 * Map(
	 * WindowInterval(1970-01-01 02:00:01.0,1970-01-01 02:00:02.0) -> ListBuffer((WindowInterval(1970-01-01 02:00:01.0,1970-01-01 02:00:02.0),a1), (WindowInterval(1970-01-01 02:00:01.0,1970-01-01 02:00:02.0),a2)),
	 * WindowInterval(1970-01-01 02:00:04.0,1970-01-01 02:00:05.0) -> ListBuffer((WindowInterval(1970-01-01 02:00:04.0,1970-01-01 02:00:05.0),z1)),
	 * WindowInterval(1970-01-01 02:00:02.0,1970-01-01 02:00:03.0) -> ListBuffer((WindowInterval(1970-01-01 02:00:02.0,1970-01-01 02:00:03.0),b1), (WindowInterval(1970-01-01 02:00:02.0,1970-01-01 02:00:03.0),b2)),
	 * WindowInterval(1970-01-01 02:00:03.0,1970-01-01 02:00:04.0) -> ListBuffer((WindowInterval(1970-01-01 02:00:03.0,1970-01-01 02:00:04.0),c1), (WindowInterval(1970-01-01 02:00:03.0,1970-01-01 02:00:04.0),c2))
	 *)
	 * @param lst
	 * @tparam A
	 * @tparam B
	 * @return
	 */
	def mergeValues[A, B](lst: ListBuffer[(A, B)]): Map[A, Seq[B]] = {
		lst.groupBy(elem => elem._1).map{ case (k, v) => (k, v.map(_._2).toList)}
	}

	/**
	 * Function to add elements (spot 2) to current element in a map
	 * E.g. if key1 is mapped to List(a) and we want to add (b) at key1 then result at key1 will be List(a, b)
	 *
	 * @param mp
	 * @param elem
	 * @tparam A
	 * @tparam B
	 * @return
	 */
	def mapPut[A, B](mp: Map[A, Seq[B]], elem: (A, B)): Map[A, Seq[B]] = {
		mp.isDefinedAt(elem._1) match {
			case true => {
				val oldElem: (A, Seq[B]) = (elem._1 -> mp(elem._1))
				val newElem: (A, Seq[B]) = (elem._1 -> (mp(elem._1) :+ elem._2))

				val newMp: Map[A, Seq[B]] = mp - oldElem._1
				newMp + newElem
			}
			case false => {
				val newElem: (A, Seq[B]) = (elem._1 -> List(elem._2))
				mp + newElem
			}
		}
	}


	/**
	 * Groups elements (consecutively) that have same starting tuple
	 * E.g.
	 * Given:
	 * 	val xs: List[(Int, Char)] = List((1,a), (1,a), (1,b), (2,b), (2,c), (2,a), (3,a), (3,b), (3,c), (1,e), (2,e), (2,h))
	 * 	val seed: List[(Int, List[Char])] = List((xs.head._1, List(xs.head._2)))
	 * Query:
	 * 	xs.tail.foldLeft(seed){case (acc, (int, char)) => mergeTupListAndNewTup(acc, (int, char))}
	 * Result:
	 * 	List((1,List(a, a, b)), (2,List(b, c, a)), (3,List(a, b, c)), (1,List(e)), (2,List(e, h)))
	 *
	 *
	 * GOAL: want to see streaming events gather in windows as they come, not just merged under one window at the final step. Want to see them as they arrive.
	 */
	def mergeTupListAndNewTup[A, B](accumulator: List[(A, List[B])], newTup: (A, B)): List[(A, List[B])] = {
		val (a, b) = newTup

		accumulator.last._1 == a match {

			case true => {
				//println(s"merging: ${accumulator.init ++ List((a, accumulator.last._2 :+ b))}")
				accumulator.init ++ List((a, accumulator.last._2 :+ b))
			}
			case false => {
				//println(s"merging: ${accumulator ++ List((a, List(b)))}")
				accumulator ++ List((a, List(b)))
			}
		}
	}
}
