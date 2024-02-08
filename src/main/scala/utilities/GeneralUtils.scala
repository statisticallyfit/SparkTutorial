package utilities

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



	// ------------------------------------------------------------------------------------------------------------



	/**
	 * TODO GOAL: convert enums in the tuples to string before passing the tuple-row to dataframe
	 *
	 * NOTE: idea 1:
	 * 1) turn the enums in the row-tuple directly to string
	 *
	 * NOTE: idea 2:
	 * 1) create Seq() of tuples
	 * 2) convert tuples inside --> shapeless hlist
	 * 3) unzip, convert just that relevant Enum col into string (or can convert typed without unzipping?)
	 * https://stackoverflow.com/questions/21442473/scala-generic-unzip-for-hlist
	 * 4) convert stringified format of the tuple/list into dataframe (spark cannot accept custom objects into dataframe)
	 * (binary result no good)
	 *
	 */


	import shapeless._
	import shapeless.ops.hlist._
	//import shapeless.ops.tuple._
	//import syntax.std.tuple._ // WARNING either this or product
	import shapeless.ops.product._
	import syntax.std.product._

	//import shapeless.ops.traversab


	import org.apache.spark.sql.Row
	/**
	 * Resources:
	 * https://stackoverflow.com/questions/19893633/filter-usage-in-shapeless-scala
	 * https://github.com/milessabin/shapeless/issues/73
	 */


	implicit class TupleToHList[T <: Product, H <: HList](tup: T) {
		// NOTE: can use this version when importing the op.stuple, syntax.tuple
		//def tupleToHList(implicit gen: Generic[T]/*, tupEv: Tupler[H]*/) = tup.productElements
		//def tupToHList(implicit /*gen: Generic[T]*/p: ProductToHList[H], t: ProductToHList[T]) = tup.toHList

		//shapeless.ops.product

		def tupleToHList(implicit ev: ToHList.Aux[T, H]): H = tup.toHList

		import utilities.EnumUtils.implicits._
		def tupleToNameList[OL <: HList, OT <: Product](implicit toh: ToHList.Aux[T, H],
											   mapper: Mapper.Aux[polyAllItemsToSimpleNameString.type, H, OL],
											   tupEv: Tupler.Aux[OL, OT],
											   trav: shapeless.ops.product.ToTraversable.Aux[OT, List, String]): List[String] =
			tup.toHList.namesAll.tupled.to[List]

		//shapeless.ops.product; shapeless.syntax.std.product
		// HELP this results in error - immplicit not found
		//def NEWTupleToHList(implicit /*ph: ProductToHList[T]*/ pha: ProductToHList.Aux[T, H]): H = tup.toHList

		// way 1: tup -> hlist -> to list (any) --> row
		// way2: tup --> productiterator --> tolist (any --> row
		// Can use way 2 because Row will not care about individual element types.
		def tupleToSparkRow(implicit gen: Generic[T], tupEv: Tupler[H]): Row = Row(tup.productIterator.toList:_*)
	}
	//import shapeless.syntax.std.tuples._

	implicit class HListToTuple[T <: Product, H <: HList, OT <: Product](hlist: H) {
		// TODO MAJOR: must rewrite this to account for hlists that have .runtimeLength > 22 else this will crash
		def hlistToTuple(implicit tup: Tupler.Aux[H, OT]): OT = hlist.tupled

		def hlistToList(implicit tup: Tupler.Aux[H, OT], trav: shapeless.ops.product.ToTraversable[OT, List]) = hlist.tupled.to[List]
		// NOTE:
		// implicit toTraversable from syntax.products ----> for the .to[List] action
		// implicit tupler from syntax.hlists ----> for the .tupled action

		// Lub = is used as M[Lub] inside ToTraversable, and M = List so it implied that Lub is the inner type of the List.
		import utilities.EnumUtils.implicits._

		// NOTE: using nested names function on this hlist in order to get nicer output
		def hlistToSparkRow[O <: HList](implicit mapper: Mapper.Aux[polyEnumsToNestedNameString.type, H, O],
								  tup: Tupler.Aux[O, OT], // TODO why error when OT and why passing when Nothing in its place (when using hlistToList ?) All this works when using below:
								  trav: shapeless.ops.product.ToTraversable[OT, List]): Row =
			Row(hlist.nestedNames.tupled.to[List]:_*)


	}


	// NOTE: Usage, to convert Seq[Tuple[Enum]] -> Seq[Tuple[String]]:
	// seq.map(_.tupToHList.stringifyEnums.hlistToTup)


	// NOTE: even List(Animal.Fox, "123", Climate.Temperate is List[Object] the compiler accepts when putting Seq[_] rather than Seq[Object] for calling the implicit methods ... TODO why?
	implicit class ListOps(lst: Seq[_]) {

		import utilities.EnumUtils.Helpers._
		// NOTE: more elegant to turn the list -> hlist then can map the polymorphic function over it
		def namesAll: Seq[String] = lst.map(x => getSimpleName(x))
		def nestedNamesAll: Seq[String] = lst.map(x => getNestedName(x))
		// convert List[Any] to spark row
		def listToSparkRow: Row = Row(lst:_*)
	}



	// -----------------------------------------------------------------------------------------------


	/**
	 * Converts seq of tuples into seq of list
	 *
	 * @param seq
	 * @tparam T
	 *
	 */
	def tuplesToLists[T <: Product](seq: Seq[T]): Seq[List[Any]] = {
		seq.map(_.asInstanceOf[Product].productIterator.toList)
	}

	/**
	 * Converts scala sequence of tuples (type any) into sequence of rows (spark)
	 * NOTE: T must be a tuple
	 */
	def tuplesToRows[T <: Product](seq: Seq[T]): Seq[Row] = {
		//val lstAny: Seq[List[Any]] = seq.map(_.asInstanceOf[Product].productIterator.toList)
		val rows: Seq[Row] = tuplesToLists(seq).map { case lst => Row(lst:_*)}

		rows
	}



	// -----------------------------------------------------------------------------------------------

	import scala.reflect.runtime.universe._




	// general type inspector function

	import scala.reflect._

	def inspector[T: TypeTag](ob: T) = typeTag[T].tpe.toString

	def inspect[T: TypeTag : ClassTag](ob: T) = {
		println(s"typeTag[T].tpe.termSymbol = ${typeTag[T].tpe.termSymbol}")
		println(s"typeTag[T].tpe.typeSymbol = ${typeTag[T].tpe.typeSymbol}")
		println(s"typeTag[E].tpe = ${typeTag[T].tpe}") // com.data.util.EnumHub.Country.Arabia.type
		println(s"typeTag[E].getClass = ${typeTag[T].getClass}")
		println(s"typeTag[E].getClass = ${typeTag[T].getClass.getClasses}")
		println(s"typeTag[E].getClass.getTypeName = ${typeTag[T].getClass.getTypeName}")
		println(s"typeTag[E].getClass.getSimpleName = ${typeTag[T].getClass.getSimpleName}")
		println(s"typeTag[E].getClass.getSuperclass = ${typeTag[T].getClass.getSuperclass}")
		println(s"typeTag[E].getClass.getCanonicalName = ${typeTag[T].getClass.getCanonicalName}")

		println(s"classTag[E].wrap = ${classTag[T].wrap}")
		println(s"classTag[E].getClass = ${classTag[T].getClass}")
		println(s"classTag[E].getClass.getClasses = ${classTag[T].getClass.getClasses}")
		println(s"classTag[E].runtimeClass = ${classTag[T].runtimeClass}")
		println(s"classTag[E].runtimeClass.getSimpleName = ${classTag[T].runtimeClass.getSimpleName}")
		println(s"classTag[E].runtimeClass.getSuperclass = ${classTag[T].runtimeClass.getSuperclass}")
		println(s"classTag[E].runtimeClass.getTypeName = ${classTag[T].runtimeClass.getTypeName}")
		println(s"classTag[E].runtimeClass.getCanonicalName = ${classTag[T].runtimeClass.getCanonicalName}")

		println(s"typeOf[E].getClass = ${typeOf[T].getClass}")
		println(s"typeOf[E].getClass.getClasses = ${typeOf[T].getClass.getClasses}")
		println(s"typeOf[E].getClass.getTypeName = ${typeOf[T].getClass.getTypeName}")
		println(s"typeOf[E].getClass.getSuperclass = ${typeOf[T].getClass.getSuperclass}")
		println(s"typeOf[E].getClass.getSimpleName = ${typeOf[T].getClass.getSimpleName}")
		println(s"typeOf[E].getClass.getCanonicalName = ${typeOf[T].getClass.getCanonicalName}")
		println(s"typeOf[E].typeSymbol = ${typeOf[T].typeSymbol}")
		println(s"typeOf[E].resultType = ${typeOf[T].resultType}") // com.data.util.EnumHub.Country.Arabia.type
		println(s"typeOf[E].finalResultType = ${typeOf[T].finalResultType}") // com.data.util.EnumHub.Country.Arabia.type
	}
}
