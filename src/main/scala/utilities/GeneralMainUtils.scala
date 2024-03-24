package utilities


import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

import shapeless._
//import shapeless.HList
import shapeless.ops.product._
import shapeless.ops.hlist._
import syntax.std.product._
import utilities.EnumUtils.implicits._
import utilities.GeneralMainUtils.implicits._
import utilities.DFUtils.implicits._

import scala.reflect.runtime.universe._
import scala.collection.mutable.ListBuffer

/**
 *
 */
object GeneralMainUtils {


	/**
	 * Dealing with Dates (using joda)
	 */

	import org.joda.time._

	//new LocalDate(1993, 3, 12)

	/**
	 * Class to cover underlying joda so can have a different name and syntax, using Date() instead of new localdate sntax.
	 * @param year
	 * @param monthOfYear
	 * @param dayOfMonth
	 */
	case class DateYMD(private val year: Int, private val monthOfYear: Int, private val dayOfMonth: Int) {
		val joda: LocalDate = new LocalDate(year, monthOfYear, dayOfMonth)

		override def toString: String = this.joda.toString()
		def toDate: java.sql.Date = java.sql.Date.valueOf(this.toString())
	}

	def date(year: Int, month: Int, day: Int): DateYMD = DateYMD(year, month, day)

	// DateYMD(1934, 1, 8).joda
	//implicit def myDateToUnderlyingJoda(dymd: DateYMD): LocalDate = dymd.joda



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



	object implicits {
		implicit class TupleWithHListOps[InP <: Product, InH <: HList](tup: InP) {
			def tupleToHList(implicit ev: ToHList.Aux[InP, InH]): InH = tup.toHList

			import utilities.EnumUtils.implicits._

			def tupleToList[OutH <: HList, OutP <: Product](implicit toh: ToHList.Aux[InP, InH],
												   mapper: Mapper.Aux[polyEnumsToSimpleString.type, InH, OutH],
												   tupEv: Tupler.Aux[OutH, OutP],
												   trav: shapeless.ops.product.ToTraversable.Aux[OutP, List, Any]): List[Any] = // list[any] since numbers not converted to string
				tup.toHList.enumNames.tupled.to[List]

			def tupleToStringList[OutH <: HList, OutP <: Product](implicit toh: shapeless.ops.product.ToHList.Aux[InP, InH],
														mapper: shapeless.ops.hlist.Mapper.Aux[polyAllItemsToSimpleNameString.type, InH, OutH],
														tupEv: shapeless.ops.hlist.Tupler.Aux[OutH, OutP],
														trav: shapeless.ops.product.ToTraversable.Aux[OutP, List, String]): List[String] = {
				tup.toHList.stringNamesOrValues.tupled.to[List]
			}
			def tupleToStringList_NOTSEENINUSE[OutH <: HList, OutP <: Product](implicit toh: shapeless.ops.product.ToHList.Aux[InP, InH],
													    mapper: shapeless.ops.hlist.Mapper.Aux[polyAllItemsToSimpleNameString.type, InH, OutH],
													    tupEv: shapeless.ops.product.ToTuple.Aux[OutH, OutP],
													    trav: shapeless.ops.product.ToTraversable.Aux[OutP, List, String]): List[String] = {
				tup.toHList.stringNamesOrValues.toTuple[OutP].to[List]
			}

			//shapeless.ops.product; shapeless.syntax.std.product
			// HELP this results in error - immplicit not found
			//def NEWTupleToHList(implicit /*ph: ProductToHList[T]*/ pha: ProductToHList.Aux[T, H]): H = tup.toHList

			// way 1: tup -> hlist -> to list (any) --> row
			// way2: tup --> productiterator --> tolist (any --> row
			// Can use way 2 because Row will not care about individual element types.
			/*(implicit gen: Generic[T], tupEv: Tupler[H])*/

			/*def tupleToSparkRow[OutH <: HList, OutP <: Product](implicit toh: shapeless.ops.product.ToHList.Aux[InP, InH],
													  mapper: shapeless.ops.hlist.Mapper.Aux[polyAllItemsToSimpleNameString.type, InH, OutH],
													  tupEv: shapeless.ops.hlist.Tupler.Aux[OutH, OutP],
													  //tupEv: shapeless.ops.product.ToTuple.Aux[OutH, OutP],
													  trav: shapeless.ops.product.ToTraversable.Aux[OutP, List, String]): Row = {
				Row(tup.toHList.stringNamesOrValues.tupled.to[List]:_*)
				//Row( tup.toHList.stringNamesOrValues.toTuple[OutP].to[List]:_* )
			}


			import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

			// NOTE: in order that rows made on-the-fly are equal to the rows extracted from datahubdfs, they must be made the same way, so the same enumNames (notstringnamesorvalues) function must be used, because the schema gets implicitly stored somehow! enumnames does not convert to string some items which are not joda date or enums. so their types remain. Using enumNames makes this like the dataframes from DataHub -> that is why this function has "DfRow" instead of just "Row" in its name.
			// IN conclusion, this is not adequate, use the other function below
			def tupleToSparkDfRowWithSchema[OutH <: HList, OutP <: Product](targetSchema: StructType)
															   (implicit toh: shapeless.ops.product.ToHList.Aux[InP, InH],
														   mapper: shapeless.ops.hlist.Mapper.Aux[polyEnumsToSimpleString.type, InH, OutH],
														    tupEv: shapeless.ops.hlist.Tupler.Aux[OutH, OutP],
														   //tupEv: shapeless.ops.product.ToTuple.Aux[OutH, OutP],
														   trav: shapeless.ops.product.ToTraversable.Aux[OutP, List, Any]): Row =

				new GenericRowWithSchema(tup.toHList.enumNames.tupled/*toTuple[OutP]*/.to[List].toArray, targetSchema)
			//Row(tup.productIterator.toList: _*)

*/

			def toRow[InH <: HList,
				OutH <: HList,
				OutP <: Product : TypeTag](targetSchema: StructType)(implicit toh: shapeless.ops.product.ToHList.Aux[InP, InH],
														   mapper: shapeless.ops.hlist.Mapper.Aux[polyEnumsToSimpleString.type, InH, OutH],
														   tupEv: shapeless.ops.hlist.Tupler.Aux[OutH, OutP]): Row = {

				Seq(tup).toRows(targetSchema).head
			}

		}



		implicit class HListToTuple[InP <: Product, InH <: HList, OutP <: Product](hlist: InH) {
			// TODO MAJOR: must rewrite this to account for hlists that have .runtimeLength > 22 else this will crash
			def hlistToTuple(implicit tup: Tupler.Aux[InH, OutP]): OutP = hlist.tupled

			def hlistToList(implicit tup: Tupler.Aux[InH, OutP], trav: shapeless.ops.product.ToTraversable[OutP, List]) = hlist.tupled.to[List]
			// NOTE:
			// implicit toTraversable from syntax.products ----> for the .to[List] action
			// implicit tupler from syntax.hlists ----> for the .tupled action

			// Lub = is used as M[Lub] inside ToTraversable, and M = List so it implied that Lub is the inner type of the List.

			import utilities.EnumUtils.implicits._

			// NOTE: using nested names function on this hlist in order to get nicer output
			def hlistToSparkRow[OutH <: HList](implicit mapper: Mapper.Aux[polyEnumsToNestedNameString.type, InH, OutH],
										tup: Tupler.Aux[OutH, OutP], // TODO why error when OT and why passing when Nothing in its place (when using hlistToList ?) All this works when using below:
										trav: shapeless.ops.product.ToTraversable[OutP, List]): Row =
				Row(hlist.enumNestedNames.tupled.to[List]: _*)
		}



		implicit class TypedListOps[T](lst: Seq[T]){
			def remove(elem: T): Seq[T] = {
				val splitList: (Seq[T], Seq[T]) = lst.splitAt(lst.indexWhere(_ == elem))
				splitList._1 ++ splitList._2.tail
			}

			def replace(oldElem: T, newElem: T): Seq[T] = {
				val two: (Seq[T], Seq[T]) = lst.splitAt(lst.indexOf(oldElem))
				two._1 ++ Seq(newElem) ++ two._2.tail
			}

			// NOTE see source below for better way to replace, suing map over the list (meaning replacing statelessly) and comparing with the mapOldToNew foreach element within the list you are replacing things in.
			// SOURCE: https://www.baeldung.com/scala/list-replace-elements-by-index
			def replaceAll(mapOldToNew: Map[T, T]): Seq[T] = {
				/*def replaceAll[T](lst: Seq[T]): Seq[T] = {
					val helper(accLst: Seq[T], rest: Seq[T])
				}*/
				val olds = lst
				val accNamesProgressivelyChanging = lst // current list need to keep replacing in it and returning it so we are updating the same list so the changes are maintained

				olds.foldLeft(olds)((accNamesProgressivelyChanging, currOld) => {
					if (mapOldToNew.keys.toSeq.contains(currOld)) {
						val currNew = mapOldToNew.get(currOld).get
						accNamesProgressivelyChanging.replace(currOld, currNew)
					} else accNamesProgressivelyChanging
				})
			}
		}


		implicit class SeqTupOps[P <: Product](tupleSeq: Seq[P]) extends SparkSessionWrapper  {

			/**
			 * This function goes through a long series of steps in order to convert tuples to spark Rows with the desired schema
			 *
			 * @param targetSchema
			 * @param toh
			 * @param mapper
			 * @param tupEv
			 * @tparam InH
			 * @tparam OutH
			 * @tparam OutP
			 * @return
			 */

			// SOURCE of the method used here = https://gist.github.com/Arnold1/9ca288ebe38ad3c99b49a4b1b3095bdc
			def toRows[InH <: HList,
				OutH <: HList,
				OutP <: Product : TypeTag](targetSchema: StructType)(implicit toh: shapeless.ops.product.ToHList.Aux[P, InH],
														   mapper: shapeless.ops.hlist.Mapper.Aux[polyEnumsToSimpleString.type, InH, OutH],
														   tupEv: shapeless.ops.hlist.Tupler.Aux[OutH, OutP]): Seq[Row] = {
				// NOTE: two options for tupling but ToTuple doesn't work gives errors in command line:
				// shapeless.ops.hlist.Tupler.Aux[OutH, OutP]
				// shapeless.ops.product.ToTuple.Aux[OutH, OutP]
				//val tupleSeq = Seq((Microsoft, Gemstone.Amethyst, 14, Sell, date(2004, 1, 23), Kenya), (IBM, Gemstone.Ruby, 24, Buy, date(2005, 7, 30), Mauritius))

				// NOTE: renaming the schema's names with index numbers in case the names are the same, then the select() from df will fail, cannot select same-colnames.
				//val newNames = targetSchema.fields.zipWithIndex.map { case (f, i) => s"${f.name}_${i}" }
				//val renamedTargetSchema = DFUtils.createSchema(newNames, targetSchema.fields.map(_.dataType))

				val tupleStrSeq: Seq[OutP] = tupleSeq.map(tup => tup.toHList.enumNames.tupled /*toTuple[OutP]*/)

				//val df_wrongSchema: DataFrame = sparkSessionWrapper.createDataFrame(tupleStrSeq).toDF(renamedTargetSchema.names:_*)
				val df_wrongSchema: DataFrame = sparkSessionWrapper.createDataFrame(tupleStrSeq).toDF(targetSchema.names: _*)

				val df_correctSchema: DataFrame = DFUtils.imposeSchema(df_wrongSchema, targetSchema)
				//df_wrongSchema.sqlContext.createDataFrame(df_wrongSchema.rdd, targetSchema)
				//tupleStrSeq.toDF(targetSchema.names: _*)

				// Even though we apply the schema we want here on this df_wrongSchema, the
				// correct schema doesn't get preserved. Try df.head.getDate(4) yields error so we need to do
				// a second step below (exprs)
				//val df_correctSchemaIntermediate: DataFrame = sparkSessionWrapper.createDataFrame(df_wrongSchema.rdd, schema = renamedTargetSchema)

				// Second step to validate the dataframe schema
				// will be done in the imposeSchema function
				/*val exprs: Array[Column] = renamedTargetSchema.fields.map { case f =>
					if (df_wrongSchema.schema.names.contains(f.name)) col(f.name).cast(f.dataType) //.alias(s"${f.name}_${i}")
					else lit(null).cast(f.dataType).alias(f.name) //(s"${f.name}_${i}")
				}*/
				df_correctSchema.collectAll //cannot use collectAll gives error why?
			}

			/**
			 * Same as above function except it used stringNamesOrValues instead of enumNames (so it converts ALL elems in the tuple to String type, not just enums or joda.dates)
			 *
			 * @param tupleSeq
			 * @param targetSchema
			 * @param toh
			 * @param mapper
			 * @param tupEv
			 * @tparam P
			 * @tparam H
			 * @tparam OL
			 * @tparam OT
			 * @return
			 */
			def toStrRows[InH <: HList,
				OutH <: HList,
				OutP <: Product : TypeTag](targetSchema: StructType)(implicit toh: shapeless.ops.product.ToHList.Aux[P, InH],
														   mapper: shapeless.ops.hlist.Mapper.Aux[polyEnumsToSimpleString.type, InH, OutH],
														   //tupEv: shapeless.ops.hlist.Tupler.Aux[OutH, OutP] // uses hlist.tupled
														   tupEv: shapeless.ops.hlist.Tupler.Aux[OutH, OutP]): Seq[Row] = {

				//val tupleSeq = Seq((Microsoft, Gemstone.Amethyst, 14, Sell, date(2004, 1, 23), Kenya), (IBM, Gemstone.Ruby, 24, Buy, date(2005, 7, 30), Mauritius))

				// NOTE: /*shapeless.ops.hlist.Tupler.Aux[OutH, OutP]*/ when using .tupled but use ToTuple for .toTuple[OutP]
				//val newNames = targetSchema.fields.zipWithIndex.map { case (f, i) => s"${f.name}_${i}" }
				//val renamedTargetSchema = DFUtils.createSchema(newNames, targetSchema.fields.map(_.dataType))

				val tupleStrSeq: Seq[OutP] = tupleSeq.map(tup => tup.toHList.enumNames.tupled /*toTuple[OutP]*/)

				//val df_wrongSchema: DataFrame = sparkSessionWrapper.createDataFrame(tupleStrSeq).toDF(renamedTargetSchema.names:_*)
				val df_wrongSchema: DataFrame = sparkSessionWrapper.createDataFrame(tupleStrSeq).toDF(targetSchema.names: _*)

				val df_correctSchema: DataFrame = DFUtils.imposeSchema(df_wrongSchema, targetSchema)

				//import utilities.DFUtils.implicits._
				df_correctSchema.collectAll //cannot use collectAll gives error why?
			}
		}




		// NOTE: even List(Animal.Fox, "123", Climate.Temperate is List[Object] the compiler accepts when putting Seq[_] rather than Seq[Object] for calling the implicit methods ... TODO why?
		implicit class ListOps(lst: Seq[_]) {

			//import utilities.EnumUtils.implicits._
			//import utilities.EnumUtils.Helpers._

			import Helpers._

			// NOTE: more elegant to turn the list -> hlist then can map the polymorphic function over it

			// WARNING: cannot use the poly function for a regular list because the poly function gets passed only to an hlist.
			/*def namesAll[H <: HList, O <: HList](implicit mapper: Mapper.Aux[polyAllItemsToSimpleNameString.type, H, O] /*, t: Tupler[O]*/): O = {

				// NOTE: now must filter out the tuples to get the Some() wherever they are
				thehlist.map(polyAllItemsToSimpleNameString)(mapper)
			}*/
			def typeNames: Seq[String] = lst.map(x => getSimpleTypeName(x))

			def stringNamesOrValues: Seq[String] = lst.map(x => getSimpleString(x))

			def stringNestedNamesOrValues: Seq[String] = lst.map(x => getNestedTypeName(x))

			// convert List[Any] to spark row
			def listToSparkRow: Row = Row(lst: _*)
		}
	}


	// NOTE: Usage, to convert Seq[Tuple[Enum]] -> Seq[Tuple[String]]:
	// seq.map(_.tupToHList.stringifyEnums.hlistToTup)


	object Helpers {

		import enumeratum._

		// Gets string of an entry
		def getSimpleString[T](item: T): String = item match {
			case null => "null"
			case str: String => str
			case d: DateYMD => d.joda.toString()
			case enumType: EnumEntry => enumType.getClass.getSimpleName.init // remove the $ at the end
			case otherType => otherType.toString // no need to get it type just convert to string (like int value -> string) .getClass.getSimpleName
		}
		// Gets type name
		def getSimpleTypeName[T](item: T): String = item match {
			case null => "null"
			case str: String => str
			case d: DateYMD => d.getClass.getSimpleName
			case enumType: EnumEntry => enumType.getClass.getSimpleName.init // remove the $ at the end
			case otherType => otherType.getClass.getSimpleName
		}

		def getNestedTypeName[T](item: T): String = item match {
			case null => "null"
			case str: String => str
			case enumType: EnumEntry => {
				val rawName: String = enumType.getClass.getTypeName // e.g. com.data.util.EnumHub$Animal$Cat$HouseCat$PersianCat$

				val pckgName: String = rawName.split('$').head // e.g. com.data.util.EnumHub
				val leftover: Array[String] = rawName.split('$').tail // e.g. Array(Animal, Cat, HouseCat, PersianCat)

				val parentEnum: String = leftover.head
				val nestedName: String = leftover.mkString(".")

				nestedName
			}
			case otherType => otherType.getClass.getSimpleName
		}
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
