package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutFiltering

import com.data.util.DataHub.ManualDataFrames.fromEnums.EnumString
import com.data.util.EnumHub._

import enumeratum._


/**
 *
 */
object tempparsehlist extends App {

	import shapeless._
	//import shapeless.ops.hlist._
	import shapeless.ops.traversable.FromTraversable._
	import shapeless.syntax.std.traversable._
	import scala.language.implicitConversions
	import utilities.GeneralUtils._
	import shapeless.syntax.sized._
	import syntax.std.tuple._
	//import syntax.std.product._
	import shapeless.ops.hlist._
	import shapeless.ops.nat._
	import shapeless.syntax.nat._

	SOUTHERN_HEMI.sized(12)

	import scala.reflect.runtime._
	import universe._
	import scala.tools.reflect.ToolBox


	val lst: Seq[Animal] = List(Animal.Squirrel, Animal.SeaCreature.Dolphin, Animal.Bird.Eagle.GoldenEagle, Animal.Cat.HouseCat.SiameseCat, Animal.Hyena, Animal.SeaCreature.Oyster)

	// TODO here - make hlist of types dynamically because we don't know what to pass in as the arg for the toHList (how long the hlist of types should be depends on the original list's length) --- how to generate arbitrary hlist of types?
	def genh[T: TypeTag](acc: HList, cnt: Int): HList = {
		if (cnt == 0) acc
		else genh[T](typeTag[T].tpe :: acc, cnt - 1)
	}
	// TODO now how to pass result of genh into the toHList[] ??? (implicit class, infer T??)
	/**
	 * idea:
	 * 1) generate string of: TYPE :: TYPE :: TYPE .... :: HNil
	 * 2) put this string in the code string "lst.toHList[GEN_TYPE]"
	 * 3) reinviate code by calling compile (see old prob dist project)
	 */


	val cm = universe.runtimeMirror(getClass.getClassLoader)

	val tb = cm.mkToolBox()
	val TPE = genh[Animal](HNil, 6)
	val parseStr = s"$lst.toHList[$TPE]"
	// TODO test this first then put in the parsestr
	/*val parsed: Tree = tb.parse("List(Animal.SeaCreature.Clam).toHList[Animal.type :: HNil]")
	val result = tb.eval(parsed)*/
	val thecode =
		s"""
		  |import com.data.util.EnumHub.Animal
		  |import com.data.util.EnumHub.Animal._
		  |import shapeless._
		  |import shapeless.ops.traversable.FromTraversable._
		  |import shapeless.syntax.std.traversable._
		  |import scala.language.implicitConversions
		  |import utilities.GeneralUtils._
		  |
		  |List(Animal.Squirrel).toHList[Animal :: HNil].asInstanceOf[Option[Animal :: HNil]]
		  |""".stripMargin
	val result = tb.eval(tb.parse(thecode))

	println(result)


	val hlst: Option[Animal :: Animal :: HNil] = lst.toHList[Animal :: Animal :: HNil]
	val hlstStrs: String :: String :: HNil = hlst.get.enumsToString
	val ss: Seq[EnumString] = hlstStrs.toList

	// ----


	// SOURCE:
	// https://stackoverflow.com/questions/14722860/convert-a-scala-list-to-a-tuple
	// int -> nat: https://stackoverflow.com/questions/39157479/int-optionnat
	def listEnumToListStr22[E <: EnumEntry](lst: List[E]): List[String] = {
		require(lst.length == 22)

		lst.toList.sized(Nat(22)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
	}


	def listEnumToListStrLess[E <: EnumEntry](lst: List[E]): List[String] = {
		require(lst.length < 22)
		val cm = universe.runtimeMirror(getClass.getClassLoader)
		val tb = cm.mkToolBox()

		// TODO must import all the enums ._ this way now ...
		val theCode: String =
			s"""
			   |import com.data.util.EnumHub._
			   |import com.data.util.EnumHub.Country._
			   |import utilities.GeneralUtils._
			   |import scala.language.implicitConversions
			   |
			   |import shapeless._
			   |import shapeless.ops.traversable.FromTraversable._
			   |import shapeless.syntax.std.traversable._
			   |import shapeless.syntax.sized._
			   |import syntax.std.tuple._
			   |import shapeless.ops.hlist._
			   |import shapeless.ops.nat._
			   |import shapeless.syntax.nat._
			   |
			   |$lst.toList.sized(Nat(${lst.length})).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			   |""".stripMargin

		val result: List[String] = tb.eval(tb.parse(theCode)).asInstanceOf[List[String]]
		result
	}


	/*def listEnumToListStrLess_OLDMANUALWAY[E <: EnumEntry](lst: List[E]): List[String] = {
		require(lst.length < 22)

		// TODO try next to use the string toolbox compile so can put this part as string without having to duplicate
		// TODO why does it have errors here? how to fill up the implicit?
		// HELP so ugly ... how to make this work? Why can't arbitrary arg be passed in to create Nat?

		val result: List[String] = lst.length match {
			case 0 => List.empty[String]
			case 1 => lst.toList.sized(Nat(1)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 2 => lst.toList.sized(Nat(2)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 3 => lst.toList.sized(Nat(3)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 4 => lst.toList.sized(Nat(4)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 5 => lst.toList.sized(Nat(5)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 6 => lst.toList.sized(Nat(6)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 7 => lst.toList.sized(Nat(7)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 8 => lst.toList.sized(Nat(8)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 9 => lst.toList.sized(Nat(9)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 10 => lst.toList.sized(Nat(10)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 11 => lst.toList.sized(Nat(11)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 12 => lst.toList.sized(Nat(12)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 13 => lst.toList.sized(Nat(13)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 14 => lst.toList.sized(Nat(14)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 15 => lst.toList.sized(Nat(15)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 16 => lst.toList.sized(Nat(16)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 17 => lst.toList.sized(Nat(17)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 18 => lst.toList.sized(Nat(18)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 19 => lst.toList.sized(Nat(19)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 20 => lst.toList.sized(Nat(20)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
			case 21 => lst.toList.sized(Nat(21)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
		}

		result
	}*/


	def listEnumsToListStringAll[E <: EnumEntry](lst: List[E]): List[String] = {

		def reiterate(acc: List[String], rest: List[E]): List[String] = {
			if (rest.isEmpty) acc
			else if (rest.length < 22) acc ++ listEnumToListStrLess(rest)
			else reiterate(acc ++ listEnumToListStr22(rest.take(22)), rest = rest.drop(22))
		}

		reiterate(List.empty[String], lst)
	}


	// -----

	/*import shapeless._
	import syntax.std.tuple._
	//import syntax.std.product._
	import shapeless.ops.hlist._
	import scala.language.implicitConversions


	def conv[T](x: Seq[T]): HList = {
		if (x == Nil) HNil
		else x.head :: conv(x.tail)
	}

	def conv3[T, H <: HList, O <: HList](xs: Seq[T]): O = {

		def helperaccum(acc: H, rest: Seq[T]): O = {
			if (rest.isEmpty) HNil.asInstanceOf[O]
			else helperaccum((rest.head :: acc).asInstanceOf[H], rest.tail)
		}

		val result: O = helperaccum(HNil.asInstanceOf[H], xs)

		result
	}
	def convertListToHlistOfStrings[T, H <: HList, O <: HList, HS <: HList](lst: Seq[T])(implicit mapper: Mapper.Aux[polyEnumsToStr.type, O, HS]): HS = {
		// convert list -> HList
		val he: O = conv[T](lst).toHList[O]// .asInstanceOf[H]
		// convert hlist(enums) -> hlist (strs)
		val hs: HS = he.enumsToString
		hs
	}
	def conv2[T, HE <: HList](x: Seq[T]) = {
		if (x == Nil) HNil //.asInstanceOf[T :: HE]
		else (x.head :: conv2(x.tail)) //.asInstanceOf[T :: HE]
	}
	def convertListToHlistOfStrings[T, H <: HList, O <: HList, HS <: HList](lst: Seq[T])(implicit mapper: Mapper.Aux[polyEnumsToStr.type, O, HS]): HS = {
		// convert list -> HList
		val he: O = conv3[T, H, O](lst) //.asInstanceOf[HE]
		// convert hlist(enums) -> hlist (strs)
		val hs: HS = he.enumsToString
		hs
	}*/

	/*import shapeless.ops.traversable._
	import scala.reflect.runtime.universe._
	type TT = Int :: Int :: Int :: HNil
	case class Holder[T <: HList](tpe: T)(implicit tt: TypeTag[T], fl: FromTraversable[T]) {
		def getHList[A](ob: Seq[A]) /*[T <: HList](implicit fl: FromTraversable[T])*/ = ob.toHList[T]
	}*/
}
