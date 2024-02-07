package com.SparkDocumentationByTesting.specs.AboutDataFrames.AboutFiltering

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
	//import syntax.std.tuple._
	//import syntax.std.product._
	import shapeless.ops.hlist._
	import shapeless.ops.nat._
	import shapeless.syntax.nat._



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


	// -----------

	// NOTE: converting list[enum] -> list[string]

	// NOTE: Method - using the toHList[_ :: _ ...] way
	/**
	 * idea:
	 * 1) generate string of: TYPE :: TYPE :: TYPE .... :: HNil
	 * 2) put this string in the code string "lst.toHList[GEN_TYPE]"
	 * 3) reinviate code by calling compile (see old prob dist project)
	 */
	/*val cm = universe.runtimeMirror(getClass.getClassLoader)
	val tb = cm.mkToolBox()
	def thecode[T] = (lst: List[T]) =>
		s"""
		   |
		   |import shapeless._
		   |import shapeless.ops.hlist._
		   |
		   |import shapeless.syntax.sized._
		   |import shapeless.ops.nat._
		   |import shapeless.syntax.nat._
		   |
		   |
		   |//import shapeless.ops.tuple._
		   |//import syntax.std.tuple._ // WARNING either this or product
		   |import shapeless.ops.product._
		   |import syntax.std.product._
		   |
		   |import shapeless.ops.traversable.FromTraversable._
		   |import shapeless.syntax.std.traversable._
		   |
		   |import scala.reflect.runtime._
		   |import universe._
		   |import scala.tools.reflect.ToolBox
		   |
		   |import scala.language.implicitConversions
		   |
		   |import utilities.GeneralUtils._
		   |import com.data.util.EnumHub._
		   |
		   |$lst.toHList[${genh[Animal](HNil, lst.length)}]
		   |""".stripMargin
	tb.eval(tb.parse(thecode(alst)))*/

	// ERROR:
	/*
	scala> tb.eval(tb.parse(thecode(alst)))
	scala.tools.reflect.ToolBoxError: reflective compilation has failed:

	')' expected but '@' found.
	invalid literal number
	at scala.tools.reflect.ToolBoxFactory$ToolBoxImpl$ToolBoxGlobal.throwIfErrors(ToolBoxFactory.scala:332)
	at scala.tools.reflect.ToolBoxFactory$ToolBoxImpl$ToolBoxGlobal.parse(ToolBoxFactory.scala:307)
	at scala.tools.reflect.ToolBoxFactory$ToolBoxImpl.$anonfun$parse$1(ToolBoxFactory.scala:433)
	at scala.tools.reflect.ToolBoxFactory$ToolBoxImpl$withCompilerApi$.apply(ToolBoxFactory.scala:371)
	at scala.tools.reflect.ToolBoxFactory$ToolBoxImpl.parse(ToolBoxFactory.scala:430)
	... 32 elided
	*/


//	val hlst: Option[Animal :: Animal :: HNil] = lst.toHList[Animal :: Animal :: HNil]
//	val hlstStrs: String :: String :: HNil = hlst.get.enumsToString
//	val ss: Seq[EnumString] = hlstStrs.toList

	// ----

	// NOTE: using the sized() .. tupled.. toHList way


	// ------

	/**
	 * Convert entire list of enums -> list of strings
	 */


	// SOURCE:
	// https://stackoverflow.com/questions/14722860/convert-a-scala-list-to-a-tuple
	// int -> nat: https://stackoverflow.com/questions/39157479/int-optionnat
	// TODO need implicit here since it says error: value tupleToHList is not a member of (this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A)


	// WARNING: no longer needed because can change list[enum] directly to list[string]

	/*def listOfEnumsToListOfStrings_Workhorse[E <: EnumEntry](lst: List[E]) /*(func: E => String)*/ : List[String] = {
		require(lst.length <= 22)
		//require(lst.length <= 22)

		val cm = universe.runtimeMirror(getClass.getClassLoader)
		val tb = cm.mkToolBox()



		// NOTE: converting elements here to the right answer so this whole procedure can work for hlist ... this is weird/bad. Works in command line but fails to work here just because the lst evaluates to e.g.List(Oyster, com.data.util.EnumHub$Animal$Cat$@38f2e97e, com.data.util.EnumHub$Animal$Cat$HouseCat$@779dfe55, PersianCat, GoldenEagle, com.data.util.EnumHub$Animal$Bird$@323659f8, Fox, com.data.util.EnumHub$Animal$@1144a55a, Oyster, com.data.util.EnumHub$Animal$Cat$@38f2e97e, com.data.util.EnumHub$Animal$Cat$HouseCat$@779dfe55, PersianCat, GoldenEagle, com.data.util.EnumHub$Animal$Bird$@323659f8, Fox, com.data.util.EnumHub$Animal$@1144a55a, Oyster, com.data.util.EnumHub$Animal$Cat$@38f2e97e, com.data.util.EnumHub$Animal$Cat$HouseCat$@779dfe55, PersianCat, GoldenEagle, com.data.util.EnumHub$Animal$Bird$@323659f8).toList.sized(Nat._22).get.tupled.toHList.nestedNames.tupled.to[List]
		//  which doesn 't allow the nestedNames function area to work to get the right names(gets mixed up in the @sign somehow)

		val theNat = tb.eval(tb.parse(s"Nat._${lst.length}" ) ).asInstanceOf[Nat]

		val theCode: String =
			s"""
			|import com.data.util.EnumHub._
			|import utilities.EnumUtils.implicits._
			|import utilities.GeneralUtils._
			|import enumeratum._
			|
			|import scala.reflect.runtime.universe._
			|
			|import scala.language.implicitConversions
			|
			|import shapeless._
			|import shapeless.ops.hlist._
			|import shapeless.ops.sized._
			|import shapeless.syntax.sized._
			|import shapeless.ops.nat._
			|import shapeless.syntax.nat._
			|import shapeless.ops.product._
			|import syntax.std.product._
			|
			|${lst.nestedNames}.toList.sized(Nat._${lst.length}).get.tupled.toHList.nestedNames.tupled.to[List]
			|""".stripMargin

		println(s"theCode = $theCode")

		//$lst.toList.sized(Nat(${lst.length})).map(_.tupled).get.tupleToHList.$func.hlistToTuple.toList

		val result: List[String] = tb.eval(tb.parse(theCode)).asInstanceOf[List[String]]
		result
	}
	// HELP function crashes with:
	// Exception in thread "main" scala.tools.reflect.ToolBoxError: reflective compilation has failed:
	//
	//value nestedNames is not a member of this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: this.A :: shapeless.HNil
	//	at scala.tools.reflect.ToolBoxFactory$ToolBoxImpl$ToolBoxGlobal.throwIfErrors(ToolBoxFactory.scala:332)


	def listOfEnumsToListOfStringsComplete[E <: EnumEntry](lst: Seq[E]) /*(func: E => String)*/ : List[String] = {

		def reiterate(acc: List[String], rest: List[E]): List[String] = {
			if (rest.isEmpty) acc
			else reiterate(acc ++ listOfEnumsToListOfStrings_Workhorse(rest.take(22)), rest = rest.drop(22))
			//else if (rest.length < 22) acc ++ listEnumToListStrLess(rest)/*(func)*/
			//else reiterate(acc ++ listEnumToListStr22(rest.take(22)), rest = rest.drop(22))

		}

		reiterate(List.empty[String], lst.toList)
	}*/



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
