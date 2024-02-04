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
	import syntax.std.tuple._
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
	/**
	 * idea:
	 * 1) generate string of: TYPE :: TYPE :: TYPE .... :: HNil
	 * 2) put this string in the code string "lst.toHList[GEN_TYPE]"
	 * 3) reinviate code by calling compile (see old prob dist project)
	 */


	/*val cm = universe.runtimeMirror(getClass.getClassLoader)

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
	val ss: Seq[EnumString] = hlstStrs.toList*/

	// ----


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
