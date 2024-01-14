package com

/**
 *
 */
object tryoutEnumeratum extends App {

	import enumeratum._

	sealed trait Nesting extends EnumEntry
	object Nesting extends Enum[Nesting] {
		val values = findValues
		case object Hello extends Nesting
		object others {
			case object GoodBye extends Nesting
		}
		case object Hi extends Nesting
		class InnerClass {
			case object NotFound extends Nesting
		}
	}
	Nesting.values


	trait Myenumtype

	sealed trait Planet extends EnumEntry with Myenumtype
	object Planet extends Enum[Planet]  with Myenumtype {
		val values: IndexedSeq[Planet] = findValues
		case object Mars extends Planet with Myenumtype
		case object Jupiter extends Planet with Myenumtype
		case object Venus extends Planet with Myenumtype
	}
	sealed trait Country extends EnumEntry with Myenumtype
	object Country extends Enum[Country] with Myenumtype {
		val values: IndexedSeq[Country] = findValues
		case object Africa extends Country with Myenumtype
		case object France extends Country with Myenumtype
		case object Spain extends Country with Myenumtype
		case object Russia extends Country with Myenumtype
	}
	sealed trait Animal extends EnumEntry with Myenumtype

	object Animal extends Enum[Animal] with Myenumtype {
		val values: IndexedSeq[Animal] = findValues
		case object Giraffe extends Animal with Myenumtype
		case object Penguin extends Animal with Myenumtype
		case object Lion extends Animal with Myenumtype
		case object Chipmunk extends Animal with Myenumtype
	}

	//import shapeless._, ops.hlist._

	sealed trait AstronomicalObject extends EnumEntry
	object AstronomicalObject extends Enum[AstronomicalObject] {
		val values: IndexedSeq[AstronomicalObject] = findValues
		case object BlackHole extends AstronomicalObject
		case object Star extends AstronomicalObject
		case object Nebula extends AstronomicalObject
		case class Asteroid() extends AstronomicalObject
	}




	import shapeless._
	//import syntax.std.tuple._
	import shapeless.ops.hlist._
	import syntax.std.product._

	import scala.language.implicitConversions
	//import shapeless.record._ // key - for updateWith function

	//import shapeless.ops.hlist.Modifier

//	import shapeless.labelled._ // {KeyTag, FieldType}
//	import shapeless.syntax.singleton._

	import AstronomicalObject._
	val astrs = Asteroid() :: BlackHole :: Star :: Nebula :: Asteroid() :: "random element string" :: 1123 :: Star :: HNil

	val seq: Seq[(Animal, String, Int, Country)] = Seq((Animal.Lion, "L", 3, Country.Africa), (Animal.Penguin, "p", 2, Country.Russia), (Animal.Giraffe, "g", 5, Country.Africa))

	val res: Seq[Animal :: String :: Int :: Country :: HNil] = seq.map(_.productElements)

	val h: Animal :: String :: Int :: Country :: HNil = res.head // hslist

	seq.map(_.productElements.updateWith((e: Animal) => e.toString))

	/*implicit class EnumStrOps[T <: Product, E <: EnumEntry](seq: Seq[T]) {
		// pass hlist of enum types to know how to convert
		def convert[H <: HList](hlistOfTypes: H)(implicit replacer : Modifier.Aux[L, U, V, (U, Out)]) = hlistOfTypes.map(ENUMTYPE =>  seq.map(_.productElements.updateWith((elem: E) => elem.toString)) )
	}*/


	/**
	 * TODO task 1: turning any Enum into string (using updateWIth)
	 */
	// NOTE: trying to make abstract type over all enums so can use this function in general not just for SPECIFIC enums
	def hlistenumToStr[E <: EnumEntry, H <: HList, O <: HList](hlst: H)(implicit ev: Modifier.Aux[H, E, String, (E, O)]) =
		hlst.updateWith((elem: E) => elem.toString)


	// NOTE need to use the Mapping approach to accomplish the goal because this function gets applied only to the first occurrence
	hlistenumToStr(h)(Modifier.hlistModify1)


	/**
	 * TODO task 2: filter / select enums to create function "contains" for enum to check the amount of Enums left in the HList.
	 *
	 * ---> way 1: using Shapeless selector
	 *
	 * ---> way 2: using Shapeless filter
	 * https://gist.github.com/yakticus/4622facd96ac6bdf17cf
	 *
	 * ---> way 3: using Shapeless Poly function
	 * https://www.appsloveworld.com/scala/100/63/type-level-filtering-using-shapeless
	 * https://hyp.is/TONDXLC3Ee6ybJdb_0sh6A/jto.github.io/articles/getting-started-with-shapeless/
	 *
	 *
	 */



	//import syntax.HListOps


	// way 1: using Selector
	implicit class SelectEnums[H <: HList](input: H) {
		// select = "Returns the first element of type U of this HList. An explicit type argument must be provided. Available only if there is evidence that this HList has an element of type U."
		def selectEnums[E <: EnumEntry](implicit s: Selector[H, E]) = input.select[E](s)
	}

	// way 2: using Filter
	implicit class FilterEnums[H <: HList](val input: H) {

		def partitionEnums[E <: EnumEntry](implicit pf : Partition[H, E]) = input.partition[E]
		// filter = "Returns all elements of type U of this HList. An explicit type argument must be provided."
		def filterEnums[E <: EnumEntry](implicit pf: Partition[H, E]) = input.filter[E](pf)
		def filter2(implicit pf: Partition[H, EnumEntry]) = input.filter[EnumEntry](pf)

		def filter3[E <: Myenumtype](implicit pf: Partition[H, E]) = input.filter[E](pf)
	}

	/**
	 * NOTE: interesting! can filter TYPES and OBJECTS
	 *
	 * val ha1 = Apple() :: Apple() :: HNil
	 * val ha1: Apple :: Apple :: shapeless.HNil = Apple() :: Apple() :: HNil
	 *
	 * scala> val ha: Apple.type :: Apple.type :: Pear.type :: HNil = Apple :: Apple :: Pear :: shapeless.HNil
	 * val ha: Apple.type :: Apple.type :: Pear.type :: shapeless.HNil = Apple :: Apple :: Pear :: HNil
	 *
	 * scala> val ha1: Apple :: Apple :: HNil = Apple() :: Apple() :: HNil
	 * val ha1: Apple :: Apple :: shapeless.HNil = Apple() :: Apple() :: HNil
	 *
	 * scala> ha.filter[Apple]
	 * val res43: shapeless.HNil = HNil
	 *
	 * scala> ha.filter[Apple.type]
	 * val res44: Apple.type :: Apple.type :: shapeless.HNil = Apple :: Apple :: HNil
	 */
	// NOTE bad side - have to pass in the type manually, cannot filter generic EnumEntry types

	// way 3: using Poly
	// Source code = https://stackoverflow.com/questions/19893633/filter-usage-in-shapeless-scala
	// SOURCE (better) = https://github.com/milessabin/shapeless/issues/73
	trait ignore extends Poly1 {
		implicit def default[T]: Case.Aux[T, T] = at[T](identity)
	}
	object enumsToStr extends ignore {
		implicit def atEnum[E <: EnumEntry]: enumsToStr.Case.Aux[E, String] = at[E](_.toString)

	}

	/**
	 * HELP HELP HELP
	 * 1) convert all enums to string --- goal: have only enum-str list
	 * 2) filter only enums + check hlist contains is empty or ot --- goal: for isempt of hlist condition for while-condition of filtering. --- need only do if cannot convert all the enums to string at once.
	 */

	// goal 1) - done with map poly function - no more having to deal with RightFolder nonsense, having to extract hlist somehow out of that!
	val resH: String :: String :: Int :: String :: HNil = h.map(enumsToStr)
	val resA: String :: String :: String :: String :: String :: String :: Int :: String :: HNil = astrs.map(enumsToStr)


	// NOTE: generalizing the above using the below code:
	implicit class HListToTupStr[H <: HList](thehlist: H) {
		//def mapperforenumtostr[O <: HList](implicit mapper: Mapper.Aux[enumsToStr.type, H, O]) = thehlist.map(enumsToStr)(mapper)
		def stringifyEnums[O <: HList](implicit mapper: Mapper.Aux[enumsToStr.type, H, O] /*, t: Tupler[O]*/) = thehlist.map(enumsToStr)(mapper)
	}
	implicit class TupToHList[T <: Product, H <: HList](tup: T) {
		def tupToHList(implicit gen: Generic[T]) = tup.productElements
		//def tupToHList(implicit /*gen: Generic[T]*/p: ProductToHList[H], t: ProductToHList[T]) = tup.toHList
	}
	implicit class HListToTup[T <: Product, H <: HList](hlist: H) {
		def hlistToTup(implicit tup: Tupler[H]) = hlist.tupled
	}

	// TODO why is the stringify function red it works fine in the repl?
	// Move on: it works fine when running the App too so maybe just IDEA bug?
	//seq.head.tupToHList.stringifyEnums.hlistToTup
	println(seq.map(_.tupToHList.stringifyEnums.hlistToTup))




	// way 4 = using fold
	implicit class PolyEnumOps[H <: HList](val input: H) {
		def mappolyEnums(op: Poly)(implicit mapper : Mapper[op.type, H]) = input.map(op)
		def foldpolyEnums/*[E <: EnumEntry]*/(op: Poly)(implicit folder: RightFolder[H, HNil.type, op.type]): RightFolder[H, HNil.type, op.type]#Out = input.foldRight(HNil)(op)(folder)
		//htemp.foldRight[E](HNil)(keepEnum)
	}

	// TODO find way to filter the general EnumEntry out of Hlist
	//val htemp = Animal.Lion :: Country.Russia :: Animal.Penguin :: Country.Africa :: HNil
	// Filtering ALL enumEntries
	//val filtered: RightFolder[tempEnumeratumTest.Animal.Lion.type :: tempEnumeratumTest.Country.Russia.type :: tempEnumeratumTest.Animal.Penguin.type :: tempEnumeratumTest.Country.Africa.type :: HNil, HNil.type, keepEnum.type]#Out = htemp.foldpolyEnums(keepEnum)
	//filtered.toList.length



	// Try: recurse through list until no more enums, using update with
	/*def hlistenumToStr[E <: EnumEntry, H <: HList, O <: HList](hlst: H)(implicit mod: Modifier.Aux[H, E, String, (E, O)], pf: Partition[H, E], is: IsHCons[O]) ={
		def helper(acc: O, processing: H): O = {
			processing match {
				case HNil => acc
				case _ => {
					val updatedfirstoccurrence = hlst.updateWith((elem: E) => elem.toString)
					val (first, rest) = (updatedfirstoccurrence.head, updatedfirstoccurrence.tail)
					helper(first :: acc, processing.tail)
					// TODO how to append element to hlist acc?
				}
			}
		}
		helper(, hlst) // TODO cannot pass acc
	}*/

	// GOAL: filter the list out of all its EnumEntry members - but the fold only works for specific enums passed in right there. So now to do that for arbitrary Enums?





//	type GL[E] = E :: String :: Int :: E :: HNil // E = MyenumType
//	type H1_ = GL[EnumEntry] // Myenumtype :: String :: Int :: Myenumtype :: HNil
//	type H2_ = GL[String] // String :: String :: Int :: String :: HNil
//
//	implicit val mod: Modifier.Aux[EnumEntry :: String :: Int :: EnumEntry :: HNil, EnumEntry, String, (EnumEntry, String :: String :: Int :: String :: HNil)] {
//		type Out = (EnumEntry, String :: String :: Int :: String :: HNil)
//	} = new Modifier.Aux[EnumEntry :: String :: Int :: EnumEntry :: HNil, EnumEntry, String, (EnumEntry, String :: String :: Int :: String :: HNil)]




	//h.updateWith((e: EnumEntry) => e.toString)(Modifier.hlistModify1)
	//h.updateWith((e: EnumEntry) => e.toString)(mod)

	// TODO make general function that converts enumtype -> string

	/*type GL[E] = E :: String :: Int :: E :: HNil // E = MyenumType

	type H1_ = GL[Myenumtype] // Myenumtype :: String :: Int :: Myenumtype :: HNil
	type H2_ = GL[String] // String :: String :: Int :: String :: HNil
	type U_ = Myenumtype
	type V_ = String
	//type Out_[H] = (U_, V_ :: H)
	type Out_ = (U_, H2_)

	import shapeless.ops.hlist.Modifier
	def my_hlistModify1[H <: HList, U, V]: Aux[U :: H, U, V, (U, V :: H)] =
		new Modifier[U :: H, U, V] {
			type Out = (U, V :: H)

			def apply(l: U :: H, f: U => V): Out = {
				val u = l.head; (u, f(u) :: l.tail)
			}
		}

	implicit def actual_hlistModify1[H <: HList]: Aux[U_ :: H, U_, V_, (U_, V_ :: H)] = my_hlistModify1[H, U_, V_]

	implicit def doupdate[H <: HList](hlist: H)(implicit my_modifier: Modifier.Aux[H, U_, V_, Out_]) = hlist.updateWith[U_, V_, H2_]((e: U_) => e.toString)

//	type TheHList = Animal :: String :: Int :: Planet :: HNil
//
//	implicit def doit[H <: HList, Out](hlist: H)(implicit modifier: Modifier.Aux[H, Myenumtype, String, (Myenumtype, H)]) = hlist.updateWith((e: Myenumtype) => e.toString)
//	//val res2 = h.updateWith((e: Myenumtype) => e.toString)
//
//	implicit val modifier2: Modifier[TheHList, Myenumtype, String] {
//		type Out = (Myenumtype, TheHList)
//	} = new Modifier.Aux[TheHList, Myenumtype, String, (Myenumtype, TheHList)]

	h.updateWith((e: Animal) => e.toString)*/

	/*type C <: Animal :: String :: Int :: Country :: HNil
	type S = String :: String :: Int :: String :: HNil
	implicit def doit[T <: HList, H <: HList](hlist: H)(implicit modifier: Modifier.Aux[H, Myenumtype, String, (Myenumtype, T)]) =  hlist.updateWith((e: Myenumtype) => e.toString)
	//val res2 = h.updateWith((e: Myenumtype) => e.toString)

	implicit val modifier2: Modifier.Aux[Animal :: String :: Int :: Country :: HNil, Myenumtype, String, S]  = new Modifier.Aux[Animal :: String :: Int :: Country :: HNil, Myenumtype, String, S]

	h.updateWith((e: Myenumtype) => e.toString)(modifier2)*/


	/*seq.head.map(e => e.isInstanceOf[EnumEntry] match {
		case true => e.toString
		case false => e
	})
	seq.head.filter(_.isInstanceOf[EnumEntry])*/
}
