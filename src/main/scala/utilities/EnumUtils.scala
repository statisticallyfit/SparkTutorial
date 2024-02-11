package utilities



// Shapeless things

import shapeless._
import shapeless.ops.hlist._

import shapeless.ops.sized._
import shapeless.syntax.sized._

import shapeless.ops.nat._
import shapeless.syntax.nat._


//import shapeless.ops.tuple._
//import syntax.std.tuple._ // WARNING either this or product
import shapeless.ops.product._
import syntax.std.product._

//import shapeless.ops.traversable.FromTraversable._
//import shapeless.syntax.std.traversable._

/*import scala.reflect._ //for classtags
import scala.reflect.runtime._
import universe._
import scala.tools.reflect.ToolBox*/
import scala.reflect.runtime.universe._

import scala.language.implicitConversions


import com.data.util.EnumHub._
import utilities.DFUtils
import utilities.GeneralUtils._
import DFUtils.implicits._
import DFUtils.TypeAbstractions._

import enumeratum._




/**
 *
 */
object EnumUtils extends App {

	object implicits {

		import Helpers._

		def parentEnumTypeName[T: TypeTag] = typeTag[T].tpe.toString.split('.').last

		//implicit class EnumSimpleOps[E <: EnumEntry /*, O <: Enum[E]*/ ](theEnum: E /*O*/)/*(implicit tt: TypeTag[E /*O*/ ])*/ {
		implicit class EnumOps[E <: EnumEntry](theEnum: E) {
			/**
			 * Nicer way to print enums rather than printing full package name with dots and $.
			 *
			 * @return
			 */
			def name: String = getEnumSimpleName[E](theEnum)
			//typeTag[E].tpe.typeSymbol.toString.split(' ').last
			// equivalent to:
			// def see[T](ob: T) = ob.getClass.getSimpleName
			def nestedName: String = getEnumNestedName[E](theEnum)
		}

		trait polyIgnore extends Poly1 {
			implicit def default[T]: Case.Aux[T, T] = at[T](identity)
		}

		object polyEnumsToSimpleString extends polyIgnore {
			implicit def caseEnum[E <: EnumEntry]: polyEnumsToSimpleString.Case.Aux[E, String] = at[E]((enum: E) => getEnumSimpleName[E](enum))
		}
		object polyEnumsToNestedNameString extends polyIgnore {
			implicit def caseEnum[E <: EnumEntry]: polyEnumsToNestedNameString.Case.Aux[E, String] = at[E]((enum: E) => getEnumNestedName[E](enum))
		}

		/**
		 * This object is for when we want to map over items and convert ALL of them to string, regardless of whether they are enum or not.
		 */
		object polyAllItemsToSimpleNameString extends polyIgnore {
			//implicit def anyOtherTypeCase[A]: this.Case.Aux[A, String] = at[A]((anyType: A) => anyType.toString)
			implicit def caseAnyType[A]: polyAllItemsToSimpleNameString.Case.Aux[A, String] = at[A]((anyType: A) => anyType.toString)
			implicit def caseEnum[E <: EnumEntry]: polyAllItemsToSimpleNameString.Case.Aux[E, String] = at[E]((enum: E) => getEnumSimpleName[E](enum))
		}

		/**
		 * This object is for when we want to map over items and convert ALL of them to string, regardless of whether they are enum or not.
		 */
		object polyAllItemsToNestedNameString extends polyIgnore {
			//implicit def anyOtherTypeCase[A]: this.Case.Aux[A, String] = at[A]((anyType: A) => anyType.toString)
			implicit def caseAnyType[A]: polyAllItemsToNestedNameString.Case.Aux[A, String] = at[A]((anyType: A) => anyType.toString)

			implicit def caseEnum[E <: EnumEntry]: polyAllItemsToNestedNameString.Case.Aux[E, String] = at[E]((enum: E) => getEnumNestedName[E](enum))
		}

		implicit class EnumHListOps[H <: HList](thehlist: H) {
			//def mapperforenumtostr[O <: HList](implicit mapper: Mapper.Aux[enumsToStr.type, H, O]) = thehlist.map(enumsToStr)(mapper)
			def names[O <: HList](implicit mapper: Mapper.Aux[polyEnumsToSimpleString.type, H, O] /*, t: Tupler[O]*/): O = {
				thehlist.map(polyEnumsToSimpleString)(mapper)
			}
			def nestedNames[O <: HList](implicit mapper: Mapper.Aux[polyEnumsToNestedNameString.type, H, O] /*, t: Tupler[O]*/): O = thehlist.map(polyEnumsToNestedNameString)(mapper)

			def namesAll[O <: HList](implicit mapper: Mapper.Aux[polyAllItemsToSimpleNameString.type, H, O] /*, t: Tupler[O]*/): O = {

				// NOTE: now must filter out the tuples to get the Some() wherever they are
				thehlist.map(polyAllItemsToSimpleNameString)(mapper)
			}
			def nestedNamesAll[O <: HList](implicit mapper: Mapper.Aux[polyAllItemsToNestedNameString.type, H, O] /*, t: Tupler[O]*/): O = thehlist.map(polyAllItemsToNestedNameString)(mapper)
		}

		implicit class ListOfEnumsOps[E <: EnumEntry](lst: Seq[E]){
			def names: Seq[String] = lst.map(x => getEnumSimpleName(x))
			def nestedNames: Seq[String] = lst.map(x => getEnumNestedName(x))
		}

		/*implicit class ListOps(lst: Seq[_]) {
			import utilities.EnumUtils.Helpers._
			// NOTE: more elegant to turn the list -> hlist then can map the polymorphic function over it
			def namesAll: Seq[String] = lst.map(x => getSimpleName(x))
			def nestedNamesAll: Seq[String] = lst.map(x => getNestedName(x))
			// convert List[Any] to spark row
			def listToSparkRow: Row = Row(lst: _*)
		}*/
	}


	// ---------------------------

	import implicits._


	/**
	 * Converting one Enum -> string
	 */

	object Helpers {


		final val PARENT_ENUMS: Seq[String] = Seq(Company.name, Transaction.name, Instrument.name, ArtDomain.name, Human.name, Artist.name, Animal.name, WaterType.name, Climate.name, Country.name, Hemisphere.name, CelestialBody.name)

		def getSimpleName[T](item: T): String = item.getClass.getSimpleName.init
		def getEnumSimpleName[E <: EnumEntry](enumNested: E): String = enumNested.getClass.getSimpleName.init

		def getNestedName[T](item: T): String = {
			val rawName: String = item.getClass.getTypeName // e.g. com.data.util.EnumHub$Animal$Cat$HouseCat$PersianCat$

			val pckgName = rawName.split('$').head // e.g. com.data.util.EnumHub
			val leftover = rawName.split('$').tail // e.g. Array(Animal, Cat, HouseCat, PersianCat)

			val parentEnum: String = leftover.head
			val nestedName: String = leftover.mkString(".")

			nestedName
		}

		// NOTE: must not put the typebound E <: EnumEntry because when using lst.sized().tupled it returns tuple with type (this.A, this.A ...) and those inside are NOT EnumEntry and so this function won't recognize/work for those. Must keep no typebound.
		def getEnumNestedName[E <: EnumEntry](enumNested: E) /*(implicit tt: TypeTag[E])*/ : String = {
			val rawName: String = enumNested.getClass.getTypeName // e.g. com.data.util.EnumHub$Animal$Cat$HouseCat$PersianCat$

			val pckgName = rawName.split('$').head // e.g. com.data.util.EnumHub
			val leftover = rawName.split('$').tail // e.g. Array(Animal, Cat, HouseCat, PersianCat)

			val parentEnum: String = leftover.head
			val nestedName: String = leftover.mkString(".")

			nestedName
			/*val enumFullPathname: String = typeTag[E].tpe.toString
			//typeTag[E].tpe.toString // e.g. com.data.util.EnumHub_NAME.Animal.Cat.HouseCat.SiameseCat.type
			println(s"enum arg = $enumNested")
			println(s"FUNCTION getEnumNestedNameFromEnumFullPathname(): enumFullPathname = $enumFullPathname")

			println(s"parent enums = $PARENT_ENUMS")

			// enum parent name e.g. 'Animal' or 'Company' ... one of the items from list above.
			val parentEnum: String = PARENT_ENUMS.filter(parentEnumStr => enumFullPathname.contains(parentEnumStr)).head
			println(s"FUNCTION getEnumNestedNameFromEnumFullPathname(): parentEnum = $parentEnum")

			// gets only the nested name (the part after the parent enum).e.g Animal.SeaCreature.Oyster
			val ip: Int = enumFullPathname.split('.').indexOf(parentEnum)
			enumFullPathname.split('.').drop(ip).init.mkString(".")*/
		}

		def getPackageNameFromEnumPathname[E <: EnumEntry](enumNested: E)(implicit tt: TypeTag[E]): String = {
			val enumFullPathname: String = typeTag[E].tpe.toString // e.g. com.data.util.EnumHub_NAME.Animal.Cat.HouseCat.SiameseCat.type

			// enum parent name e.g. 'Animal' or 'Company' ... one of the items from list above.
			val parentEnum: String = PARENT_ENUMS.filter(parentEnumStr => enumFullPathname.contains(parentEnumStr)).head

			// gets package name
			// example == com.data.util.EnumHub_NAME
			val ip: Int = enumFullPathname.split('.').indexOf(parentEnum)
			enumFullPathname.split('.').take(ip).mkString(".")
		}

	}


	// INPUT
	val atup = (Animal.SeaCreature.Oyster, Animal.Cat, Animal.Cat.DomesticCat, Animal.Cat.DomesticCat.PersianCat, Animal.Bird.Eagle.GoldenEagle, Animal.Bird, Animal.Fox, Animal)

	val alst = List(Animal.SeaCreature.Oyster, Animal.Cat, Animal.Cat.DomesticCat, Animal.Cat.DomesticCat.PersianCat, Animal.Bird.Eagle.GoldenEagle, Animal.Bird, Animal.Fox, Animal)

	val clst = List(Country.Arabia, Country.Russia, Country.China, Country.Brazil, Country.Argentina, Country.France, Country.Spain, Country.Italy)

	println("SEE IF ANIMAL NESTED NAMES GETS PRINTED: FOR LIST")
	//println(s"listEnumsToListStringAll(lst) = ${Helpers.listEnumsToListStringAll(alst)}")

	val longerlist = alst ++ alst ++ alst

	val hlstraw = Animal.SeaCreature.Oyster :: Animal.Cat :: Animal.Cat.DomesticCat :: Animal.Fox :: Animal :: HNil
	val hlstsized = alst.sized(8).get.tupled.toHList
	println(s"hlstraw.nestedNames.tupled.to[List] = ${hlstraw.nestedNames.tupled.to[List]}")

	println(s"\nhlstsized.nestedNames.tupled.to[List] = ${hlstsized.nestedNames.tupled.to[List]}" +
		s"\nits type = ${inspector(hlstsized.nestedNames.tupled.to[List])}")


	println(s"\nhlstraw.namesEnumOnly = ${hlstraw.names}")
	println(s"\nhlstraw.namesAll = ${hlstraw.namesAll}")


	/*println("seeing: hlist -> list")
	println(hlstraw.hlistToList.getClass.getSimpleName)
	println(hlstsized.hlistToList.getClass.getSimpleName)
	println(hlstraw.tupled.to[List].getClass.getSimpleName)
	println(hlstsized.tupled.to[List].getClass.getSimpleName )*/

	/*println("SEEING IF nat compiled works: ")
	val cm = universe.runtimeMirror(getClass.getClassLoader)
	val tb = cm.mkToolBox()
	//val theNat = tb.eval(tb.parse(s"Nat._${(alst ++ alst).length}")).asInstanceOf[Nat]
	val theNat = tb.eval(tb.parse(s"Nat._${8}"))
	println(theNat)
	println(theNat.getClass.getTypeName)
	println(inspector(theNat))*/

	//println(s"\nlonger list = ${Helpers.listOfEnumsToListOfStringsComplete(longerlist)}")
}
