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

import scala.reflect._ //for classtags
import scala.reflect.runtime._
import universe._
import scala.tools.reflect.ToolBox

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


		//implicit class EnumSimpleOps[E <: EnumEntry /*, O <: Enum[E]*/ ](theEnum: E /*O*/)/*(implicit tt: TypeTag[E /*O*/ ])*/ {
		implicit class EnumSimpleOps[E <: EnumEntry](theEnum: E) {
			/**
			 * Nicer way to print enums rather than printing full package name with dots and $.
			 *
			 * @return
			 */
			def name: String = getEnumSimpleName[E](theEnum)
			//typeTag[E].tpe.typeSymbol.toString.split(' ').last
			// equivalent to:
			// def see[T](ob: T) = ob.getClass.getSimpleName
		}

		//implicit class EnumNestedOps[E <: EnumEntry /*, O <: Enum[E]*/ ](theEnum: E /*O*/)(implicit tt: TypeTag[E /*O*/ ]) {
		implicit class EnumNestedOps[E <: EnumEntry](theEnum: E) {
			/**
			 * Nested way to print enums rather than printing full package name with dots and $.
			 *
			 * @return
			 */
			def nestedName: String = getEnumNestedName[E](theEnum)
		}



		trait polyIgnore extends Poly1 {
			implicit def default[T]: Case.Aux[T, T] = at[T](identity)
		}

		object polyEnumsToSimpleStr extends polyIgnore {
			// NOTE: must not put the typebound E <: EnumEntry because when using lst.sized().tupled it returns tuple with type (this.A, this.A ...) and those inside are NOT EnumEntry and so this function won't recognize/work for those. Must keep no typebound.
			implicit def atEnum[E <: EnumEntry]: polyEnumsToSimpleStr.Case.Aux[E, String] = at[E]((enum: E) => getEnumSimpleName[E](enum))

			// NOTE: must use option tuples here because otherwise when mapping over the tuple of elements, it turns ALL of them to string
			/*implicit def atEnum[T]: polyEnumsToSimpleStr.Case.Aux[T, (Option[T], Option[String])] = at[T]((maybeEnum: T) => maybeEnum.isInstanceOf[EnumEntry] match {
				// Case: None of the original type, Some(stringified enum))
				case true => (None, Some(getEnumSimpleName[T](maybeEnum)) )
				// Case: Some(Original type), None stringified enum)
				case false => (Some(maybeEnum), None)
			})*/
			//implicit def extractFromTup[T]: polyEnumsToSimpleStr.Case.Aux[(Option[T], Option[String])]
		}

		object polyEnumsToFullnameStr extends polyIgnore {

			// NOTE: must not put the typebound E <: EnumEntry because when using lst.sized().tupled it returns tuple with type (this.A, this.A ...) and those inside are NOT EnumEntry and so this function won't recognize/work for those. Must keep no typebound.
			implicit def atEnum[E <: EnumEntry]/*(implicit tt: TypeTag[E])*/: polyEnumsToFullnameStr.Case.Aux[E, String] = at[E]((enum: E) => getEnumNestedName[E](enum))

			/*implicit def atEnum[E <: EnumEntry]: polyEnumsToFullnameStr.Case.Aux[E, (Option[E], Option[String])] =
				at[E]((maybeEnum: E) => maybeEnum.isInstanceOf[EnumEntry] match {
				// Case: None of the original type, Some(stringified enum))
				case true => (None, Some(getEnumNestedName[E](maybeEnum)))
				// Case: Some(Original type), None stringified enum)
				case false => (Some(maybeEnum), None)
			})*/

			//implicit def atEnum[E <: EnumEntry]: polyEnumsToFullnameStr.Case.Aux[E, String] = at[E]((enum: E) => enum.toString)
		}

		implicit class EnumHListOps[H <: HList](thehlist: H) {
			//def mapperforenumtostr[O <: HList](implicit mapper: Mapper.Aux[enumsToStr.type, H, O]) = thehlist.map(enumsToStr)(mapper)
			def names[O <: HList](implicit mapper: Mapper.Aux[polyEnumsToSimpleStr.type, H, O] /*, t: Tupler[O]*/): O = {

				// NOTE: now must filter out the tuples to get the Some() wherever they are
				thehlist.map(polyEnumsToSimpleStr)(mapper)
			}

			def nestedNames[O <: HList](implicit mapper: Mapper.Aux[polyEnumsToFullnameStr.type, H, O] /*, t: Tupler[O]*/): O = thehlist.map(polyEnumsToFullnameStr)(mapper)
		}

	}


	// ---------------------------

	import implicits._


	/**
	 * Converting one Enum -> string
	 */

	object Helpers {


		final val PARENT_ENUMS: Seq[String] = Seq(Company.name, Transaction.name, Instrument.name, Art.name, Human.name, Artist.name, Animal.name, WaterType.name, Climate.name, Country.name, Hemisphere.name, CelestialBody.name)

		def getEnumSimpleName[E <: EnumEntry](enumNested: E): String = enumNested.getClass.getSimpleName.init


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

		// ------

		/**
		 * Convert entire list of enums -> list of strings
		 */


		// SOURCE:
		// https://stackoverflow.com/questions/14722860/convert-a-scala-list-to-a-tuple
		// int -> nat: https://stackoverflow.com/questions/39157479/int-optionnat
		// TODO need implicit here since it says error: value tupleToHList is not a member of (this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A, this.A)
		//		lst.sized(Nat(22)).map(_.tupled).get.tupleToHList.enumsToNestedNames.hlistToTuple.toList // TODO how to fix this FIll.Aux error?
		//	def listEnumToListStr22[E <: EnumEntry](lst: List[E]) /*(func: E => String)*/ : List[String] = {
		//		require(lst.length == 22)
		//
		//		//lst.sized(Nat(22)).map(_.tupled).get.tupleToHList.enumsToString.hlistToTuple.toList
		//		//lst.sized(Nat(22)).map(_.tupled).get.tupleToHList.enumsToNestedNames.hlistToTuple.toList // TODO how to fix this FIll.Aux error?
		//		lst.sized(Nat(22)).map(_.tupled).get.enumsToNestedNames.hlistToTuple.toList // TODO how to fix this FIll.Aux error?
		//	}
		//shapeless.ops.hlist.Fill


		def listEnumToListStrLess[E <: EnumEntry](lst: List[E]) /*(func: E => String)*/ : List[String] = {
			require(lst.length <= 22)
			//require(lst.length <= 22)

			val cm = universe.runtimeMirror(getClass.getClassLoader)
			val tb = cm.mkToolBox()


			val enumHubParentEnumImports: String = PARENT_ENUMS.map((parentEnum: String) => s"import com.data.util.EnumHub.$parentEnum._").mkString("\n") // making the imports so that toolbox won't fail with error
			println(s"enumHubParentEnumImports = $enumHubParentEnumImports")

			//import com.data.util.EnumHub.Country._
//			$enumHubParentEnumImports
//			import com.data.util.EnumHub.Animal.SeaCreature._
//			import com.data.util.EnumHub.Animal.Cat.HouseCat._
//			import com.data.util.EnumHub.Animal.Bird.Eagle._

			// TODO must import all the enums ._ this way now ...
			val theCode: String =
				s"""
				   |import com.data.util.EnumHub._
				   |
				   |import utilities.GeneralUtils._
				   |import utilities.EnumUtils.implicits._
				   |
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
				   |$lst.toList.sized(Nat(${lst.length})).map(_.tupled).get.tupleToHList.enumsToNestedNames.hlistToTuple.toList
				   |""".stripMargin

			println(s"theCode = $theCode")

			//$lst.toList.sized(Nat(${lst.length})).map(_.tupled).get.tupleToHList.$func.hlistToTuple.toList

			val result: List[String] = tb.eval(tb.parse(theCode)).asInstanceOf[List[String]]
			result
		}


		def listEnumsToListStringAll[E <: EnumEntry](lst: Seq[E]) /*(func: E => String)*/ : List[String] = {

			def reiterate(acc: List[String], rest: List[E]): List[String] = {
				if (rest.isEmpty) acc
				else reiterate(acc ++ listEnumToListStrLess(rest.take(22)), rest = rest.drop(22))
				//else if (rest.length < 22) acc ++ listEnumToListStrLess(rest)/*(func)*/
				//else reiterate(acc ++ listEnumToListStr22(rest.take(22)), rest = rest.drop(22))

			}

			reiterate(List.empty[String], lst.toList)
		}
	}


	// INPUT
	val atup = (Animal.SeaCreature.Oyster, Animal.Cat, Animal.Cat.HouseCat, Animal.Cat.HouseCat.PersianCat, Animal.Bird.Eagle.GoldenEagle, Animal.Bird, Animal.Fox, Animal)

	val alst = List(Animal.SeaCreature.Oyster, Animal.Cat, Animal.Cat.HouseCat, Animal.Cat.HouseCat.PersianCat, Animal.Bird.Eagle.GoldenEagle, Animal.Bird, Animal.Fox, Animal)

	val clst = List(Country.Arabia, Country.Russia, Country.China, Country.Brazil, Country.Argentina, Country.France, Country.Spain, Country.Italy)

	println("SEE IF ANIMAL NESTED NAMES GETS PRINTED: FOR LIST")
	//println(s"listEnumsToListStringAll(lst) = ${Helpers.listEnumsToListStringAll(alst)}")

	val hlstraw = Animal.SeaCreature.Oyster :: Animal.Cat :: Animal.Cat.HouseCat :: Animal.Fox :: Animal :: HNil
	val hlstsized = alst.sized(8).get.tupled.toHList
	println(s"hlstraw.nestedNames.tupled.to[List] = ${hlstraw.nestedNames.tupled.to[List]}")

	println(s"\nhlstsized.nestedNames.tupled.to[List] = ${hlstsized.nestedNames.tupled.to[List]}" +
		s"\nits type = ${inspector(hlstsized.nestedNames.tupled.to[List])}")
}

