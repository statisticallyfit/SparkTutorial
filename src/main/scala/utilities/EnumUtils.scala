package utilities

import shapeless._
import syntax.std.tuple._
//import syntax.std.product._
import shapeless.ops.hlist._
import shapeless.ops.sized._
import shapeless.syntax.sized._
import shapeless.ops.nat._
import shapeless.syntax.nat._

import shapeless.ops.traversable.FromTraversable._
import shapeless.syntax.std.traversable._

import scala.language.implicitConversions

import scala.reflect.runtime._
import universe._
import scala.tools.reflect.ToolBox

import enumeratum._

import com.data.util.EnumHub._


/**
 *
 */
object EnumUtils extends App {

	object implicits {

		/*implicit class SeqEnumOps[E <: EnumEntry](seqEnum: Seq[E])(implicit tt: TypeTag[E]){
			def inspector[T: TypeTag](ob: T): String = typeTag[T].tpe.typeSymbol.toString.split(' ').last
			def str: Seq[String] = seqEnum.map((enum: E) => inspector[E](enum))
		}*/

		trait polyIgnore extends Poly1 {
			implicit def default[T]: Case.Aux[T, T] = at[T](identity)
		}

		object polyEnumsToSimpleStr extends polyIgnore {
			implicit def atEnum[E <: EnumEntry]: polyEnumsToSimpleStr.Case.Aux[E, String] = at[E]((enum: E) => enum.toString)
		}

		object polyEnumsToFullnameStr extends polyIgnore {
			import Helpers._
			implicit def atEnum[E <: EnumEntry](implicit tt: TypeTag[E]): polyEnumsToFullnameStr.Case.Aux[E, String] = at[E]((enum: E) => getEnumNestedNameFromEnumPathname[E](enum))

			//implicit def atEnum[E <: EnumEntry]: polyEnumsToFullnameStr.Case.Aux[E, String] = at[E]((enum: E) => enum.toString)
		}

		implicit class EnumHListOps[H <: HList](thehlist: H) {
			//def mapperforenumtostr[O <: HList](implicit mapper: Mapper.Aux[enumsToStr.type, H, O]) = thehlist.map(enumsToStr)(mapper)
			def enumsToShortNames[O <: HList](implicit mapper: Mapper.Aux[polyEnumsToSimpleStr.type, H, O] /*, t: Tupler[O]*/): O = thehlist.map(polyEnumsToSimpleStr)(mapper)

			def enumsToNestedNames[O <: HList](implicit mapper: Mapper.Aux[polyEnumsToFullnameStr.type, H, O] /*, t: Tupler[O]*/): O = thehlist.map(polyEnumsToFullnameStr)(mapper)
		}

		implicit class EnumOps[E <: EnumEntry /*, O <: Enum[E]*/ ](theEnum: E /*O*/)(implicit tt: TypeTag[E /*O*/ ]) {

			/**
			 * Nicer way to print enums rather than printing full package name with dots and $.
			 *
			 * // WARNING: this only works when the object Name extends also the trait Name not just the Enum[Name] because it has to be of type extending EnumEntry
			 *
			 * @return
			 */
			def enumSimpleName: String = typeTag[E].tpe.typeSymbol.toString.split(' ').last

			def enumNestedName: String = {
				import Helpers._

				getEnumNestedNameFromEnumPathname[E](theEnum)
			}
		}
	}


	// ---------------------------

	import implicits._


	/**
	 * Converting one Enum -> string
	 */

	object Helpers {
		final val PARENT_ENUMS: Seq[String] = Seq(Company.enumSimpleName, Transaction.enumSimpleName, Instrument.enumSimpleName, Art.enumSimpleName, Human.enumSimpleName, Artist.enumSimpleName, Animal.enumSimpleName, WaterType.enumSimpleName, Climate.enumSimpleName, Country.enumSimpleName, Hemisphere.enumSimpleName, CelestialBody.enumSimpleName)

		// TODO map this function over the list, and pass which kind of string function to use
		// import utilities.GeneralUtils._
		//val listOfAllParentEnums: Seq[String] = Seq(Company, Transaction)

		def getEnumNestedNameFromEnumPathname[E <: EnumEntry](enumNested: E)(implicit tt: TypeTag[E]): String = {
			val enumFullPathname: String = typeOf[E].resultType.toString
			//typeTag[E].tpe.toString // e.g. com.data.util.EnumHub_NAME.Animal.Cat.HouseCat.SiameseCat.type
			println(s"enum arg = $enumNested")
			println(s"FUNCTION getEnumNestedNameFromEnumFullPathname(): enumFullPathname = $enumFullPathname")

			println(s"parent enums = $PARENT_ENUMS")

			// enum parent name e.g. 'Animal' or 'Company' ... one of the items from list above.
			val parentEnum: String = PARENT_ENUMS.filter(parentEnumStr => enumFullPathname.contains(parentEnumStr)).head
			println(s"FUNCTION getEnumNestedNameFromEnumFullPathname(): parentEnum = $parentEnum")

			// gets only the nested name (the part after the parent enum).e.g Animal.SeaCreature.Oyster
			val ip: Int = enumFullPathname.split('.').indexOf(parentEnum)
			enumFullPathname.split('.').drop(ip).init.mkString(".")
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

			//import com.data.util.EnumHub.Country._
			/*import com.data.util.EnumHub.Animal.SeaCreature._
			import com.data.util.EnumHub.Animal.SeaCreature._
			import com.data.util.EnumHub.Animal.Cat.HouseCat._
			import com.data.util.EnumHub.Animal.Bird.Eagle._*/

			val enumHubParentEnumImports: String = PARENT_ENUMS.map((parentEnum: String) => s"import com.data.util.EnumHub.$parentEnum._").mkString("\n") // making the imports so that toolbox won't fail with error
			println(s"enumHubParentEnumImports = $enumHubParentEnumImports")

			// TODO must import all the enums ._ this way now ...
			val theCode: String =
				s"""
				   |import com.data.util.EnumHub._
				   |$enumHubParentEnumImports
				   |import com.data.util.EnumHub.Animal.SeaCreature._
				   |import com.data.util.EnumHub.Animal.Cat.HouseCat._
				   |import com.data.util.EnumHub.Animal.Bird.Eagle._
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




	val lst: Seq[Animal] = List(Animal.Squirrel, Animal.SeaCreature.Dolphin, Animal.Bird.Eagle.GoldenEagle, Animal.Cat.HouseCat.SiameseCat, Animal.Hyena, Animal.SeaCreature.Oyster)


	println("SEE IF ANIMAL NESTED NAMES GETS PRINTED: FOR LIST")
	println(s"listEnumsToListStringAll(lst) = ${Helpers.listEnumsToListStringAll(lst)}")
}

