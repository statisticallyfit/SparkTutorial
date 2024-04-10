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
import scala.reflect._ //for classtag
import scala.reflect.runtime.universe._

import scala.language.implicitConversions


import utilities.EnumHub._
import utilities.DFUtils
import utilities.GeneralMainUtils._
import utilities.GeneralMainUtils.implicits._
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
			def enumName: String = getEnumSimpleName[E](theEnum)
			//typeTag[E].tpe.typeSymbol.toString.split(' ').last
			// equivalent to:
			// def see[T](ob: T) = ob.getClass.getSimpleName
			def enumNestedName: String = getEnumNestedName[E](theEnum)
		}

		trait polyIgnore extends Poly1 {
			implicit def default[T]: Case.Aux[T, T] = at[T](identity)
		}

		object polyEnumsToSimpleString extends polyIgnore {
			import utilities.EnumUtils.Helpers._
			implicit def caseEnum[E <: EnumEntry]: polyEnumsToSimpleString.Case.Aux[E, String] = at[E]((enum: E) => getEnumSimpleName[E](enum))
			implicit def caseJodaDate: polyEnumsToSimpleString.Case.Aux[DateYMD, String] = at[DateYMD]((d: DateYMD) => d. /*joda.*/ toString)
			implicit def caseArrayOfEnums[E <: EnumEntry]: polyEnumsToSimpleString.Case.Aux[Array[E], Array[String]] = at((arr: Array[E]) => arr.map(elem => caseEnum(elem)))
			implicit def caseSeqOfEnums[E <: EnumEntry]: polyEnumsToSimpleString.Case.Aux[Seq[E], Seq[String]] = at((seq: Seq[E]) => seq.map(elem => caseEnum(elem)))
			implicit def caseString: polyEnumsToSimpleString.Case.Aux[String, String] = at[String]((str: String) => str)

			/*implicit def caseAnyType[A]: polyEnumsToSimpleString.Case.Aux[A, String] = at[A] {
				(anyType: A) => GeneralMainUtils.Helpers.getSimpleString(anyType)
			}*/
		}

		/**
		 * This object is for when we want to map over items and convert ALL of them to string, regardless of whether they are enum or not.
		 */
		object polyAllItemsToSimpleNameString extends polyIgnore {

			import utilities.EnumUtils.Helpers._
			implicit def caseEnum[E <: EnumEntry]: polyAllItemsToSimpleNameString.Case.Aux[E, String] = at[E]((enum: E) => getEnumSimpleName[E](enum))
			implicit def caseJodaDate: polyAllItemsToSimpleNameString.Case.Aux[DateYMD, String] = at[DateYMD]((d: DateYMD) => d. /*joda.*/ toString)
			implicit def caseArrayOfEnums[E <: EnumEntry]: polyAllItemsToSimpleNameString.Case.Aux[Array[E], Array[String]] = at((arr: Array[E]) => arr.map(elem => caseEnum(elem)))

			implicit def caseSeqOfEnums[E <: EnumEntry]: polyAllItemsToSimpleNameString.Case.Aux[Seq[E], Seq[String]] = at((seq: Seq[E]) => seq.map(elem => caseEnum(elem)))

			//implicit def caseArrayOfAny[T]: polyAllItemsToSimpleNameString.Case.Aux[Array[T], Array[String]] = at((arr: Array[T]) => arr.map(anyType => GeneralMainUtils.Helpers.getSimpleString(anyType)))
			// The nested array case
			/*implicit def caseArrayOfAny[T]: polyAllItemsToSimpleNameString.Case.Aux[Array[T], Array[Any]] = at((arr: Array[T]) => {

				def helper(acc: Array[Any], rest: Array[T]): Array[Any] = {
					if (rest.isEmpty) acc
					else helper(acc :+ GeneralMainUtils.Helpers.getSimpleString(rest.head), rest.tail)
				}

				helper(Array(), arr)
			})
			// The nested array case
			implicit def caseSeqOfAny[T]: polyAllItemsToSimpleNameString.Case.Aux[Seq[T], Seq[Any]] = at((seq: Seq[T]) => {

				def helper(acc: Seq[Any], rest: Seq[T]): Seq[Any] = {
					if (rest.isEmpty) acc
					else helper(acc :+ GeneralMainUtils.Helpers.getSimpleString(rest.head), rest.tail)
				}

				helper(Seq(), seq)
			})*/
			//val tts = (Seq(Animal.Cat, Animal.Bird.Pelican, Animal.Bird.Swan), true)
			//tts.toHList.enumNames.tupled

			/*implicit def caseSeqOfAny[T, S <: Seq[_]]: polyAllItemsToSimpleNameString.Case.Aux[Seq[T], S] = at((seq: Seq[T]) => seq.map(anyType => anyType match {
				case _:Seq[T] => caseSeqOfAny(anyType) // recursive case
				case _ => GeneralMainUtils.Helpers.getSimpleString(anyType) // base case
			}))*/

			//import scala.collection.Iterable
			// TODO how to handle nested array case?
			//implicit def caseArraysNested[A, B]: polyAllItemsToSimpleNameString.Case.Aux[A, B] = at((arrA: Array[Array[A]]))
			//implicit def caseArraysNested[A <: Iterable[A], B <: Iterable[B]]: polyAllItemsToSimpleNameString.Case.Aux[A, B] = at((arrA: Array[A]) => arrA.map(elemA => caseArraysNested(elemA)))
			implicit def caseString: polyAllItemsToSimpleNameString.Case.Aux[String, String] = at[String]((str: String) => str)

		}

		object polyEnumsToNestedNameString extends polyIgnore {
			implicit def caseEnum[E <: EnumEntry]: polyEnumsToNestedNameString.Case.Aux[E, String] = at[E]((enum: E) => getEnumNestedName[E](enum))

			implicit def caseJodaDate: polyEnumsToNestedNameString.Case.Aux[DateYMD, String] = at[DateYMD]((d: DateYMD) => d. /*joda.*/ toString)
		}

		/**
		 * This object is for when we want to map over items and convert ALL of them to string, regardless of whether they are enum or not.
		 */
		object polyAllItemsToNestedNameString extends polyIgnore {
			//implicit def anyOtherTypeCase[A]: this.Case.Aux[A, String] = at[A]((anyType: A) => anyType.toString)
			implicit def caseEnum[E <: EnumEntry]: polyAllItemsToNestedNameString.Case.Aux[E, String] = at[E]((enum: E) => getEnumNestedName[E](enum))
			//implicit def caseJodaDate: polyAllItemsToNestedNameString.Case.Aux[DateYMD, String] = at[DateYMD]((d: DateYMD) => d.joda.toString)
			implicit def caseAnyType[A]: polyAllItemsToNestedNameString.Case.Aux[A, String] = at[A]((anyType: A) => anyType.toString)
			implicit def caseString: polyAllItemsToNestedNameString.Case.Aux[String, String] = at[String]((str: String) => str)
		}

		implicit class EnumHListOps[H <: HList](thehlist: H) {
			//def mapperforenumtostr[O <: HList](implicit mapper: Mapper.Aux[enumsToStr.type, H, O]) = thehlist.map(enumsToStr)(mapper)
			def enumNames[O <: HList](implicit mapper: Mapper.Aux[polyEnumsToSimpleString.type, H, O] /*, t: Tupler[O]*/): O = {
				thehlist.map(polyEnumsToSimpleString)(mapper)
			}
			def enumNestedNames[O <: HList](implicit mapper: Mapper.Aux[polyEnumsToNestedNameString.type, H, O] /*, t: Tupler[O]*/): O = thehlist.map(polyEnumsToNestedNameString)(mapper)

			// converts all entries to string
			def stringNamesOrValues[O <: HList](implicit mapper: Mapper.Aux[polyAllItemsToSimpleNameString.type, H, O] /*, t: Tupler[O]*/): O = {

				// NOTE: now must filter out the tuples to get the Some() wherever they are
				thehlist.map(polyAllItemsToSimpleNameString)(mapper)
			}
			def stringNestedNamesOrValues[O <: HList](implicit mapper: Mapper.Aux[polyAllItemsToNestedNameString.type, H, O] /*, t: Tupler[O]*/): O = thehlist.map(polyAllItemsToNestedNameString)(mapper)
		}

		implicit class ListOfEnumsOps[E <: EnumEntry](lst: Seq[E]){
			def enumNames: Seq[String] = lst.map(x => getEnumSimpleName(x))
			def enumNestedNames: Seq[String] = lst.map(x => getEnumNestedName(x))
		}

		implicit class ArrayOfEnumsOps[E <: EnumEntry](arr: Array[E]) {
			def enumNames: Seq[String] = arr.map(x => getEnumSimpleName(x))
			def enumNestedNames: Seq[String] = arr.map(x => getEnumNestedName(x))
		}



		implicit class CheckWorld[W <: World](lst: Seq[W]) {
			/**
			 * GOAL: when given a list of values from World, return the elements that are of query type C
			 * Example usage: World.values.returnIt[Russia] should return Russia.values + Russia
			 *
			 * SOURCE: https://stackoverflow.com/a/21181344
			 */
			def returnIt[C <: World : ClassTag]: Seq[C] = lst.map(c => {
				//typeOf[W]
				val b = classTag[C].runtimeClass.isInstance(c) //w.isInstanceOf[W]
				if (b) c else null
			}).filterNot(_ == null).asInstanceOf[Seq[C]]

			/**
			 * GOAL: when given a list of values from World, return C for the elements that are of query type C
			 * Example: World.values.returnCopy[Russia] should return Russia, Russia, Russia ... for as many elements there are in  Russia.values
			 */
			def returnParent[C <: World : ClassTag]: Seq[EnumString] = lst.map(c => {
				val b = classTag[C].runtimeClass.isInstance(c) //w.isInstanceOf[W]
				if (b) classTag[C].runtimeClass.getSimpleName else null
			}).filterNot(_ == null).asInstanceOf[Seq[EnumString]]


			import World.Africa._
			import World.Europe._
			import World.NorthAmerica._
			import World.SouthAmerica._
			import World._
			import World.Asia._
			import World.Oceania._
			import World.CentralAmerica._

			def returnMultiParent: Seq[W] = lst.map((c: W) => c match {
				case _: Russia => Russia
				case _: UnitedStates => UnitedStates
				case _: Canada => Canada
				case _: Oceania => Oceania
				case _: SouthAmerica => SouthAmerica
				case _: CentralAmerica => CentralAmerica
				case _: France => France
				case _: England => England
				case _: Europe => Europe
				case _: Africa => Africa
				case _: Arabia => Arabia //if classTag[Arabia].runtimeClass.isInstance(c) => ar
				case _: Asia => Asia // no
				//case _: Italy => Italy // no
				case _ => null
			}).asInstanceOf[Seq[W]]

		}
	}


	// ---------------------------

	import implicits._


	/**
	 * Converting one Enum -> string
	 */

	object Helpers {


		// TODO use this? in order to more automatically develop the packagenames for collectEnumCol ???
		//final val PARENT_ENUMS: Seq[String] = Seq(Company.enumName, Transaction.enumName, Instrument.enumName, Craft.enumName, Human.enumName, Artist.enumName, Animal.enumName, WaterType.enumName, ClimateZone.enumName, World.enumName, Hemisphere.enumName, CelestialBody.enumName)

		def getEnumSimpleName[E <: EnumEntry](enumNested: E): String = {
			if(enumNested == null) "null" else enumNested.getClass.getSimpleName.init
		}



		// NOTE: must not put the typebound E <: EnumEntry because when using lst.sized().tupled it returns tuple with type (this.A, this.A ...) and those inside are NOT EnumEntry and so this function won't recognize/work for those. Must keep no typebound.
		def getEnumNestedName[E <: EnumEntry](enumNested: E) /*(implicit tt: TypeTag[E])*/ : String = {
			if(enumNested == null) return "null"
			// else
			val rawName: String = enumNested.getClass.getTypeName // e.g. utilities.EnumHub$Animal$Cat$HouseCat$PersianCat$

			val pckgName: String = rawName.split('$').head // e.g. utilities.EnumHub
			val leftover: Array[String] = rawName.split('$').tail // e.g. Array(Animal, Cat, HouseCat, PersianCat)

			val parentEnum: String = leftover.head
			val nestedName: String = leftover.mkString(".")

			nestedName
			/*val enumFullPathname: String = typeTag[E].tpe.toString
			//typeTag[E].tpe.toString // e.g. utilities.EnumHub_NAME.Animal.Cat.HouseCat.SiameseCat.type
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

		/*def getPackageNameFromEnumPathname[E <: EnumEntry](enumNested: E)(implicit tt: TypeTag[E]): String = {
			if(enumNested == null) return "null" //else

			val enumFullPathname: String = typeTag[E].tpe.toString // e.g. utilities.EnumHub_NAME.Animal.Cat.HouseCat.SiameseCat.type

			// enum parent name e.g. 'Animal' or 'Company' ... one of the items from list above.
			val parentEnum: String = PARENT_ENUMS.filter(parentEnumStr => enumFullPathname.contains(parentEnumStr)).head

			// gets package name
			// example == utilities.EnumHub_NAME
			val ip: Int = enumFullPathname.split('.').indexOf(parentEnum)
			enumFullPathname.split('.').take(ip).mkString(".")
		}*/


		// ----------------------------------------------------------------------------------------------------------------

		/**
		 * Enum logic for the dfutils function collectEnumCol, better if it stays her ein the enumutils file.
 		 */

		import scala.reflect.runtime._
		import scala.tools.reflect.ToolBox

		// WARNING find way to automate the creating of these string imports? otherwise have to update each timei add a new one.
		// TODO update here already - have mathematician/scientist group


		val ENUM_IMPORTS =
		"""
		  |// NOTE: importing enums that are 1) nested, and 2) can be names of columns in dataframes,  so that the reflection-parser for collectenumcol can see those enums, otherwise withName() won't work.
		  |
		  |import utilities.EnumHub._
		  |
		  |import Instrument._;
		  |import FinancialInstrument._;  import Commodity._ ; import PreciousMetal._; import Gemstone._
		  |import MusicalInstrument._;  import BassInstrument._; import StringInstrument._; import  WoodwindInstrument._
		  |
		  |import Human._
		  |import ArtPeriod._
		  |import Artist._ ; import Painter._; import Writer._; import Sculptor._; import Architect._; import Dancer._; import Singer._; import Actor._; import Musician._
		  |import Scientist._ ; import NaturalScientist._ ; import Mathematician._;  import Engineer._
		  |import Craft._;
		  |import Art._ ; import Literature._; import PublicationMedium._; import Genre._;
		  |import Science._; import NaturalScience._ ; import Mathematics._ ; import Engineering._ ;
		  |
		  |//import Tree._; import Flower._
		  |
		  |import Animal._  ; import Insect._; import Reptile._; import Cat._; import DomesticCat._; import WildCat._ ; import SeaCreature._ ; import Whale._ ; import Bird._; import Eagle._;
		  |
		  |//import WaterType._
		  |
		  |import World.Africa._
		  |import World.Europe._
		  |import World.NorthAmerica._
		  |import World.SouthAmerica._
		  |import World._
		  |import World.Asia._
		  |import World.Oceania._
		  |import World.CentralAmerica._
		  |
		  |import CelestialBody._ ; import Planet._ ; import Galaxy._ ; import Constellation._
		  |""".stripMargin
		// TODO TO ADD SOON: Rodent, WeaselMustelid, Canine, Amphibian .... BIOMES

		type CodeString = String

		/**
		 * Key Hacky Strategy: Treating Y = EnumEntry like an E = Enum[Y] so can call the withName method that exists only for Enum[Y]. If not doing this then have to pass both as type parameters within the function like so:
		 * e.g. collectCol[Y, E](obj: E)
		 * and that looks ugly and too stuffy when calling the function,
		 * e.g. collectCol[Animal, Animal.type](Animal)
		 */
		// NOTE: the withName() function returns type ENumEntry anyway so no need to worry of converting the result to Enum[Y], which would have type Animal.Fox.type instead of the type here Animal.Fox
		def funcEnumStrToCode[Y: TypeTag](enumStr: EnumString): CodeString =
			s"""
			   |import enumeratum._
			   |import scala.reflect.runtime.universe._
			   |${ENUM_IMPORTS}
			   |
			   |${parentEnumTypeName[Y]}.withName("$enumStr")
			   |""".stripMargin

		def funcCodeToResultY[Y: TypeTag](tb: ToolBox[universe.type])(codeStr: CodeString): Y = tb.eval(tb.parse(codeStr)).asInstanceOf[Y]

		// WARNING do i have to move this back to dfutils under collectenumcol? it is very slowin command line when this code is here.... is that why???

	}


	// INPUT
	val atup = (Animal.SeaCreature.Oyster, Animal.Cat, Animal.Cat.DomesticCat, Animal.Cat.DomesticCat.PersianCat, Animal.Bird.Eagle.GoldenEagle, Animal.Bird, Animal.Canine.WildCanine.Fox, Animal)

	val alst = List(Animal.SeaCreature.Oyster, Animal.Cat, Animal.Cat.DomesticCat, Animal.Cat.DomesticCat.PersianCat, Animal.Bird.Eagle.GoldenEagle, Animal.Bird, Animal.Canine.WildCanine.Fox, Animal)

	import World.Africa._
	import World.Europe._
	import World.NorthAmerica._
	import World.SouthAmerica._
	import World._
	import World.Asia._
	import World.Oceania._
	import World.CentralAmerica._
	val clst = List(Arabia, Russia, China, Brazil, Argentina, France, Spain, Italy)

	println("SEE IF ANIMAL NESTED NAMES GETS PRINTED: FOR LIST")
	//println(s"listEnumsToListStringAll(lst) = ${Helpers.listEnumsToListStringAll(alst)}")

	val longerlist = alst ++ alst ++ alst

	val hlstraw = Animal.SeaCreature.Oyster :: Animal.Cat :: Animal.Cat.DomesticCat :: Animal.Canine.WildCanine.Fox :: Animal :: HNil
	val hlstsized = alst.sized(8).get.tupled.toHList
	println(s"hlstraw.nestedNames.tupled.to[List] = ${hlstraw.enumNestedNames.tupled.to[List]}")

	println(s"\nhlstsized.nestedNames.tupled.to[List] = ${hlstsized.enumNestedNames.tupled.to[List]}" +
		s"\nits type = ${inspector(hlstsized.stringNamesOrValues.tupled.to[List])}")


	println(s"\nhlstraw.namesEnumOnly = ${hlstraw.enumNames}")
	println(s"\nhlstraw.namesAll = ${hlstraw.stringNamesOrValues}")


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

