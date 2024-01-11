package com.DocumentingSparkByTestScenarios

/**
 *
 */
object tempEnumeratumTest {


	import enumeratum._
	import enumeratum.values._

	sealed class C extends EnumEntry
	object C1 extends Enum[C] {
		def values: IndexedSeq[C] = findValues
	}

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
}
