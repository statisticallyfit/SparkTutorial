package utilities


import enumeratum._
import enumeratum.values


object EnumHub  {

	//val fullName: FullName = implicitly[sourcecode.FullName]


	sealed trait Company extends EnumEntry

	// NOTE: must make every object a Case because otherwise won't work when trying Trait.entryName("obj here")
	case object Company extends Enum[Company] with Company {
		val values: IndexedSeq[Company] = findValues

		case object GoldmanSachs extends Company
		case object Deloitte extends Company
		case object JPMorgan extends Company
		case object Samsung extends Company
		case object Ford extends Company
		case object Disney extends Company
		case object Apple extends Company
		case object Google extends Company
		case object Microsoft extends Company
		case object Tesla extends Company
		case object Amazon extends Company
		case object Walmart extends Company
		case object Nike extends Company
		case object Facebook extends Company
		case object Starbucks extends Company
		case object IBM extends Company
	}


	sealed trait Transaction extends EnumEntry

	case object Transaction extends Enum[Transaction] with Transaction {
		val values: IndexedSeq[Transaction] = findValues

		case object Buy extends Transaction
		case object Sell extends Transaction
	}

	sealed trait Instrument extends EnumEntry

	sealed trait FinancialInstrument extends Instrument
	sealed trait Commodity extends FinancialInstrument
	sealed trait PreciousMetal extends Commodity
	sealed trait Gemstone extends Commodity
	//sealed trait Oil extends Commodity

	sealed trait MusicalInstrument extends Instrument
	sealed trait StringInstrument extends MusicalInstrument
	sealed trait WoodwindInstrument extends MusicalInstrument
	sealed trait BassInstrument extends MusicalInstrument

	case object Instrument extends Enum[Instrument] with Instrument {
		val values: IndexedSeq[Instrument] = findValues

		case object FinancialInstrument extends Enum[FinancialInstrument] with FinancialInstrument {
			val values: IndexedSeq[FinancialInstrument] = findValues

			case object Stock extends FinancialInstrument
			case object Bond extends FinancialInstrument
			case object Option extends FinancialInstrument
			case object Derivative extends FinancialInstrument
			case object Swap extends FinancialInstrument
			case object Future extends FinancialInstrument
			case object Equity extends FinancialInstrument
			case object Share extends FinancialInstrument
			case object Cash extends FinancialInstrument

			case object Commodity extends Enum[Commodity] with Commodity {
				val values: IndexedSeq[Commodity] = findValues

				case object CrudeOil extends Commodity

				case object PreciousMetal extends Enum[PreciousMetal] with PreciousMetal {
					val values: IndexedSeq[PreciousMetal] = findValues
					case object Gold extends PreciousMetal
					case object Silver extends PreciousMetal
					case object Copper extends PreciousMetal
					case object Platinum extends PreciousMetal
				}

				case object Gemstone extends Enum[Gemstone] with Gemstone {
					val values: IndexedSeq[Gemstone] = findValues
					case object Ruby extends Gemstone
					case object Diamond extends Gemstone
					case object Emerald extends Gemstone
					case object Sapphire extends Gemstone
					case object Tourmaline extends Gemstone
					case object Moonstone extends Gemstone
					case object Citrine extends Gemstone
					case object Pearl extends Gemstone
					case object Amethyst extends Gemstone
					case object Aquamarine extends Gemstone
					case object Opal extends Gemstone
					case object Garnet extends Gemstone
					case object Beryl extends Gemstone
					case object Onyx extends Gemstone
					case object Peridot extends Gemstone
				}
			}
		}

		case object MusicalInstrument extends Enum[MusicalInstrument] with MusicalInstrument {
			val values: IndexedSeq[MusicalInstrument] = findValues

			//trait Piano extends MusicalInstrument
			case class Piano() extends MusicalInstrument
			case object Xylophone extends MusicalInstrument
			case object Voice extends MusicalInstrument

			case object BassInstrument extends Enum[BassInstrument] with BassInstrument {
				val values: IndexedSeq[BassInstrument] = findValues

				case object Trombone extends BassInstrument
				case object Trumpet extends BassInstrument
				case object Tuba extends BassInstrument
				case object FrenchHorn extends BassInstrument
				case object Saxophone extends BassInstrument
			}

			case object StringInstrument extends Enum[StringInstrument] with StringInstrument {
				val values: IndexedSeq[StringInstrument] = findValues

				case object Violin extends StringInstrument
				case object Harp extends StringInstrument
				case object Guitar extends StringInstrument
				case object Cello extends StringInstrument
			}

			case object WoodwindInstrument extends Enum[WoodwindInstrument] with WoodwindInstrument {
				val values: IndexedSeq[WoodwindInstrument] = findValues

				case object Flute extends WoodwindInstrument
				case object Oboe extends WoodwindInstrument
				case object Clarinet extends WoodwindInstrument
				case object Harmonica extends WoodwindInstrument
			}

		}
	}




	sealed trait Craft extends EnumEntry
	sealed trait Art extends Craft
	sealed trait Science extends Craft

	sealed trait NaturalScience extends Science

	sealed trait Mathematics extends Science
	sealed trait Engineering extends Mathematics

	sealed trait ArtPeriod extends Craft
	sealed trait Literature extends Craft
	sealed trait PublicationMedium extends Literature
	sealed trait Genre extends Literature


	case object Craft extends Enum[Craft] with Craft {

		val values: IndexedSeq[Craft] = findValues

		// TODO left off here MAJOR - update this to be categorized reflecting the different types of crafts (math, engineer, artist etc)

		case object Science extends Enum[Science] with Science {

			val values = findValues

			case object NaturalScience extends Enum[NaturalScience] with NaturalScience {

				val values = findValues

				case object Botany extends NaturalScience
				case object Astrology extends NaturalScience
				case object Chemistry extends NaturalScience
				case object Geology extends NaturalScience
				case object Medicine extends NaturalScience
				case object Physics extends NaturalScience
			}

			case object Mathematics extends Enum[Mathematics] with Mathematics {
				val values = findValues

				case object Statistics extends Mathematics
				case object Engineering extends Enum[Engineering] with Engineering {
					val values = findValues

					case object Architecture extends Engineering
				}
				// TODO add: mechanics, inventing, ...
			}
		}

		case object Art extends Enum[Art] with Art {
			val values = findValues

			case object Painting extends Craft
			case object Music extends Craft

			// TODO underneath - MusicalInstrument (Voice, Flute etc), MusicalPerson (Musician)
			// TODO set instruments - mix them in as traits into the People Musicians (below)
			case object Literature extends Enum[Literature] with Literature {
				val values: IndexedSeq[Literature] = findValues

				case object PublicationMedium extends Enum[PublicationMedium] with PublicationMedium {
					val values: IndexedSeq[PublicationMedium] = findValues

					case object Poetry extends PublicationMedium
					case object Play extends PublicationMedium
					case object Drama extends PublicationMedium
					case object Epic extends PublicationMedium

					case object Ballad extends PublicationMedium

					case object Essay extends PublicationMedium

					case object Letter extends Literature

					case object Novel extends PublicationMedium

					case object ShortStory extends PublicationMedium

					case object Prose extends PublicationMedium

					case object Autobiography extends PublicationMedium
				}

				case object Genre extends Enum[Genre] with Genre {
					val values: IndexedSeq[Genre] = findValues

					case object Fable extends Genre
					case object Mystery extends Genre
					case object Horror extends Genre
					case object Satire extends Genre
					case object Morbidity extends Genre
					case object Criticism extends Genre
					case object Comedy extends Genre
					case object Fiction extends Genre
					case object HistoricalFiction extends Genre
					case object History extends Genre
					case object Nonfiction extends Genre
					case object Mythology extends Genre
					case object Religion extends Genre
					case object Morality extends Genre
				}
			}

			// TODO list eras - Romanticisim, ... (Jane Austen)
			// TODO lsit types of literature - Poetry, autobiography
			case object Sculpture extends Craft
			case object Theatre extends Craft // actor
			case object Cinema extends Craft // dance,s ing
		}
	}

	case object ArtPeriod extends Enum[ArtPeriod] with ArtPeriod {
		val values: IndexedSeq[ArtPeriod] = findValues

		case object Romanticism extends ArtPeriod
		case object DarkRomanticism extends ArtPeriod
		case object Gothic extends ArtPeriod
		case object OldEnglish extends ArtPeriod
		case object MiddleEnglish extends ArtPeriod

		case object Edwardian extends ArtPeriod

		case object Victorian extends ArtPeriod

		case object Medieval extends ArtPeriod

		case object Modernism extends ArtPeriod

		case object Renaissance extends ArtPeriod
	}


	//val a1: Artist = Art.Painting.Painter

	sealed trait Human extends EnumEntry
	sealed trait Craftsman extends Human with Craft
	sealed trait Scientist extends Craftsman // math, engineer, botany, chemistry, geology, doctor, physics, computer science

	sealed trait Mathematician extends Scientist
	sealed trait Statistician extends Mathematician
	sealed trait Engineer extends Mathematician
	sealed trait ComputerScientist extends Engineer
	sealed trait Architect extends Engineer

	sealed trait NaturalScientist extends Scientist
	sealed trait Botanist extends NaturalScientist
	sealed trait Astrologist extends NaturalScientist
	sealed trait Chemist extends NaturalScientist
	sealed trait Geologist extends NaturalScientist
	sealed trait Doctor extends NaturalScientist
	sealed trait Physicist extends NaturalScientist



	case object Scientist extends Enum[Scientist] with Scientist {
		val values: IndexedSeq[Scientist] = findValues

		case object NaturalScientist extends Enum[NaturalScientist] with NaturalScientist {
			val values: IndexedSeq[NaturalScientist] = findValues


			case object Botanist extends Enum[Botanist] with Botanist {
				val values: IndexedSeq[Botanist] = findValues
				// TODO here
			}

			case object Physicist extends Enum[Physicist] with Physicist {
				val values: IndexedSeq[Physicist] = findValues

				case class AlbertEinstein() extends Human.AlbertEinstein
			}

			case object Chemist extends Enum[Chemist] with Chemist {
				val values: IndexedSeq[Chemist] = findValues
				// TODO here
			}

			case object Astrologist extends Enum[Astrologist] with Astrologist {
				val values: IndexedSeq[Astrologist] = findValues
				// TODO here
			}

			case object Geologist extends Enum[Geologist] with Geologist {
				val values: IndexedSeq[Geologist] = findValues
				// TODO here
			}

			case object Doctor extends Enum[Doctor] with Doctor {
				val values: IndexedSeq[Doctor] = findValues
				// TODO here
			}
		}

		// ------------------------------------------------------------

		case object Mathematician extends Enum[Mathematician] with Mathematician {
			val values: IndexedSeq[Mathematician] = findValues

			case class LeonardoDaVinci() extends Human.LeonardoDaVinci
			case class AlbertEinstein() extends Human.AlbertEinstein


			case object Statistician extends Enum[Statistician] with Statistician {
				val values: IndexedSeq[Statistician] = findValues


			}

			case object Engineer extends Enum[Engineer] with Engineer {
				val values = findValues

				// TODO fill up

				case object Architect extends Enum[Architect] with Architect {
					val values: IndexedSeq[Architect] = findValues

					case class AntoniGaudi() extends Human.AntoniGaudi
					case class WilliamPereira() extends Human.WilliamPereira
				}

				case object ComputerScientist extends Enum[ComputerScientist] with ComputerScientist {
					val values: IndexedSeq[ComputerScientist] = findValues

					// TODO fill here
				}
			}


		}
	}


	// ----------

	sealed trait Artist extends Craftsman

	sealed trait Painter extends /*EnumEntry with*/ Artist
	sealed trait Sculptor extends Artist

	sealed trait Musician extends Artist
	sealed trait Dancer extends Artist
	sealed trait Singer extends Artist
	sealed trait Actor extends Artist
	sealed trait Designer extends Artist
	sealed trait Inventor extends Artist // TODO inventor should be underneath Scientist not Artist - must change the Datahub dataset
	sealed trait Producer extends Artist
	sealed trait Director extends Artist

	sealed trait Writer extends Artist
	sealed trait Linguist extends Artist

	//sealed trait Architect extends Artist




	case object Artist extends Enum[Artist] with Artist {

		val values: IndexedSeq[Artist] = findValues

		case object Painter extends Enum[Painter] with Painter {
			val values: IndexedSeq[Painter] = findValues
			/*val values: IndexedSeq[Painter] = Human.values.map((h: Human) => h match {
			    case p: Painter => Some(p.asInstanceOf[Painter])
			    case _ => None
			}).filter(_.isDefined).map(_.get)*/
			case class VanGogh() extends Human.VanGogh
			case class LeonardoDaVinci() extends Human.LeonardoDaVinci
			case class Michelangelo() extends Human.Michelangelo
			case class ClaudeMonet() extends Human.ClaudeMonet
			case class Rembrandt() extends Human.Rembrandt
			case class ElGreco() extends Human.ElGreco
		}

		case object Sculptor extends Enum[Sculptor] with Sculptor {
			val values: IndexedSeq[Sculptor] = findValues

			case class LeonardoDaVinci() extends Human.LeonardoDaVinci
			case class Michelangelo() extends Human.Michelangelo
			case class ConstantinBrancusi() extends Human.ConstantinBracusi
			case class AugusteRodin() extends Human.AugusteRodin
		}

		case object Musician extends Enum[Musician] with Musician {
			val values: IndexedSeq[Musician] = findValues
			// TODO to separate these out using "Violinits, Singer, Dancer, Flutist, Tubaist...? attributing the instrument to the person, like above - art field to the person? Or can just leave mixin-trait
			// Saxophone
			case class JohnColtrane() extends Human.JohnColtrane
			case class PaulDesmond() extends Human.PaulDesmond
			case class SonnyStitt() extends Human.SonnyStitt
			// Trombone
			case class JackTeagarden() extends Human.JackTeagarden
			case class FredWesley() extends Human.FredWesley
			// Tuba
			case class MinLeiBrook() extends Human.MinLeiBrook
			case class WalterEnglish() extends Human.WalterEnglish
			case class SquireGersh() extends Human.SquireGersh
			// Flute
			case class HerbieMann() extends Human.HerbieMann
			case class TheobaldBoehm() extends Human.TheobaldBoehm
			case class YusefLateef() extends Human.YusefLateef
			// Trumpet, voice
			case class LouisArmstrong() extends Human.LouisArmstrong
			// Piano, voice
			case class JellyRollMorton() extends Human.JellyRollMorton
			// Violin
			case class NiccoloPaganini() extends Human.NiccoloPaganini
			case class ViktoriaMullova() extends Human.ViktoriaMullova
			case class GeorgeEnescu() extends Human.GeorgeEnescu
		}

		case object Actor extends Enum[Actor] with Actor { // theatre, cinema
			val values: IndexedSeq[Actor] = findValues

			case class KeiraKnightley() extends Human.KeiraKnightley
			case class OrlandoBloom() extends Human.OrlandoBloom
			case class SarahBlueRichards() extends Human.SarahBlueRichards
			case class FreddieHighmore() extends Human.FreddieHighmore
		}

		case object Dancer extends Enum[Dancer] with Dancer { // theatre
			val values: IndexedSeq[Dancer] = findValues

			case class AnnaPavlova() extends Human.AnnaPavlova
			case class RudolfNureyev() extends Human.RudolfNureyev
			case class MayaAngelou() extends Human.MayaAngelou
		}

		case object Singer extends Enum[Singer] with Singer { // theatre
			val values: IndexedSeq[Singer] = findValues

			// Voice
			case class SarahBrightman() extends Human.SarahBrightman
			case class Adele() extends Human.Adele
			case class Beyonce() extends Human.Beyonce
			case class PhilCollins() extends Human.PhilCollins
			case class RodStewart() extends Human.RodStewart
			case class AlannisMorrisette() extends Human.AlannisMorrisette
		}

		case object Designer extends Enum[Designer] with Designer {
			val values: IndexedSeq[Designer] = findValues

			case class LeonardoDaVinci() extends Human.LeonardoDaVinci
		}

		case object Inventor extends Enum[Inventor] with Inventor {
			val values = findValues

			case class LeonardoDaVinci() extends Human.LeonardoDaVinci
		}

		case object Director extends Enum[Director] with Director {
			val values = findValues

			case class MayaAngelou() extends Human.MayaAngelou
		}

		case object Producer extends Enum[Producer] with Producer {
			val values = findValues

			case class MayaAngelou() extends Human.MayaAngelou
		}



		case object Writer extends Enum[Writer] with Writer {
			val values: IndexedSeq[Writer] = findValues

			case class MayaAngelou() extends Human.MayaAngelou
			case class CharlotteBronte() extends Human.CharlotteBronte
			case class SamuelTaylorColeridge() extends Human.SamuelTaylorColeridge
			case class WilliamWordsworth() extends Human.WilliamWordsworth
			case class RalphWaldoEmerson() extends Human.RalphWaldoEmerson
			case class NathanielHawthorne() extends Human.NathanielHawthorne
			case class HenryDavidThoreau() extends Human.HenryDavidThoreau
			case class JohnKeats() extends Human.JohnKeats
			case class VictorHugo() extends Human.VictorHugo
			case class PercyByssheShelley() extends Human.PercyByssheShelley
			case class AlfredLordTennyson() extends Human.AlfredLordTennyson
			case class LordByron() extends Human.LordByron
			case class WilliamShakespeare() extends Human.WilliamShakespeare
			case class CharlesDickens ()extends Human.CharlesDickens
			case class EmilyDickenson() extends Human.EmilyDickinson
			case class JaneAusten() extends Human.JaneAusten
			case class JulesVerne() extends Human.JulesVerne
			case class EdgarAllanPoe() extends Human.EdgarAllanPoe
			case class TSEliot() extends Human.TSEliot
			case class HansChristianAnderson() extends Human.HansChristianAnderson
			case class MarkTwain() extends Human.MarkTwain
			case class JamesJoyce() extends Human.JamesJoyce
			case class LeoTolstoy() extends Human.LeoTolstoy
			case class FScottFitzgerald() extends Human.FScottFitzgerald
			case class GeorgeOrwell() extends Human.GeorgeOrwell
			case class HermanMelville() extends Human.HermanMelville
			case class RoaldDahl() extends Human.RoaldDahl
			case class AlexandreDumas() extends Human.AlexandreDumas
			case class AgathaChristie () extends Human.AgathaChristie
			case class ErnestHemingway() extends Human.ErnestHemingway
			case class HermanHesse() extends Human.HermanHesse
			case class AntonChekhov() extends Human.AntonChekhov
			case class AlexanderPushkin() extends Human.AlexanderPushkin
			case class GeoffreyChaucer() extends Human.GeoffreyChaucer
		}

		case object Linguist extends Enum[Linguist] with Linguist {
			val values: IndexedSeq[Linguist] = findValues

			case class MayaAngelou() extends Human.MayaAngelou
			case class LeonardoDaVinci() extends Human.LeonardoDaVinci
		}


	}

	case object Human extends Enum[Human] with Human {
		val values: IndexedSeq[Human] = findValues

		/**
		 * MATHS
		 */
		trait AlbertEinstein extends Physicist with Mathematician // TODO more?
		case object AlbertEinstein extends AlbertEinstein

		trait AntoniGaudi extends Architect
		case object AntoniGaudi extends AntoniGaudi

		trait WilliamPereira extends Architect
		case object WilliamPereira extends WilliamPereira

		// -------------------------------------------------

		/**
		 * ARTS
		 */

		trait VanGogh extends Painter
		case object VanGogh extends VanGogh // NOTE weird cannot use case class or else complains that VanGogh is already defined in scope.

		// TODO put these people under each respective field they have here
		trait LeonardoDaVinci extends Painter with Sculptor with Writer with Musician with Mathematician with Architect with Engineer with Geologist with Botanist
		case object LeonardoDaVinci extends LeonardoDaVinci

		trait Michelangelo extends Painter with Sculptor with Architect with Writer
		case object Michelangelo extends Michelangelo

		trait ClaudeMonet extends Painter
		case object ClaudeMonet extends ClaudeMonet

		trait Rembrandt extends Painter
		case object Rembrandt extends Rembrandt

		trait ElGreco extends Painter
		case object ElGreco extends ElGreco

		trait ConstantinBracusi extends Sculptor
		case object ConstantinBracusi extends ConstantinBracusi

		trait AugusteRodin extends Sculptor
		case object AugusteRodin extends AugusteRodin

		//-------------------------------
		// Voice
		trait SarahBrightman extends Musician with Singer
		case object SarahBrightman extends SarahBrightman

		trait Adele extends Musician with Singer
		case object Adele extends Adele

		trait Beyonce extends Musician with Singer
		case object Beyonce extends Beyonce

		trait PhilCollins extends Musician with Singer
		case object PhilCollins extends PhilCollins

		trait RodStewart extends Musician with Singer
		case object RodStewart extends RodStewart

		trait AlannisMorrisette extends Musician with Singer
		case object AlannisMorrisette extends AlannisMorrisette

		// Saxophone
		trait JohnColtrane extends Musician
		case object JohnColtrane extends JohnColtrane

		trait PaulDesmond extends Musician
		case object PaulDesmond extends PaulDesmond

		trait SonnyStitt extends Musician
		case object SonnyStitt extends SonnyStitt

		// Trombone

		trait JackTeagarden extends Musician
		case object JackTeagarden extends JackTeagarden

		trait FredWesley extends Musician
		case object FredWesley extends FredWesley

		// Tuba
		trait MinLeiBrook extends Musician
		case object MinLeiBrook extends MinLeiBrook

		trait WalterEnglish extends Musician
		case object WalterEnglish extends WalterEnglish

		trait SquireGersh extends Musician
		case object SquireGersh extends SquireGersh

		// Flute
		trait HerbieMann extends Musician
		case object HerbieMann extends HerbieMann

		trait TheobaldBoehm extends Musician
		case object TheobaldBoehm extends TheobaldBoehm

		trait YusefLateef extends Musician
		case object YusefLateef extends YusefLateef

		// Trumpet, voice
		trait LouisArmstrong extends Musician
		case object LouisArmstrong extends LouisArmstrong

		// Piano, voice
		trait JellyRollMorton extends /*Pianist with */ Musician
		case object JellyRollMorton extends JellyRollMorton

		// Violin
		trait NiccoloPaganini extends Musician
		case object NiccoloPaganini extends NiccoloPaganini

		trait ViktoriaMullova extends Musician
		case object ViktoriaMullova extends ViktoriaMullova

		trait GeorgeEnescu extends Musician
		case object GeorgeEnescu extends GeorgeEnescu

		// ---------------

		trait MayaAngelou extends Writer with Producer with Director with Dancer with Linguist with Actor
		case object MayaAngelou extends MayaAngelou

		trait HenryDavidThoreau extends Writer
		case object HenryDavidThoreau extends HenryDavidThoreau

		trait CharlotteBronte extends Writer
		case object CharlotteBronte extends CharlotteBronte

		trait SamuelTaylorColeridge extends Writer
		case object SamuelTaylorColeridge extends SamuelTaylorColeridge

		trait RalphWaldoEmerson extends Writer
		case object RalphWaldoEmerson extends RalphWaldoEmerson

		trait NathanielHawthorne extends Writer
		case object NathanielHawthorne extends NathanielHawthorne

		trait WilliamWordsworth extends Writer
		case object WilliamWordsworth extends WilliamWordsworth

		trait JohnKeats extends Writer
		case object JohnKeats extends JohnKeats

		trait VictorHugo extends Writer
		case object VictorHugo extends VictorHugo

		trait PercyByssheShelley extends Writer
		case object PercyByssheShelley extends PercyByssheShelley

		trait AlfredLordTennyson extends Writer
		case object AlfredLordTennyson extends AlfredLordTennyson

		trait LordByron extends Writer
		case object LordByron extends LordByron

		trait WilliamShakespeare extends Writer
		case object WilliamShakespeare extends WilliamShakespeare

		trait CharlesDickens extends Writer
		case object CharlesDickens extends CharlesDickens

		trait EmilyDickinson extends Writer
		case object EmilyDickinson extends EmilyDickinson

		trait JaneAusten extends Writer
		case object JaneAusten extends JaneAusten

		trait JulesVerne extends Writer
		case object JulesVerne extends JulesVerne

		trait EdgarAllanPoe extends Writer
		case object EdgarAllanPoe extends EdgarAllanPoe

		trait TSEliot extends Writer
		case object TSEliot extends TSEliot

		trait HansChristianAnderson extends Writer
		case object HansChristianAnderson extends HansChristianAnderson

		trait MarkTwain extends Writer
		case object MarkTwain extends MarkTwain

		trait JamesJoyce extends Writer
		case object JamesJoyce extends JamesJoyce

		trait LeoTolstoy extends Writer
		case object LeoTolstoy extends LeoTolstoy

		trait FScottFitzgerald extends Writer
		case object FScottFitzgerald extends FScottFitzgerald

		trait GeorgeOrwell extends Writer
		case object GeorgeOrwell extends GeorgeOrwell

		trait HermanMelville extends Writer
		case object HermanMelville extends HermanMelville

		trait RoaldDahl extends Writer
		case object RoaldDahl extends RoaldDahl

		trait AlexandreDumas extends Writer
		case object AlexandreDumas extends AlexandreDumas

		trait AgathaChristie extends Writer
		case object AgathaChristie extends AgathaChristie

		trait ErnestHemingway extends Writer
		case object ErnestHemingway extends ErnestHemingway

		trait HermanHesse extends Writer
		case object HermanHesse extends HermanHesse

		trait AntonChekhov extends Writer
		case object AntonChekhov extends AntonChekhov

		trait AlexanderPushkin extends Writer
		case object AlexanderPushkin extends AlexanderPushkin

		trait GeoffreyChaucer extends Writer
		case object GeoffreyChaucer extends GeoffreyChaucer


		// ------------------------------------------------
		trait KeiraKnightley extends Actor
		case object KeiraKnightley extends KeiraKnightley

		trait OrlandoBloom extends Actor
		case object OrlandoBloom extends OrlandoBloom

		trait SarahBlueRichards extends Actor
		case object SarahBlueRichards extends SarahBlueRichards

		trait FreddieHighmore extends Actor
		case object FreddieHighmore extends FreddieHighmore

		// ------------------------------------------------
		trait AnnaPavlova extends Dancer
		case object AnnaPavlova extends AnnaPavlova

		trait RudolfNureyev extends Dancer
		case object RudolfNureyev extends RudolfNureyev
	}


	val v: Painter = Human.LeonardoDaVinci
	val v0: Human  = Human.VanGogh
	val v1: Painter = Human.VanGogh
	val v11: Human.VanGogh = Human.VanGogh
	val v2: Artist = Artist.Painter.VanGogh()
	val v3: Painter = Artist.Painter.VanGogh()
	val v4: Sculptor = Artist.Sculptor.LeonardoDaVinci()
	val vv: Sculptor = Artist.Sculptor.LeonardoDaVinci()
	val v5: Painter = Artist.Painter.LeonardoDaVinci()
	val v6: Sculptor = Artist.Painter.LeonardoDaVinci()




	sealed trait Tree extends EnumEntry

	case object Tree extends Enum[Tree] with Tree {
		val values: IndexedSeq[Tree] = findValues

		case object Oak extends Tree
		case object Hazel extends Tree
		case object Holly extends Tree
		case object Elder extends Tree
		case object Dogwood extends Tree
		case object Juniper extends Tree
		case object Yew extends Tree
		case object Alder extends Tree
		case object Willow extends Tree
		case object Hemlock extends Tree
		case object Pine extends Tree
		case object Birch extends Tree
		case object Aspen extends Tree
		case object Poplar extends Tree
		case object Spruce extends Tree
		case object Cedar extends Tree
		case object Fir extends Tree
		case object Tuia extends Tree
		case object Beech extends Tree
		case object Maple extends Tree
		case object Ash extends Tree
		case object Sycamore extends Tree
		case object Magnolia extends Tree
		case object Laurel extends Tree
		case object Olive extends Tree
		case object BlackLocust extends Tree
		case object Sequoia extends Tree
		case object Redwood extends Tree
		case object Palm extends Tree
		case object Elm extends Tree
		case object Hickory extends Tree
		case object Larch extends Tree
		case object Hornbeam extends Tree
		case object Hawthorn extends Tree
		case object Rowan extends Tree


		// Nut
		case object Walnut extends Tree
		case object Chestnut extends Tree
		case object Butternut extends Tree
		// Fruit
		case object AppleTree extends Tree
		case object CherryTree extends Tree
		case object FigTree extends Tree

		// China
		case object Goldenrain extends Tree
		case object MoneyTree extends Tree
		case object JapaneseMaple extends Tree
		case object Gingko extends Tree
		case object Maidenhair extends Tree
		// Africa
		case object Acacia extends Tree
		case object Baobab extends Tree
		case object QuiverTree extends Tree
		case object Yellowthorn extends Tree
		case object Buffalothorn extends Tree
		case object Digitata extends Tree
		case object Bushwillow extends Tree
	}

	sealed trait Flower extends EnumEntry
	case object Flower extends Enum[Flower] with Flower {
		val values: IndexedSeq[Flower] = findValues

		case object Rose extends Flower
		case object Daffodil extends Flower
		case object Lily extends Flower
		case object Daisy extends Flower
		case object Margaret extends Flower
		case object Violet extends Flower
		case object Hydrangea extends Flower
		case object Peony extends Flower
		case object Sunflower extends Flower
		case object Iris extends Flower
		case object Lavender extends Flower
		case object Dahlia extends Flower
		case object Azalea extends Flower
		case object Begonia extends Flower
		case object Marigold extends Flower
		case object LilyOfTheValley extends Flower
		case object ForgetMeNot extends Flower
		case object Chamomile extends Flower
		case object Clover extends Flower
		case object Tulip extends Flower
		case object Aster extends Flower
		case object Carnation extends Flower
		case object Chrysanthemum extends Flower
		case object Snapdragon extends Flower
	}



	sealed trait Animal extends EnumEntry

	sealed trait Reptile extends Animal
	sealed trait Lizard extends Reptile
	sealed trait Amphibian extends Animal
	sealed trait Frog extends Amphibian

	sealed trait Insect extends Animal

	sealed trait Cat extends Animal
	sealed trait HouseCat extends Cat
	sealed trait WildCat extends Cat

	sealed trait Canine extends Animal
	sealed trait HouseDog extends Canine
	sealed trait WildCanine extends Canine
	sealed trait Fox extends WildCanine

	sealed trait WeaselMustelid extends Animal

	sealed trait Rodent extends Animal
	sealed trait Squirrel extends Rodent

	sealed trait Monkey extends Animal
	sealed trait Ape extends Monkey

	sealed trait Bear extends Animal

	sealed trait Deer extends Animal

	sealed trait Bird extends Animal
	sealed trait Eagle extends Bird

	sealed trait SeaCreature extends Animal
	sealed trait Whale extends SeaCreature

	sealed trait Equine extends Animal
	sealed trait Horse extends Equine

	sealed trait Camelid extends Animal

	case object Animal extends Enum[Animal] with Animal {
		val values: IndexedSeq[Animal] = findValues //AnimalEnum.values.toIndexedSeq.map(_.asInstanceOf[Animal])

		case object Giraffe extends Animal
		case object Hippo extends Animal
		case object Elephant extends Animal

		case object Rabbit extends Animal

		case object Equine extends Enum[Equine] with Equine {

			val values: IndexedSeq[Equine] = findValues

			case object Zebra extends Equine
			case object Donkey extends Equine

			case object Horse extends Enum[Horse] with Horse {
				val values: IndexedSeq[Horse] = findValues

				case object ArabianHorse extends Horse
				case object FriesianHorse extends Horse
				case object Mustang extends Horse
				case object Thoroughbred extends Horse
				case object PaintHorse extends Horse
				case object QuarterHorse extends Horse
				case object Appaloosa extends Horse
				case object Percheron extends Horse
				case object Clydesdale extends Horse
				case object TurkomanHorse extends Horse
				case object FjordHorse extends Horse
				case object MarchardorHorse extends Horse
				case object HaflingerHorse extends Horse
				case object BretonHorse extends Horse
				case object ShetlandPony extends Horse
				case object CriolloHorse extends Horse
				case object DutchWarmbloodHorse extends Horse
				case object AndalusianHorse extends Horse
				case object HanoverianHorse extends Horse
				case object LipizanHorse extends Horse
				case object FalabellaHorse extends Horse
			}
		}

		case object Bear extends Enum[Bear] with Bear {
			val values: IndexedSeq[Bear] = findValues

			case object BlackBear extends Bear
			case object GrizzlyBear extends Bear
			case object Koala extends Bear
			case object Panda extends Bear
			case object BrownBear extends Bear
			case object PolarBear extends Bear
		}


		case object Bison extends Animal

		case object Deer extends Enum[Deer] with Deer {
			val values: IndexedSeq[Deer] = findValues

			case object Reindeer extends Deer
			case object Elk extends Deer
			case object Moose extends Deer
			case object WhiteTailed extends Deer
			case object RedDeer extends Deer
			case object SiberianRoe extends Deer
			case object RoeDeer extends Deer
			case object Caribou extends Deer
			case object PersianDeer extends Deer
			case object Antelope extends Deer
			case object Gazelle extends Deer

		}




		case object Monkey extends Enum[Monkey] with Monkey {

			val values: IndexedSeq[Monkey] = findValues


			case object Tamarin extends Monkey
			case object Capuchin extends Monkey
			case object Marmoset extends Monkey
			case object Macaque extends Monkey
			case object Lemur extends Monkey
			case object Howler extends Monkey
			case object Bushbaby

			case object Ape extends Enum[Ape] with Ape {
				val values: IndexedSeq[Ape] = findValues

				case object Orangutan extends Ape
				case object Gorilla extends Ape
				case object Chimpanzee extends Ape
				case object Gibbon extends Ape
				case object Bonobo extends Ape
			}
		}


		case object WeaselMustelid extends Enum[WeaselMustelid] with WeaselMustelid {
			val values: IndexedSeq[WeaselMustelid] = findValues

			case object Badger extends WeaselMustelid
			case object Otter extends WeaselMustelid
			case object Ferret extends WeaselMustelid
			case object Wolverine extends WeaselMustelid
			case object Mink extends WeaselMustelid
			case object Marten extends WeaselMustelid
			case object Weasel extends WeaselMustelid
		}

		case object Mongoose extends Animal

		case object Camelid extends Enum[Camelid] with Camelid {
			val values = findValues

			case object Camel extends Camelid
			case object Alpaca extends Camelid
			case object Llama extends Camelid
		}

		case object Rodent extends Enum[Rodent] with Rodent {

			val values: IndexedSeq[Rodent] = findValues

			case object Mouse extends Rodent
			case object Rat extends Rodent
			case object Beaver extends Rodent
			case object Procupine extends Rodent
			case object Chinchilla extends Rodent

			case object Squirrel extends Enum[Squirrel] with Squirrel {
				val values: IndexedSeq[Squirrel] = findValues

				case object Marmot extends Squirrel
				case object Groundhog extends Squirrel
				case object RedSquirrel extends Squirrel
				case object BrownSquirrel extends Squirrel
				case object Chipmunk extends Squirrel
			}
		}

		case object Insect extends Enum[Insect] with Insect {
			val values: IndexedSeq[Insect] = findValues

			case object Termite extends Insect
			case object Spider extends Insect
			case object Caterpillar extends Insect
			case object Butterfly extends Insect
			case object Bee extends Insect
			case object Ladybug extends Insect
			case object Fly extends Insect
			case object Beetle extends Insect
			case object Scorpion extends Insect
			case object Dragonfly extends Insect
			case object Wasp extends Insect
			case object Cricket extends Insect
			case object Centipede extends Insect
		}

		case object Reptile extends Enum[Reptile] with Reptile {
			val values: IndexedSeq[Reptile] = findValues

			case object Turtle extends Reptile
			case object Tortoise extends Reptile
			case object Crocodile extends Animal
			case object Snake extends Animal

			case object Lizard extends Enum[Lizard] with Lizard {
				val values = findValues

				case object GilaMonster extends Lizard
				case object Snapdragon extends Lizard
				case object Iguana extends Lizard
			}
		}

		case object Amphibian extends Enum[Amphibian] with Amphibian {
			val values: IndexedSeq[Amphibian] = findValues

			case object Frog extends Enum[Frog] with Frog {
				val values = findValues

				case object TrueFrog extends Frog
				case object GlassFrog extends Frog
				case object CommonReedFrog extends Frog
				case object BlueFrog extends Frog
				case object RedFrog extends Frog
				case object Bullfrog extends Frog
				case object PoisonDartFrog extends Frog
				case object GoldenPoisonFrog extends Frog
			}
			case object Newt extends Amphibian
			case object Salamander extends Amphibian
			case object Toad extends Amphibian
		}



		case object Canine extends Enum[Canine] with Canine {
			val values: IndexedSeq[Canine] = findValues

			case object HouseDog extends Enum[HouseDog] with HouseDog {
				val values: IndexedSeq[HouseDog] = findValues

				case object GoldenRetriever extends HouseDog
				case object Poodle extends HouseDog
				case object Labrador extends HouseDog
				case object Pomeranian extends HouseDog
			}

			case object WildCanine extends Enum[WildCanine] with WildCanine {
				val values: IndexedSeq[WildCanine] = findValues

				case object PrairieDog extends WildCanine
				case object Wolf extends WildCanine
				case object Jackal extends WildCanine
				case object Coyote extends WildCanine
				case object Hyena extends WildCanine

				case object Fox extends Enum[Fox] with Fox {
					val values: IndexedSeq[Fox] = findValues

					case object FennecFox extends Fox
					case object ArcticFox extends Fox
					case object RedFox extends Fox
					case object GreyFox extends Fox
					case object TibetanFox extends Fox
				}
			}
		}

		case object Cat extends Enum[Cat] with Cat {
			val values: IndexedSeq[Cat] = findValues

			case object WildCat extends Enum[WildCat] with WildCat {
				val values: IndexedSeq[WildCat] = findValues

				case object Cougar extends WildCat
				case object Puma extends WildCat
				case object MountainLion extends WildCat
				case object Lynx extends WildCat
				case object IberianLynx extends WildCat
				case object Bobcat extends WildCat
				case object Lion extends WildCat
				case object Cheetah extends WildCat
				case object Tiger extends WildCat
				case object Panther extends WildCat
				case object Leopard extends WildCat
				case object Jaguar extends WildCat
				case object Ocelot extends WildCat
				case object Caracal extends WildCat
				case object SandCat extends WildCat
			}

			case object DomesticCat extends Enum[HouseCat] with HouseCat {
				val values: IndexedSeq[HouseCat] = findValues

				case object TabbyCat extends HouseCat
				case object PersianCat extends HouseCat
				case object ShorthairedCat extends HouseCat
				case object SiameseCat extends HouseCat
				case object BirmanCat extends HouseCat
				case object RagdollCat extends HouseCat
				case object SphynxCat extends HouseCat
				case object ScottishFoldCat extends HouseCat
				case object AmericanBobtailCat extends HouseCat
				case object TonkineseCat extends HouseCat
			}
		}

		case object SeaCreature extends Enum[SeaCreature] with SeaCreature {
			val values: IndexedSeq[SeaCreature] = findValues

			case object Anemone extends SeaCreature
			case object Coral extends SeaCreature
			case object Pearl extends SeaCreature
			case object Oyster extends SeaCreature
			case object Clam extends SeaCreature
			case object Seahorse extends SeaCreature
			case object Lobster extends SeaCreature
			case object Crab extends SeaCreature
			case object Shrimp extends SeaCreature
			case object Flounder extends SeaCreature
			case object AngelFish extends SeaCreature
			case object Marlin extends SeaCreature
			case object Clownfish extends SeaCreature
			case object Dolphin extends SeaCreature
			case object Shark extends SeaCreature

			case object Whale extends Enum[Whale] with Whale {
				val values = findValues
				case object BelugaWhale extends Whale
				case object BlueWhale extends Whale
				case object Orca extends Whale
			}
			case object Stingray extends SeaCreature
			case object Octopus extends SeaCreature
			case object Squid extends SeaCreature
			case object Jellyfish extends SeaCreature
			case object Leviathan extends SeaCreature

		}

		case object Bird extends Enum[Bird] with Bird {
			val values: IndexedSeq[Bird] = findValues

			case object Owl extends Bird
			case object Duck extends Bird
			case object Goose extends Bird
			case object Pelican extends Bird
			case object Flamingo extends Bird
			case object Albatross extends Bird
			case object Vulture extends Bird
			case object Hawk extends Bird
			case object Falcon extends Bird
			case object Goldfinch extends Bird
			case object Starling extends Bird
			case object Nightingale extends Bird
			case object Swallow extends Bird
			case object Magpie extends Bird
			case object Canary extends Bird
			case object Parrot extends Bird
			case object Sparrow extends Bird
			case object Robin extends Bird
			case object Swan extends Bird
			case object Woodpecker extends Bird
			case object Chickadee extends Bird
			case object Raven extends Bird
			case object Crow extends Bird
			case object Bluejay extends Bird
			case object Mockingbird extends Bird
			case object Penguin extends Bird
			case object Ostrich extends Bird
			case object Emu extends Bird
			case object Cockatiel extends Bird
			case object Macaw extends Bird
			case object Toucan extends Bird

			case object Eagle extends Enum[Eagle] with Eagle {
				val values: IndexedSeq[Eagle] = findValues

				case object BaldEagle extends Eagle
				case object GoldenEagle extends Eagle
			}
		}
	}

	//val p1: Animal = Animal.cat.houseCat.PersianCat
	//Animal.Bird.Eagle.GoldenEagle
	//Animal.Bird.Canary



	sealed trait Biome extends EnumEntry

	sealed trait Grassland extends Biome

	sealed trait Forest extends Biome

	sealed trait Marine extends Biome
	sealed trait Freshwater extends Marine
	sealed trait Saltwater extends Marine

	sealed trait Tundra extends Biome

	case object Biome extends Enum[Biome] with Biome {
		val values: IndexedSeq[Biome] = findValues

		case object Forest extends Enum[Forest] with Forest {
			val values: IndexedSeq[Forest] = findValues

			case object ConiferousForest extends Forest
			case object DeciduousForest extends Forest
			case object TaigaBorealForest extends Forest
			case object Rainforest extends Forest
		}

		case object Grassland extends Enum[Grassland] with Grassland {
			val values = findValues

			case object Prairie extends Grassland // temperate
			case object Steppes extends Grassland // temperate
			case object Savannah extends Grassland // tropical
			case object Shrubland extends Grassland // temperate
		}

		case object Desert extends Biome


		// TODO erase WaterType enum and replace usages with marine biome
		case object Marine extends Enum[Marine] with Marine {

			val values = findValues

			case object Freshwater extends Enum[Freshwater] with Freshwater {
				val values = findValues

				case object Lake extends Freshwater
				case object Pond extends Freshwater
				case object River extends Freshwater
				case object Stream extends Freshwater
				case object Wetland extends Freshwater
			}
			case object Saltwater extends Enum[Saltwater] with Saltwater {
				val values = findValues

				case object Ocean extends Saltwater
				case object Seashore extends Saltwater
			}
		}


		case object Tundra extends Enum[Tundra] with Tundra {
			val values = findValues

			case object ArcticTundra extends Tundra
			case object AntarcticTundra extends Tundra
			case object AlpineTundra extends Tundra
		}
	}




	sealed trait ClimateZone extends EnumEntry

	case object ClimateZone extends Enum[ClimateZone] with ClimateZone {
		val values: IndexedSeq[ClimateZone] = findValues

		case object Temperate extends ClimateZone
		case object Arid extends ClimateZone
		case object Dry extends ClimateZone
		case object Desert extends ClimateZone
		case object Tundra extends ClimateZone
		case object Arctic extends ClimateZone
		case object Tropical extends ClimateZone
		//case object Rainforest extends Climate
		case object Humid extends ClimateZone
		case object Mediterranean extends ClimateZone
		case object Continental extends ClimateZone
		case object MountainHighland extends ClimateZone
	}

	// TODO - plant - tree,flower - associated with climate/country
	// EXAMPLE:
	// Plant.Tree.Oak -- Climate.Temperate -- Country.Canada
	// Plant.Tree.Birch, Climate.Continental, Country.Russia
	// Plant.Flower.Cactus, Climate.Desert, Country.Africa
	// Plant.Flower.Aloe, Climate.Desert, Country.Arabia
	// Trees - Oak, Birch, Maple, Chestnut, Fir - SilverFir, Tuia..
	// Flower - Peony, Rose, Daisy, Marigold, ForgetMeNot...


	// SOURCE: https://worldpopulationreview.com/country-rankings/list-of-countries-by-continent
	sealed trait World extends EnumEntry


	trait Province
	trait Territory
	trait State
	trait District
	trait Continent
	trait Island
	trait Country
	trait Peninsula
	trait PersianGulf
	trait City
	trait Town
	trait Parish
	trait CivilParish extends Parish
	trait Village
	trait County
	trait Gate
	trait Gulf
	trait Region

	sealed trait Africa extends World with Continent
	sealed trait NorthAmerica extends World with Continent
	sealed trait SouthAmerica extends World with Continent
	sealed trait Asia extends World with Continent
	sealed trait Europe extends World with Continent
	sealed trait Oceania extends World with Continent

	sealed trait CentralAmerica extends World  // NOT a continent

	sealed trait Canada extends NorthAmerica with Country
	sealed trait UnitedStates extends NorthAmerica with Country
	sealed trait MassachusettsState extends UnitedStates with State
	sealed trait EssexCounty extends MassachusettsState with County
	sealed trait NewHampshireState extends UnitedStates with State
	sealed trait GraftonCounty extends NewHampshireState with County
	sealed trait MiddlesexCountyUS extends MassachusettsState with County
	sealed trait Maryland extends UnitedStates with State

	sealed trait England extends Europe with Country
	sealed trait London extends England with City
	sealed trait WestSussexCounty extends England with County
	sealed trait HorshamDistrict extends WestSussexCounty with District
	sealed trait WestYorkshireCounty extends England with County
	sealed trait Bradford extends WestYorkshireCounty with City
	sealed trait DevonCounty extends England with County
	sealed trait DevonDistrict extends DevonCounty with District
	sealed trait MiddlesexCountyE extends England with County
	sealed trait CumberlandCounty extends England with County
	sealed trait WestmorlandCounty extends England with County

	sealed trait France extends Europe with Country
	sealed trait Greece extends Europe with Country
	sealed trait Aetolia extends Greece with Region
	sealed trait Italy extends Europe with Country
	sealed trait Sardinia extends Italy with State
	sealed trait ApenninePeninsula extends Italy with Peninsula
	sealed trait PapalStates extends ApenninePeninsula with State
	sealed trait Russia extends Asia with Country

	sealed trait Arabia extends Asia with Peninsula  with PersianGulf
	sealed trait Antarctica extends World with Continent

	case object World extends Enum[World] with World {
		val values: IndexedSeq[World] = findValues

		case object Africa extends Enum[Africa] with Africa {
			val values: IndexedSeq[Africa] = findValues

			case object Kenya extends Africa with Country
			case object Tanzania extends Africa with Country
			case object Uganda extends Africa with Country
			case object Algeria extends Africa with Country
			case object Angola extends Africa with Country
			case object Benin extends Africa with Country
			case object Botswana extends Africa with Country
			case object BurkinaFaso extends Africa with Country
			case object Burundi extends Africa with Country
			case object Egypt extends Africa with Country
			case object EquatorialGuinea extends Africa with Country
			case object CapeVerde extends Africa with Country
			case object Cameroon extends Africa with Country
			case object Congo extends Africa with Country
			case object Chad extends Africa with Country
			case object Comoros extends Africa with Country
			case object Djibouti extends Africa with Country
			case object Eswatini extends Africa with Country
			case object Eritrea extends Africa with Country
			case object Ethiopia extends Africa with Country
			case object Gabon extends Africa with Country
			case object Gambia extends Africa with Country
			case object Ghana extends Africa with Country
			case object Guinea extends Africa with Country
			case object IvoryCoast extends Africa with Country
			case object Liberia extends Africa with Country
			case object Libya extends Africa with Country
			case object Lesotho extends Africa with Country
			case object Mauritania extends Africa with Country
			case object Mali extends Africa with Country
			case object Madagascar extends Africa with Country
			case object Mauritius extends Africa with Country
			case object Mozambique extends Africa with Country
			case object Malawi extends Africa with Country
			case object Morocco extends Africa with Country
			case object Namibia extends Africa with Country
			case object Niger extends Africa with Country
			case object Nigeria extends Africa with Country
			case object Rwanda extends Africa with Country
			case object Senegal extends Africa with Country
			case object Seychelles extends Africa with Country
			case object SierraLeone extends Africa with Country
			case object Somalia extends Africa with Country
			case object Sudan extends Africa with Country
			case object Tunisia extends Africa with Country
			case object Togo extends Africa with Country
			case object Zimbabwe extends Africa with Country
			case object Zambia extends Africa with Country
		}

		// SUORCE: https://worldpopulationreview.com/country-rankings/list-of-countries-by-continent
		case object Asia extends Enum[Asia] with Asia {
			val values: IndexedSeq[Asia] = findValues

			case object China extends Asia with Country
			case object Russia extends Enum[Russia] with Russia {

				val values: IndexedSeq[Russia] = findValues

				case object SaintPetersburg extends Russia with City
				case object Yekaterinburg extends Russia with City
				case object Moscow extends Russia with City
				case object NizhnyNovgorod extends Russia with City
				case object Khabarovsk extends Russia with City
				case object Murmansk extends Russia with City
				case object Kazan extends Russia with City
				case object Sochi extends Russia with City
				case object Volgograd extends Russia with City
				case object Derbent extends Russia with City
				case object Tobolsk extends Russia with City
				case object Omsk extends Russia with City
				case object Kalingrad extends Russia with City
				case object Norilsk extends Russia with City
				case object Ufa extends Russia with City
				case object Alexandrov extends Russia with City
				case object Sarov extends Russia with City
				case object Smolensk extends Russia with City
			}
			case object Armenia extends Asia with Country
			case object Afghanistan extends Asia with Country
			case object Azerbaijan extends Asia with Country
			case object Kazakhstan extends Asia with Country
			case object Kyrgyzstan extends Asia with Country
			case object Turkmenistan extends Asia with Country
			case object Tajikistan extends Asia with Country
			case object Uzbekistan extends Asia with Country
			case object Bahrain extends Asia with Country
			case object Bangladesh extends Asia with Country
			case object Bhutan extends Asia with Country
			case object Brunei extends Asia with Country
			case object BritishIndianOceanTerritory extends Asia with Country
			case object Cyprus extends Asia with Country
			case object Georgia extends Asia with Country
			case object India extends Asia with Country
			case object Iran extends Asia with Country
			case object Israel extends Asia with Country
			case object Jordan extends Asia with Country
			case object Kuwait extends Asia with Country
			case object Laos extends Asia with Country
			case object Lebanon extends Asia with Country
			case object Macau extends Asia with Country
			case object Malaysia extends Asia with Country
			case object Maldives extends Asia with Country
			case object Nepal extends Asia with Country
			case object Pakistan extends Asia with Country

			// China-ish
			case object NorthKorea extends Asia with Country
			case object SouthKorea extends Asia with Country
			case object Mongolia extends Asia with Country
			case object HongKong extends Asia with Country
			case object Cambodia extends Asia with Country
			case object Vietnam extends Asia with Country
			case object Indonesia extends Asia with Country
			case object Japan extends Asia with Country
			case object Myanmar extends Asia with Country
			case object Philippines extends Asia with Country
			case object Singapore extends Asia with Country
			case object Taiwan extends Asia with Country
			case object Thailand extends Asia with Country

			case object SriLanka extends Asia with Country
			case object Syria extends Asia with Country
			case object Turkey extends Asia with Country
			case object Yemen extends Asia with Country
			case object TimorLeste extends Asia with Country

			// SOURCE: https://en.wikipedia.org/wiki/Arab_states_of_the_Persian_Gulf#:~:text=The%20Arab%20states%20of%20the,and%20the%20United%20Arab%20Emirates.
			case object Arabia extends Enum[Arabia] with Arabia{

				val values: IndexedSeq[Arabia] = findValues

				case object UnitedArabEmirates extends Arabia with Country
				case object SaudiaArabia extends Arabia with Country
				case object Iraq extends Arabia with Country
				case object Qatar extends Arabia with Country
				case object Oman extends Arabia with Country
				case object Kuwait extends Arabia with Country
				case object Bahrain extends Arabia with Country
			}
		}

		// SOURCE: https://worldpopulationreview.com/country-rankings/list-of-countries-by-continent
		case object Europe extends Enum[Europe] with Europe {
			val values: IndexedSeq[Europe] = findValues

			case object Spain extends Europe with Country
			case object France extends Enum[France] with France {
				val values: IndexedSeq[France] = findValues

				case object Besancon extends France  with City
				case object Paris extends France with City
				case object Bordeaux extends France with City
				case object Marseille extends France with City
				case object Nice extends France with City
				case object Lyon extends France with City
				case object Dijon extends France with City
				case object Cannes extends France with City
				case object Toulouse extends France with City
				case object Montpellier extends France with City
				case object Amiens extends France with City
				case object Grenoble extends France with City
				case object Versailles extends France with City
				case object Reims extends France with City
				case object Rennes extends France with City
				case object Rouen extends France with City
				case object Strasbourg extends France with City
				case object Avignon extends France with City
				case object Chamonix extends France with City
				case object Antibes extends France with City
				case object LaRochelle extends France with City
				case object SaintTropez extends France with City
				case object Calais extends France with City

			}
			case object Portugal extends Europe with Country
			case object Italy extends Enum[Italy] with Italy {
				val values: IndexedSeq[Italy] = findValues

				case object Venice extends Italy with City
				//case object Rome extends Italy with City
				case object Milan extends Italy with City
				case object Naples extends Italy with City
				case object Genoa extends Italy with City
				case object Verona extends Italy with City
				case object Siena extends Italy with City
				case object Syracuse extends Italy with City
				case object Pisa extends Italy with City
				case object Cagliari extends Italy with City

				case object ApenninePeninsula extends Enum[ApenninePeninsula] with ApenninePeninsula {

					val values: IndexedSeq[ApenninePeninsula] = findValues

					case object PapalStates extends Enum[PapalStates] with PapalStates {
						val values: IndexedSeq[PapalStates] = findValues

						case object Rome extends PapalStates with City
					}
				}

				case object Sardinia extends Enum[Sardinia] with Sardinia {
					val values = findValues
					case object GulfOfLaSpezia extends Sardinia with Gulf
				}
			}
			case object Malta extends Europe with Country
			case object Monaco extends Europe with Country
			case object Montenegro extends Europe with Country

			case object Greece extends Enum[Greece] with Greece {
				val values: IndexedSeq[Greece] = findValues

				case object Aetolia extends Enum[Aetolia] with Aetolia {
					val values: IndexedSeq[Aetolia] = findValues

					case object Missolonghii extends Aetolia with Town
				}
			}


			case object NorthMacedonia extends Europe with Country
			case object Scotland extends Europe with Country
			case object Ireland extends Europe with Country
			case object England extends Enum[England] with England {
				val values: IndexedSeq[England] = findValues

				case object WestSussexCounty extends Enum[WestSussexCounty] with WestSussexCounty {
					val values = findValues

					case object HorshamDistrict extends Enum[HorshamDistrict] with HorshamDistrict {
						val values = findValues

						case object Warnham extends HorshamDistrict with Village with CivilParish
					}
				}

				case object London extends Enum[London] with London {
					val values = findValues
					case object Moorgate extends London with Gate
				}

				case object Middlesex extends Enum[MiddlesexCountyE] with MiddlesexCountyE {
					val values: IndexedSeq[MiddlesexCountyE] = findValues
					case object Highgate extends MiddlesexCountyE with District
				}

				case object WestYorkshire extends Enum[WestYorkshireCounty] with WestYorkshireCounty {

					val values: IndexedSeq[WestYorkshireCounty] = findValues

					case object Haworth extends WestYorkshireCounty with Village
					case object Bradford extends Enum[Bradford] with Bradford {
						val values: IndexedSeq[Bradford] = findValues
						case object Thornton extends Bradford with Village
					}
				}

				case object CumberlandCounty extends Enum[CumberlandCounty] with CumberlandCounty {
					val values: IndexedSeq[CumberlandCounty] = findValues
					case object Cockermouth extends CumberlandCounty with Town with CivilParish
				}

				case object Westmorland extends Enum[WestmorlandCounty] with WestmorlandCounty {
					val values: IndexedSeq[WestmorlandCounty] = findValues
					case object Rydal extends WestmorlandCounty with Village
				}

				case object DevonCounty extends Enum[DevonCounty] with DevonCounty {
					val values: IndexedSeq[DevonCounty] = findValues

					case object DevonDistrict extends Enum[DevonDistrict] with DevonDistrict {
						val values: IndexedSeq[DevonDistrict] = findValues

						case object OtteryStMary extends DevonDistrict with Town with CivilParish
					}
				}

				case object York extends England with City
				case object Manchester extends England with City
				case object Bath extends England with City
				case object Liverpool extends England with City
				case object Bristol extends England with City
				case object Oxfor extends England with City
				case object Nottingham extends England with City
				case object Birmingham extends England with City
				case object Coventry extends England with City
				case object Leeds extends England with City
				case object Cardiff extends England with City
				case object Chester extends England with City
				case object Sheffield extends England with City
				case object Leicester extends England with City
				case object Derby extends England with City
				case object Peterborough extends England with City
				case object Salisbury extends England with City
				case object Portsmouth extends England with City
			}
			case object Germany extends Europe with Country
			case object Denmark extends Europe with Country
			case object Netherlands extends Europe with Country
			case object Norway extends Europe with Country
			case object Finland extends Europe with Country
			case object Romania extends Europe with Country
			case object Moldova extends Europe with Country
			case object Lithuania extends Europe with Country
			case object Latvia extends Europe with Country
			case object Liechtenstein extends Europe with Country
			case object Luxembourg extends Europe with Country
			case object Poland extends Europe with Country
			case object Serbia extends Europe with Country
			case object Slovakia extends Europe with Country
			case object Slovenia extends Europe with Country
			case object Sweden extends Europe with Country
			case object Switzerland extends Europe with Country
			case object Ukraine extends Europe with Country
			case object Hungary extends Europe with Country
			case object Croatia extends Europe with Country
			case object Czechia extends Europe with Country
			case object BosniaAndHerzegovina extends Europe with Country
			case object Bulgaria extends Europe with Country
			case object Belgium extends Europe with Country
			case object Belarus extends Europe with Country
			case object Estonia extends Europe with Country
			case object Albania extends Europe with Country
			case object Andorra extends Europe with Country
			case object Austria extends Europe with Country
			case object Iceland extends Europe with Country

			case object FaroeIslands extends Europe with Territory // finland
			case object IsleOfMan extends Europe with Territory // uk
			case object Svalbard extends Europe with Territory // norway
		}

		case object Oceania extends Enum[Oceania] with Oceania {
			val values: IndexedSeq[Oceania] = findValues

			case object Australia extends Oceania with Country
			case object NewZealand extends Oceania with Country
			case object Fiji extends Oceania with Country
			case object Micronesia extends Oceania with Country
			case object PapuaNewGuinea extends Oceania with Country
			case object Samoa extends Oceania with Country
			case object SolomonIslands extends Oceania with Country

			case object AmericanSamoa extends Oceania with Territory
			case object CookIslands extends Oceania with Territory with Island
			case object Guam extends Oceania with Territory
			case object NewCaledonia extends Oceania with Territory
			case object NorfolkIsladn extends Oceania with Territory with Island
		}

		case object SouthAmerica extends Enum[SouthAmerica] with SouthAmerica {
			val values: IndexedSeq[SouthAmerica] = findValues

			case object Peru extends SouthAmerica with Country
			case object Paraguay extends SouthAmerica with Country

			case object Uruguay extends SouthAmerica with Country
			case object Bolivia extends SouthAmerica with Country
			case object Brazil extends SouthAmerica with Country
			case object Chile extends SouthAmerica with Country
			case object Colombia extends SouthAmerica with Country
			case object Argentina extends SouthAmerica with Country
			case object Ecuador extends SouthAmerica with Country
			case object Guyana extends SouthAmerica with Country
			case object Suriname extends SouthAmerica with Country
			case object Venezuela extends SouthAmerica with Country
		}


		case object NorthAmerica extends Enum[NorthAmerica] with NorthAmerica {

			val values: IndexedSeq[NorthAmerica] = findValues

			case object Greenland extends NorthAmerica with Territory
			// SOURCE = https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States
			case object UnitedStates extends Enum[UnitedStates] with UnitedStates {
				val values: IndexedSeq[UnitedStates] = findValues

				case object Mexico extends UnitedStates with Country

				case object Alabama extends UnitedStates with State
				case object Alaska extends UnitedStates with State
				case object Arizona extends UnitedStates with State
				case object Arkansas extends UnitedStates with State
				case object California extends UnitedStates with State
				case object Colorado extends UnitedStates with State
				case object Connecticut extends UnitedStates with State
				case object Delaware extends UnitedStates with State
				case object Florida extends UnitedStates with State
				case object Georgia extends UnitedStates with State
				case object Hawaii extends UnitedStates with State
				case object Idaho extends UnitedStates with State
				case object Illinois extends UnitedStates with State
				case object Indiana extends UnitedStates with State
				case object Iowa extends UnitedStates with State
				case object Kansas extends UnitedStates with State
				case object Kentucky extends UnitedStates with State
				case object Louisiana extends UnitedStates with State
				case object Maine extends UnitedStates with State


				case object Maryland extends Enum[Maryland] with Maryland {
					val values: IndexedSeq[Maryland] = findValues
					case object Baltimore extends Maryland with City
				}
				case object Michigan extends UnitedStates with State

				case object Massachusetts extends Enum[MassachusettsState] with MassachusettsState {
					val values: IndexedSeq[MassachusettsState] = findValues

					case object Boston extends MassachusettsState with City

					case object MiddlesexCountyUS extends Enum[MiddlesexCountyUS] with MiddlesexCountyUS {
						val values: IndexedSeq[MiddlesexCountyUS] = findValues
						case object Concord extends MiddlesexCountyUS with Town
					}

					case object EssexCounty extends Enum[EssexCounty] with EssexCounty {
						val values: IndexedSeq[EssexCounty] = findValues

						case object Salem extends EssexCounty with City
					}

					case object Amherst extends MassachusettsState with City
				}
				case object Minnesota extends UnitedStates with State
				case object Mississippi extends UnitedStates with State
				case object Missouri extends UnitedStates with State
				case object Montana extends UnitedStates with State
				case object Nebraska extends UnitedStates with State

				case object NewHampshire extends Enum[NewHampshireState] with NewHampshireState {
					val values = findValues

					case object GraftonCounty extends Enum[GraftonCounty] with GraftonCounty {
						val values: IndexedSeq[GraftonCounty] = findValues

						case object Plymouth extends GraftonCounty with Town
					}
				}
				case object NewJersey extends UnitedStates with State
				case object NewMichigan extends UnitedStates with State
				case object NewMexico extends UnitedStates with State
				case object NewYork extends UnitedStates with State
				case object Nevada extends UnitedStates with State
				case object NorthCarolina extends UnitedStates with State
				case object SouthCarolina extends UnitedStates with State
				case object NorthDakota extends UnitedStates with State
				case object SouthDakota extends UnitedStates with State
				case object Ohio extends UnitedStates with State
				case object Oklahoma extends UnitedStates with State
				case object Oregon extends UnitedStates with State
				case object Pennsylvania extends UnitedStates with State
				case object RhodeIsland extends UnitedStates with State
				case object Tennessee extends UnitedStates with State
				case object Texas extends UnitedStates with State
				case object Vermont extends UnitedStates with State
				case object Virginia extends UnitedStates with State
				case object Utah extends UnitedStates with State
				case object Washington extends UnitedStates with State
				case object WestVirginia extends UnitedStates with State
				case object Wisconsin extends UnitedStates with State
				case object Wyoming extends UnitedStates with State

				case object DistrictOfColumbia extends UnitedStates with District

				case object AmericanSamoa extends UnitedStates with Territory
				case object Guam extends UnitedStates with Territory
				case object NorthernMarianaIslands extends UnitedStates with Territory
				case object PuertoRico extends UnitedStates with Territory with Island
				case object VirginIslands extends UnitedStates with Territory
			}

			// SOURCE: https://en.wikipedia.org/wiki/Provinces_and_territories_of_Canada
			case object Canada extends Enum[Canada] with Canada {
				val values: IndexedSeq[Canada] = findValues

				case object Ontario extends Canada with Province
				case object Quebec extends Canada with Province
				case object NovaScotia extends Canada with Province
				case object NewBrunswick extends Canada with Province
				case object Manitoba extends Canada with Province
				case object BritishColumbia extends Canada with Province
				case object PrincEdwardIsland extends Canada with Province
				case object Saskatchewan extends Canada with Province
				case object Alberta extends Canada with Province
				case object NewfoundlandAndLabrador extends Canada with Province

				case object NorthwestTerritories extends Canada with Territory
				case object Yukon extends Canada with Territory
				case object Nunavut extends Canada with Territory
			}
		}

		// SOURCE = https://en.wikipedia.org/wiki/Central_America
		case object CentralAmerica extends Enum[CentralAmerica] with CentralAmerica {

			val values: IndexedSeq[CentralAmerica] = findValues

			case object CostaRica extends CentralAmerica with Country
			case object Guatemala extends CentralAmerica with Country
			case object Belize extends CentralAmerica with Country
			case object Honduras extends CentralAmerica with Country
			case object Nicaragua extends CentralAmerica with Country
			case object Panama extends CentralAmerica with Country
			case object AntiguaAndBarbuda extends CentralAmerica with Country
			case object Bahamas extends CentralAmerica with Country
			case object Barbados extends CentralAmerica with Country
			case object Cuba extends CentralAmerica with Country
			case object DominicanRepublic extends CentralAmerica with Country
			case object SaintLucia extends CentralAmerica with Country
			case object Jamaica extends CentralAmerica with Country
			case object Trinidad extends CentralAmerica with Country
			case object Bermuda extends CentralAmerica with Territory
			case object PuertoRico extends CentralAmerica with Territory
		}

		case object Antarctica extends Antarctica

	}


	val s: World = World.NorthAmerica.UnitedStates.Massachusetts.EssexCounty.Salem

	object CountryTemperature {
		import World.Africa._
		import World.Europe._
		import World.NorthAmerica._
		import World.SouthAmerica._
		import World._
		import World.Asia._
		import World.Oceania._
		import World.CentralAmerica._

		val HOT: Seq[World] = List(Australia, Fiji, SolomonIslands, Africa, Africa.Kenya, Africa.Tanzania, Africa.Uganda, Africa.Mauritius, Peru, Colombia, Bolivia, Chile, Spain, Italy, Malta, Monaco, Greece, Turkey, Arabia, Pakistan, Brazil, CostaRica, Argentina, Guatemala, Belize, Nicaragua, Panama, Paraguay, Uruguay, Jamaica, Arabia, Arabia.Qatar, Arabia.Kuwait, Asia.Tajikistan, Asia.Lebanon, Asia.Myanmar, Asia.Singapore, Asia.Syria, Asia.Yemen, Asia.Pakistan, Asia.India)
		val COLD: Seq[World] = List(Russia, Canada, Canada.Manitoba, Canada.BritishColumbia, Canada.Quebec, Canada.Saskatchewan, Canada.Yukon, Scotland, Ireland, England, Germany, Serbia, Slovakia, Slovenia, Iceland, BosniaAndHerzegovina, Switzerland, Austria, Croatia, Ukraine, Belarus, Bulgaria, Denmark, Netherlands, Iceland, Greenland, Antarctica)
		val NEITHER: Seq[World] = List(China, France, UnitedStates, Romania, Poland, Hungary, Croatia, Estonia)

		assert((HOT ++ COLD ++ NEITHER).length == World.values.length)
	}

	object ClimateTemperature {
		import ClimateZone._

		val HOT: Seq[ClimateZone] = List(Arid, Tropical, Desert, Mediterranean)
		val COLD: Seq[ClimateZone] = List(Tundra, Arctic)
		val NEITHER: Seq[ClimateZone] = List(Temperate, Continental, Dry, Humid)

		assert((HOT ++ COLD ++ NEITHER).length == ClimateZone.values.length)
	}


	sealed trait Hemisphere extends EnumEntry
	sealed trait SouthernHemisphere extends Hemisphere
	sealed trait NorthernHemisphere extends Hemisphere
	sealed trait EasternHemisphere extends Hemisphere
	sealed trait WesternHemisphere extends Hemisphere
	sealed trait CentralHemisphere extends Hemisphere

	case object Hemisphere extends Enum[Hemisphere] with Hemisphere {
		val values: IndexedSeq[Hemisphere] = findValues

		case object SouthernHemisphere extends SouthernHemisphere
		case object NorthernHemisphere extends NorthernHemisphere
		case object EasternHemisphere extends EasternHemisphere
		case object WesternHemisphere extends WesternHemisphere
		case object CentralHemisphere extends CentralHemisphere
		//case object SouthernHemisphere extends Enum[SouthernHemisphere] with SouthernHemisphere
	}
	// Shorter names
	val SH = Hemisphere.SouthernHemisphere
	val NH = Hemisphere.NorthernHemisphere
	val EH = Hemisphere.EasternHemisphere
	val WH = Hemisphere.WesternHemisphere
	val CH = Hemisphere.CentralHemisphere

	/*sealed trait Hemisphere extends EnumEntry
	sealed trait SouthernHemisphere extends Hemisphere
	object Hemisphere extends Enum[Hemisphere] with Hemisphere {
	    val values = findValues
	    case object SouthernHemisphere extends Enum[SouthernHemisphere] with SouthernHemisphere {
		   val values: IndexedSeq[SouthernHemisphere] = SOUTHERN_HEMISPHERE.toIndexedSeq
	    }
	}*/

	import World._
	import World.Asia._
	import World.Africa._
	import World.NorthAmerica._
	import World.SouthAmerica._
	import World.CentralAmerica._
	import World.Oceania._
	import World.Europe._
	// SOUTH + West = Argentina, Brazil, Peru
	// NORTH + EAST = Russia
	// SOUTH + CENTRAL = Pakistan, Arabia
	val SOUTHERN_HEMI: List[World] = List(Africa, Arabia, Pakistan, Brazil, Peru, Africa.Uganda, Africa.Mauritius, Africa.Kenya, Africa.Tanzania, Argentina)
	val CENTRAL_HEMI: List[World] = List(CostaRica, Spain, France, Italy, Scotland, Ireland, England, Germany, Arabia, Pakistan, Greece, Turkey)
	val NORTHERN_HEMI: List[World] = List(Russia, Canada, Iceland, Greenland)
	val EASTERN_HEMI: List[World] = List(Russia, China, Australia, Romania, Estonia, Poland, Serbia, Slovakia, Slovenia, Hungary, Croatia)
	val WESTERN_HEMI: List[World] = List(UnitedStates, Canada, Argentina, Brazil, Peru)
	// arabia, pakistan, argentina, brazil, peru, russia, canada


	/**
	 * Checks if a country is present in multiple Hemispheres
	 */
	def isInMultipleHemis(country: World): Boolean = {
		val allHemis: Seq[World] = SOUTHERN_HEMI ++ EASTERN_HEMI ++ WESTERN_HEMI ++ NORTHERN_HEMI ++ CENTRAL_HEMI
		val diffLen: Int = allHemis.length - allHemis.filter(_ != country).length // or allHemis.count(_ == country)
		diffLen > 1
	}

	/**
	 * Gets the names of the countries that are present in multiple Hemispheres
	 */
	val multiHemiCountries: Seq[World] = {
		val allCountries: Seq[World] = World.values
		allCountries.filter(ctry => isInMultipleHemis(ctry))
	}
	val singleHemiCountries: Seq[World] = {
		World.values.filterNot(ctry => multiHemiCountries.contains(ctry))
	}

	// 1 convert list (enums) -> hlist (enums)
	// 2 convert hlist (enums) -> hlist (strs)
	// 3 hlist (strs) -> list (strs)

	/**
	 * given a hemisphere, returns the other hemispheres NAMES NOT equal to the given hemisphere
	 *
	 * @param h
	 */
	def excludeHemi(h: Hemisphere): Seq[Hemisphere] = {
		val hs: Seq[Hemisphere] = List(Hemisphere.SouthernHemisphere, Hemisphere.NorthernHemisphere, Hemisphere.EasternHemisphere, Hemisphere.WesternHemisphere, Hemisphere.CentralHemisphere)

		hs.filter(_ != h)
	}

	/**
	 * Given a hemisphere, returns the other (countries in) the hemispheres NOT equal to the given hemisphere
	 * NOTE: does not check if a particular country from THIS hemisphere (h) is present in the other hemispheres (just does a strict, simple check for this current hemisphere against other ones)
	 */
	def countriesNotFromThisHemi(h: Hemisphere): Seq[World] = {

		def matchHemiEnumToHemiList(h: Hemisphere): Seq[World] = h match {
			case Hemisphere.SouthernHemisphere => SOUTHERN_HEMI
			case Hemisphere.NorthernHemisphere => NORTHERN_HEMI
			case Hemisphere.EasternHemisphere => EASTERN_HEMI
			case Hemisphere.WesternHemisphere => WESTERN_HEMI
			case Hemisphere.CentralHemisphere => CENTRAL_HEMI
			case _ => Seq()
		}

		// hemispheres that are not this given hemisphere
		val notThisHemi: Seq[Hemisphere] = excludeHemi(h)

		// For each allowed hemisphere, must get the corresponding countries within that hemisphere
		notThisHemi.flatMap(hemi => matchHemiEnumToHemiList(hemi))
	}

	// Ideas for astronomy df
	// colnames: GalaxyName, DistanceFromEarth(light years), Satellite nearby, Ringed (boolean, whether planet is ringed or not), Star (for each planet, as array-list), Moon (for each planet, as array-list) as option[list] since not each asteroid has a moon or star for instance.

	sealed trait CelestialBody extends EnumEntry
	sealed trait Planet extends CelestialBody
	sealed trait Moon extends CelestialBody


	sealed trait Constellation extends CelestialBody
	sealed trait MilkyWayConstellation extends Constellation
	sealed trait AndromedaConstellation extends Constellation
	sealed trait CorvusConstellation extends Constellation
	sealed trait CentaurusConstellation extends Constellation
	sealed trait ComaBerenicesConstellation extends Constellation
	sealed trait UrsaMajorConstellation extends Constellation
	sealed trait VirgoConstellation extends Constellation
	sealed trait SculptorConstellation extends Constellation
	sealed trait CygnusConstellation extends Constellation
	sealed trait CanesVenaticiConstellation extends Constellation


	sealed trait Galaxy extends CelestialBody


	case object CelestialBody extends Enum[CelestialBody] with CelestialBody {
		val values: IndexedSeq[CelestialBody] = findValues

		case object BlackHole extends CelestialBody
		case object Star extends CelestialBody
		// TODO make object constellation then object star and have specific star names underneath
		case object Sun extends CelestialBody
		// TODO make list of moon names
		case object Moon extends CelestialBody
		case class Nebula() extends CelestialBody

		case class Asteroid() extends CelestialBody
		case class Meteor() extends CelestialBody
		//case object Planet extends CelestialBody

		case object Planet extends Enum[Planet] with Planet {
			val values: IndexedSeq[Planet] = findValues

			case object Mercury extends Planet with MilkyWayConstellation
			case object Venus extends Planet with MilkyWayConstellation
			case object Earth extends Planet with MilkyWayConstellation
			case object Mars extends Planet with MilkyWayConstellation
			case object Jupiter extends Planet with MilkyWayConstellation
			case object Saturn extends Planet with MilkyWayConstellation
			case object Uranus extends Planet with MilkyWayConstellation
			case object Neptune extends Planet with MilkyWayConstellation
			case object Pluto extends Planet with MilkyWayConstellation
		}


		// SOURCE = https://littleastronomy.com/galaxy-names/
		case object Galaxy extends Enum[Galaxy] with Galaxy {
			val values: IndexedSeq[Galaxy] = findValues

			case object AndromedaGalaxy extends Galaxy with AndromedaConstellation

			case object AntennaeGalaxy extends Galaxy with CorvusConstellation

			case object BackwardGalaxy extends Galaxy with CentaurusConstellation
			case object CentaurusAGalaxy extends Galaxy with CentaurusConstellation

			case object MilkyWayGalaxy extends Galaxy with MilkyWayConstellation

			case object BlackEyeGalaxy extends Galaxy with ComaBerenicesConstellation
			case object ComaPinwheelGalaxy extends Galaxy with ComaBerenicesConstellation
			case object NeedleGalaxy extends Galaxy with ComaBerenicesConstellation

			case object BodesGalaxy extends Galaxy with UrsaMajorConstellation
			case object CigarGalaxy extends Galaxy with UrsaMajorConstellation
			case object MayallsObjectGalaxy extends Galaxy with UrsaMajorConstellation
			case object PinwheelGalaxy extends Galaxy with UrsaMajorConstellation

			case object ButterflyGalaxy extends Galaxy with VirgoConstellation
			case object SombreroGalaxy extends Galaxy with VirgoConstellation

			case object CometGalaxy extends Galaxy with SculptorConstellation
			case object CartwheelGalaxy extends Galaxy with SculptorConstellation
			case object SculptorDwarfGalaxy extends Galaxy with SculptorConstellation

			case object FireworksGalaxy extends Galaxy with CygnusConstellation

			case object HockeyStickGalaxy extends Galaxy with CanesVenaticiConstellation
			case object SunflowerGalaxy extends Galaxy with CanesVenaticiConstellation
			case object WhirlpoolGalaxy extends Galaxy with CanesVenaticiConstellation
			case object EyeOfSauronGalaxy extends Galaxy with CanesVenaticiConstellation
		}

		case object Constellation extends Enum[Constellation] with Constellation {

			val values: IndexedSeq[Constellation] = findValues

			case object AndromedaConstellation extends Constellation
			case object CorvusConstellation extends Constellation
			case object CentaurusConstellation extends Constellation
			case object MilkyWayConstellation extends Constellation
			case object ComaBerenicesConstellation extends Constellation
			case object UrsaMajorConstellation extends Constellation
			case object SculptorConstellation extends Constellation
			case object CygnusConstellation extends Constellation
			/*case object AndromedaConstellation extends Enum[AndromedaConstellation] with AndromedaConstellation {
			    val values = findValues
			}  */
		}
	}
	/*val c1: CelestialBody = CelestialBody.Planet.Venus
	val c2: Planet = CelestialBody.Planet.Mars
	val c3: CelestialBody = CelestialBody.Galaxy
	val c4: CelestialBody = CelestialBody.Nebula()*/
}
