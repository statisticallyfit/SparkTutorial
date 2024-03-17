ThisBuild / organization := "statisticallyfit"
ThisBuild / name := "SparkTutorial"

ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.13.2" // Changed to version 2.13.2 from 2.13.5 because of this bug = https://users.scala-lang.org/t/match-would-fail-on-the-following-input-list/7281/2


// Sources:
// https://stackoverflow.com/questions/66228218/intellij-doesnt-recognize-code-in-build-sbt-and-doesnt-compile
// https://medium.com/@supermanue/how-to-publish-a-scala-library-in-github-bfb0fa39c1e4
ThisBuild / githubOwner := "statisticallyfit"
ThisBuild / githubRepository := "SparkTutorial"



Test / parallelExecution := false
Test / testOptions += Tests.Argument("-oDF")
Test / logBuffered := false

run / fork := false
Global / cancelable := true



// Recommended scala 2.13 compiler options = https://nathankleyn.com/2019/05/13/recommended-scalac-flags-for-2-13/
lazy val compilerOptions = Seq(
	"-deprecation",
	"-unchecked",
	"-feature",
	"-Xlog-reflective-calls", // copied from scalacOptions
	"-Xlint", "Xlint:unused",
	"-Wconf:cat=unused-nowarn:s",
	"-Wconf:cat=other-match-analysis:error",
	"-language:existentials",
	"-language:higherKinds",
	"-language:implicitConversions",
	"-language:postfixOps",

	//Remove several options at once:  https://stackoverflow.com/a/75554657
	//"-Xopt1", "-Xopt1",
	"-Wunused:imports", "-Wunused:params", "Wunused:nowarn",

	// Source = https://medium.com/life-at-hopper/make-your-scala-compiler-work-harder-971be53ae914
	"-Ywarn-extra-implicit",  // More than one implicit parameter section is defined.
	//"-Ywarn-inaccessible",  // Inaccessible types in method signatures.
	//"-Ywarn-infer-any",  // A type argument is inferred to be `Any`.
	//"-Ywarn-nullary-override", // non-nullary `def f()' overrides nullary `def f'.
	//"-Ywarn-nullary-unit",  // nullary method returns Unit.
	//"-Ywarn-numeric-widen",  // Numerics are implicitly widened.
	"-Ywarn-unused:implicits",  // An implicit parameter is unused.
	"-Ywarn-unused:imports",   // An import selector is not referenced.
	"-Ywarn-unused:locals",   // A local definition is unused.
	"-Ywarn-unused:params",  // A value parameter is unused.
	"-Ywarn-unused:patvars",   // A variable bound in a pattern is unused.
	//"-Ywarn-value-discard",   // Non-Unit expression results are unused.
	"-Ywarn-unused:privates",  // A private member is unused.

	//"-Ylog-classpath"
	// TODO try putting Xnojline:off = https://hyp.is/Ard1uM71Ee2sWMf7uSXXaQ/docs.scala-lang.org/overviews/compiler-options/index.html

	//"-XJline:off" // TODO trying to stop this message from appearing on REPL: warning: -Xnojline is
	// deprecated: Replaced by -Xjline:off
	//"-Ypartial-unification" //todo got error in sbt compilation " error: bad option" why?
	//"-encoding",	//"utf8"
)



Compile / scalacOptions ++= Seq(
  //s"-target:$javaVersion",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlog-reflective-calls",
  "-Xlint",
  "-Wconf:cat=other-match-analysis:error",
  //"-Ywarn-unused-imports",
  "-Ywarn-unused:imports")
//scalacOptions in (Compile, Console) ~= { _.filterNot(Set("-Ywarn-unused-import", "-Ywarn-unused:imports")) }

Compile / javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation")





// HELP
// way 1 - trying to coursier publish spark-pit causes error "Repository for publishing is not found"
// way 2 - when trying to import spark-pit as local git repo from SparkTutorial sbt , error appears "ssh: Could not resolve hostname file: Temporary failure in name resolution
//fatal: Could not read from remote repository."


lazy val commitTime = "f4a4f042dadb35bdfd78c4808c1db4fd1aef7534"

// NOTE: way 1 - using git link, but this downloads the git repo into home anyway so best to download manually and use the manual path
// SOURCE (gitlink way): https://hyp.is/e_ivtOLWEe66e1fWOyQMPg/xebia.com/blog/git-subproject-compile-time-dependencies-in-sbt/
//lazy val sparkPitGitRepo = ProjectRef(uri("git://github.com/Ackuq/spark-pit.git#master"), "spark-pit")
// NOTE: way 2 - using manual path
// SOURCES (path way):
// https://stackoverflow.com/a/67908451
lazy val sparkPitLocalGitRepo = ProjectRef(file(s"git:file:///development/projects/statisticallyfit/github/learningspark/libs/spark-pit/"), "spark-pit")
// https://stackoverflow.com/a/21211777 (WARNING uses RootProject not ProjectRef so contrasts xebia's comment



//lazy val sparkPITExtendedInGit = ProjectRef(uri("https://github.com/statisticallyfit/spark-pit.git#master"), "spark-pit")

/*
lazy val sparkPITExtendedInLocalCoursier = ProjectRef(
	//file("/development/tmp/.coursier"),
	file("/development/tmp/.coursier"),
	"root--coursier"
)
*/



enablePlugins(BuildInfoPlugin) // TODO how to know what is the name of my declared plugins in the plugins.sbt file?
//enablePlugins(GitPlugin)
//enablePlugins(SbtGithubPlugin)
enablePlugins(SbtDotenv)
enablePlugins(GitHubPackagesPlugin)



// global is the parent project, which aggregates all the other projects
lazy val global: Project = project
	.in(file(".") ) //.dependsOn(commonSettings % "compile->compile;test->test")
	.settings(
		name := "SparkTutorial"
	)
	.settings(commonSettings)
	.settings(
		libraryDependencies ++= rootDependencies ++ testLibDependencies
	)
	.enablePlugins(BuildInfoPlugin) // TODO how to know what is the name of my declared plugins in the plugins.sbt file?
	//.enablePlugins(GitPlugin)
	//.enablePlugins(SbtGithubPlugin)
	.enablePlugins(SbtDotenv)
	.enablePlugins(GitHubPackagesPlugin)
	//.dependsOn(sparkPitLocalGitRepo)
	//.dependsOn(sparkPITExtendedInLocalCoursier)








lazy val rootDependencies = /*libraryDependencies ++=*/ Seq(/*commonDependencies ++*/

	allDependencies.scalaLibrary,
	allDependencies.scalaCompiler,
	allDependencies.scalaReflect,

	allDependencies.scalaCheck,
	allDependencies.scalaCheckCats,

	allDependencies.specs2Core,
	allDependencies.specs2ScalaCheck,

	allDependencies.scalaTest,

	allDependencies.scalactic,

	allDependencies.sourcecodeLiHaoyi,

	allDependencies.enumeratumLib,

	/*allDependencies.discipline,
	allDependencies.discipline_core,
	allDependencies.discipline_scalatest,
	allDependencies.discipline_specs2,


	allDependencies.cats_core,
	allDependencies.cats_kernel,
	allDependencies.cats_laws,
	allDependencies.cats_free,
	allDependencies.cats_macros,
	allDependencies.cats_testkit,
	allDependencies.cats_effects,*/

	allDependencies.shapelessCore,

	allDependencies.scalazCore,

	/*allDependencies.zio,
	allDependencies.zioSchema,
	allDependencies.zioSchemaAvro,
	allDependencies.zioSchemaJson,
	allDependencies.zioSchemaProtobuf,
	allDependencies.zioSchemaDerivation,
	allDependencies.zioStream,
	allDependencies.zioTest,

	allDependencies.matryoshkaCore,

	allDependencies.spireKindProjector,
	allDependencies.typelevelKindProjector,

	allDependencies.drosteCore,
	allDependencies.drosteLaws,
	allDependencies.drosteMacros,
	allDependencies.drosteScalaCheck, */


	// Dependecy (json4s-core, ast, jackson) - versioning error. If for all the json4s libs, if I don't keep
	// the version the same, and state them explciitly here, then compiler complains with classpath error (jvalue not found)
	// Solution source = https://stackoverflow.com/a/47669923


	// HELP not working to load this
	/*allDependencies.json4s,
	allDependencies.json4s_native,
	allDependencies.json4s_jackson,
	allDependencies.json4s_jackson_core,
	allDependencies.json4s_core,
	allDependencies.json4s_ast,
	allDependencies.json4s_native_core,
	allDependencies.json4s_ext,
	allDependencies.json4s_scalap, */

	/*allDependencies.avroTools_for_avdlToAvsc,


	allDependencies.avro4s_core,
	allDependencies.avro4s_json,*/

	allDependencies.sparkCore,
	allDependencies.sparkSql,
	allDependencies.sparkMLLib,
	allDependencies.sparkAvro,
	allDependencies.sparkStreaming,
	//allDependencies.sparkDatabricksXML,

	/*allDependencies.sparkCoreTests,
	allDependencies.sparkCoreTestSources,
	allDependencies.sparkSqlTests,
	allDependencies.sparkSqlTestSources,
	allDependencies.sparkCatalystTests,
	allDependencies.sparkCatalystTestSources,*/


	/*allDependencies.sparkConnectorTests,
	allDependencies.sparkConnectorTestSources,
	allDependencies.sparkExecutionTests,
	allDependencies.sparkExecutionTestSources,*/


	allDependencies.sparkFastTestsMrPowers,
	allDependencies.sparkDariaMrPowers,


	// HELP not working to load this
	//allDependencies.sparkStreamingKafka, // HELP not found


	allDependencies.kafkaApache,

	allDependencies.thoughtworksXtream,


	allDependencies.nscalaTime,

	allDependencies.duckDB,
	allDependencies.questDB,
	allDependencies.deephaven_Base,
	allDependencies.deephaven_EngineTable,
	allDependencies.deephaven_EngineFunction,
	allDependencies.deephaven_EngineTime,
	allDependencies.deephaven_EngineTuple,
	allDependencies.deephaven_Stats,
	allDependencies.deephaven_Util,
	allDependencies.deephaven_qst,
	allDependencies.deephaven_qsttype,
	allDependencies.deephaven_csv,
)


lazy val testLibDependencies = Seq(
	// allDependencies.sparkCoreTests,
	allDependencies.sparkCoreCCTT,
	allDependencies.sparkCoreTestSources,

	// allDependencies.sparkSqlTests,
	allDependencies.sparkSqlCCTT,
	allDependencies.sparkSqlTests,
	allDependencies.sparkSqlTestSources,

	allDependencies.sparkStreamingCCTT,
	allDependencies.sparkStreamingTestSources,

	allDependencies.sparkCatalystCCTT,
	allDependencies.sparkCatalystTestSources,
)





lazy val commonSettings: Seq[Def.Setting[_ >: Task[Seq[String]] with Seq[Resolver] with Seq[ModuleID] <: Equals]] = Seq(
	scalacOptions ++= compilerOptions,

	resolvers ++= Seq(
		//"Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
		"Local Maven Repository" at "file:///development/projects/statisticallyfit/github/learningspark/libs/spark-pit" + "/.m2/repository",
		Resolver.sonatypeRepo("releases"),
		Resolver.sonatypeRepo("snapshots"),
		"Local Coursier Repository" at ("file://" + "/development/tmp/.coursier")
	)
) ++ compilerPlugins


lazy val compilerPlugins: Seq[Def.Setting[Seq[ModuleID]]] = Seq(
	libraryDependencies ++= Seq(
			compilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),

			// NOTE: got withFilter error (in objectJsonSchemaDecoder) in for-comprehension so using this plugin = https://github.com/oleg-py/better-monadic-for
			compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
		))







lazy val allDependencies =
	new {

		// Listing the versions as values
		val versionOfScala = "2.13.2"  //TODO how to use the scalaVersion variable above?

		val versionOfScalaTest = "3.2.17" //"3.3.0-SNAP2"

		val versionOfScalaCheck = "1.17.0"

		val versionOfScalaCheckCats = "0.3.2"



		// TODO pom.xml has "org.specs" version 1.2.5???
		val versionOfSpecs2 = "4.20.3" //"4.19.2" //4.9.4

		val versionOfScalactic = "3.2.17"


		val versionOfSourcecode = "0.3.1"

		val versionOfEnumeratum = "1.7.3"

		val versionOfSpireKindProjector = "0.9.10"
		val versionOfTypelevelKindProjector = "0.13.2"

		val versionOfCats = "2.10.0" //"2.9.0" // "2.2.0-M3"
		val versionOfCats_effects = "3.5.2"
		val versionOfCats_macros = "2.1.1"


		val versionOfShapeless = "2.3.10"

		val versionOfScalaz = "7.3.8"
		val versionOfDroste = "0.8.0" // "0.9.0"
		val versionOfMatryoshka = "0.21.3"


		/*val versionOfZIO = "2.0.13"
		val versionOfZIO_streams = "2.0.13"
		val versionOfZIO_test = "2.0.13"

		val versionOfZIO_schema = "0.4.11" //"0.4.8"*/

		// Try downgrading to 3.6.6 because of "NoClassDefFoundError" for Jvalue
		// Source = https://stackoverflow.com/questions/69912882/java-lang-classnotfoundexception-org-json4s-jsonastjvalue

		val versionOfJson4s_simple = "3.2.11" //"3.6.6" // for scala 2.11
		val versionOfJson4s_others = "4.0.6" //"3.6.6" //3.6.6"//"4.0.6"

		/*val versionOfAvroTools = "1.11.1"

		val versionOfAvro4S = "4.1.1"*/


		val versionOfSpark = "3.5.0"
		val versionOfSparkStreamingKafka = "1.6.3"
		val versionOfSparkDatabricksXML = "0.17.0" //"0.17.0" // was 0.4.1 in pom.xml

		val versionOfSparkFastTests = "1.3.0" // "2.3.1_0.15.0"
		val versionOfSparkDaria = "1.2.3"

		val versionOfKafkaApache = "3.6.1"

		val versionOfThoughtworksXtream = "1.4.20" // was 1.4.11 in pom.xml

		val versionOfNScalaTime = "2.32.0"


		val versionOfDuckDB = "0.10.0"
		val versionOfQuestDB = "7.3.10"
		val versionOfDeephaven = "0.33.2" //snapshot
		val versionOfDeephavenCSV = "0.14.0"
		val versionOfAxelPetersonSparkPIT = "" // snapshot

		//------------------

		// Listing the different dependencies
		val scalaLibrary = "org.scala-lang" % "scala-library" % versionOfScala
		val scalaCompiler = "org.scala-lang" % "scala-compiler" % versionOfScala
		val scalaReflect = "org.scala-lang" % "scala-reflect" % versionOfScala


		val scalactic = "org.scalactic" %% "scalactic" % versionOfScalactic

		val scalaTest = "org.scalatest" %% "scalatest" % versionOfScalaTest % Test

		val scalaCheck = "org.scalacheck" %% "scalacheck" % versionOfScalaCheck % Test
		// https://mvnrepository.com/artifact/io.chrisdavenport/cats-scalacheck
		val scalaCheckCats = "io.chrisdavenport" %% "cats-scalacheck" % versionOfScalaCheckCats % Test


		val specs2Core = "org.specs2" %% "specs2-core" % versionOfSpecs2 % Test
		val specs2ScalaCheck = "org.specs2" %% "specs2-scalacheck" % versionOfSpecs2 % Test
		// TODO - difference between specs2-scalacheck and the ordinary scalacheck???

		val sourcecodeLiHaoyi = "com.lihaoyi" %% "sourcecode" % versionOfSourcecode

		/**
		 * Key feature - want to do nesting = https://hyp.is/zV75Zq-cEe6aB4tRh_dhVg/github.com/lloydmeta/enumeratum
		 * e.g. Artist -> Painter -> VanGogh to connect the types
		 */
		val enumeratumLib = "com.beachape" %% "enumeratum" % versionOfEnumeratum

		//val discipline = "org.typelevel" %% "discipline" % versionOfDiscipline
		//val discipline_core = "org.typelevel" %% "discipline-core" % versionOfDiscipline_core
		//val discipline_scalatest = "org.typelevel" %% "discipline-scalatest" % versionOfDiscipline_scalatest % Test
		//val discipline_specs2 = "org.typelevel" %% "discipline-specs2" % versionOfDiscipline_specs2 % Test

		// SOURCE = https://mvnrepository.com/artifact/org.spire-math/kind-projector
		val spireKindProjector = "org.spire-math" %% "kind-projector" % versionOfSpireKindProjector
		val typelevelKindProjector = "org.typelevel" %% "kind-projector" % versionOfTypelevelKindProjector cross CrossVersion.full

		val cats_core = "org.typelevel" %% "cats-core" % versionOfCats
		val cats_kernel = "org.typelevel" %% "cats-kernel" % versionOfCats
		val cats_laws = "org.typelevel" %% "cats-laws" % versionOfCats % Test
		val cats_free = "org.typelevel" %% "cats-free" % versionOfCats
		val cats_macros = "org.typelevel" %% "cats-macros" % versionOfCats_macros //versionOfCats
		//versionOfCats_macros
		val cats_testkit = "org.typelevel" %% "cats-testkit" % versionOfCats % Test
		val cats_effects = "org.typelevel" %% "cats-effect" % versionOfCats_effects % Test

		//Shapeless
		val shapelessCore = "com.chuusai" %% "shapeless" % versionOfShapeless


		// Scalaz
		val scalazCore = "org.scalaz" %% "scalaz-core" % "7.3.8"

		// Matryoshka recursion schemes
		val matryoshkaCore = "com.slamdata" %% "matryoshka-core" % "0.21.3"
		// TODO WARNING matryoshka is the only lib that doesn't support over scala 2.12

		//Droste recursion schemes
		val drosteCore = "io.higherkindness" %% "droste-core" % versionOfDroste
		val drosteLaws = "io.higherkindness" %% "droste-laws" % versionOfDroste
		val drosteMacros = "io.higherkindness" %% "droste-macros" % versionOfDroste
		/*"io.higherkindness" %% "droste-meta" % "0.8.0",
		"io.higherkindness" %% "droste-reftree" % "0.8.0",*/
		val drosteScalaCheck = "io.higherkindness" %% "droste-scalacheck" % versionOfDroste


		// ZIO-schema
		/*val zio = "dev.zio" %% "zio" % versionOfZIO
		val zioSchema = "dev.zio" %% "zio-schema" % versionOfZIO_schema
		val zioSchemaAvro = "dev.zio" %% "zio-schema-avro" % versionOfZIO_schema
		val zioSchemaJson = "dev.zio" %% "zio-schema-json" % versionOfZIO_schema
		val zioSchemaProtobuf = "dev.zio" %% "zio-schema-protobuf" % versionOfZIO_schema
		// Required for automatic generic derivation of schemas
		val zioSchemaDerivation = "dev.zio" %% "zio-schema-derivation" % versionOfZIO_schema
		val zioStream = "dev.zio" %% "zio-streams" % versionOfZIO_streams
		val zioTest = "dev.zio" %% "zio-test" % versionOfZIO_test*/



		// SPARK THINGS ---------------------------------------

		//val scalaMavenPlugin = "org.scala-tools" % "maven-scala-plugin" % "2.15.2"

		// For Zubair Nabi book
		val json4s = "org.json4s" %% "json4s" % versionOfJson4s_simple
		val json4s_jackson = "org.json4s" %% "json4s-jackson" % versionOfJson4s_others
		val json4s_jackson_core = "org.json4s" %% "json4s-jackson-core" % versionOfJson4s_others
		val json4s_core = "org.json4s" %% "json4s-core" % versionOfJson4s_others
		val json4s_ast = "org.json4s" %% "json4s-ast" % versionOfJson4s_others
		val json4s_native = "org.json4s" %% "json4s-native" % versionOfJson4s_others
		val json4s_native_core = "org.json4s" %% "json4s-native-core" % versionOfJson4s_others
		val json4s_ext = "org.json4s" %% "json4s-ext" % versionOfJson4s_others
		val json4s_scalap = "org.json4s" %% "json4s-scalap" % versionOfJson4s_others

		// https://mvnrepository.com/artifact/org.apache.avro/avro-tools
		//val avroTools_for_avdlToAvsc = "org.apache.avro" % "avro-tools" % versionOfAvroTools

		/*val avro4s_core = "com.sksamuel.avro4s" %% "avro4s-core" % versionOfAvro4S
		val avro4s_json = "com.sksamuel.avro4s" %% "avro4s-json" % versionOfAvro4S*/



		// Spark:
		val sparkCore = "org.apache.spark" %% "spark-core" % versionOfSpark
		// NOTE: removing provided because got "NoClassDefFoundError" for org/apache/spark.sql/streaming/DataStreamWriter
		// Source = https://stackoverflow.com/a/55753164
		val sparkSql = "org.apache.spark" %% "spark-sql" % versionOfSpark // % "provided"
		val sparkMLLib = "org.apache.spark" %% "spark-mllib" % versionOfSpark // % "provided"
		val sparkAvro = "org.apache.spark" %% "spark-avro" % versionOfSpark
		val sparkStreaming =  "org.apache.spark" %% "spark-streaming" % versionOfSpark // % "provided"

		val sparkStreamingKafka = "org.apache.spark" %% "spark-streaming-kafka" % versionOfSparkStreamingKafka

		val sparkDatabricksXML = "com.databricks" %% "spark-xml" % versionOfSparkDatabricksXML

		// Spark Test code:
		val sparkCoreCCTT = "org.apache.spark" %% "spark-core" % versionOfSpark % "compile->compile;test->test" // Test classifier "tests"
		val sparkCoreTests = "org.apache.spark" %% "spark-core" % versionOfSpark % Test classifier "tests"
		val sparkCoreTestSources = "org.apache.spark" %% "spark-core" % versionOfSpark % Test classifier "test-sources"

		val sparkSqlCCTT = "org.apache.spark" %% "spark-sql" % versionOfSpark % "compile->compile;test->test" // Test classifier "tests"
		val sparkSqlTests = "org.apache.spark" %% "spark-sql" % versionOfSpark %  Test classifier "tests"
		val sparkSqlTestSources =  "org.apache.spark" %% "spark-sql" % versionOfSpark % Test classifier "test-sources"

		val sparkStreamingCCTT =  "org.apache.spark" %% "spark-streaming" % versionOfSpark % "compile->compile;test->test" // Test classifier "tests"
		val sparkStreamingTestSources =  "org.apache.spark" %% "spark-streaming" % versionOfSpark % Test classifier "test-sources" // Test classifier "tests"


		val sparkCatalystCCTT = "org.apache.spark" %% "spark-catalyst" % versionOfSpark % "compile->compile;test->test" // Test classifier "tests"
		val sparkCatalystTestSources = "org.apache.spark" %% "spark-catalyst" % versionOfSpark % Test classifier "test-sources"

		/*val sparkConnectorTests = "org.apache.spark" %% "spark-sql-connector" % versionOfSpark % Test classifier "tests"
		val sparkConnectorTestSources = "org.apache.spark" %% "spark-sql-connector" % versionOfSpark % Test classifier "test-sources"
		val sparkExecutionTests = "org.apache.spark" %% "spark-execution" % versionOfSpark % Test classifier "tests"
		val sparkExecutionTestSources = "org.apache.spark" %% "spark-execution" % versionOfSpark % Test classifier "test-sources"*/


		// Spark Fast Tests
		val sparkFastTestsMrPowers = "com.github.mrpowers" %% "spark-fast-tests" % versionOfSparkFastTests % Test
			//"com.github.mrpowers" % "spark-fast-tests_2.12" % "1.3.0"
			//"com.github.mrpowers" %% "spark-fast-tests" % versionOfSparkFastTests % Test
			//"MrPowers" % "spark-fast-tests" % versionOfSparkFastTests % Test

		// Spark-Daria
		val sparkDariaMrPowers =  "com.github.mrpowers" %% "spark-daria" % versionOfSparkDaria % Test

		// Kafka
		val kafkaApache = "org.apache.kafka" %% "kafka" % versionOfKafkaApache

		// Xstream
		val thoughtworksXtream =  "com.thoughtworks.xstream" % "xstream" % versionOfThoughtworksXtream

		// Nscala-time
		// https://mvnrepository.com/artifact/com.github.nscala-time/nscala-time
		val nscalaTime = "com.github.nscala-time" %% "nscala-time" % versionOfNScalaTime


		// SQL Streaming Data Joins (point-in-time joins / as-of joins)
		// DuckDB = https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc
		val duckDB = "org.duckdb" % "duckdb_jdbc" % versionOfDuckDB
		// QuestDB
		val questDB = "org.questdb" % "questdb" % versionOfQuestDB
		// Deephaven
		val deephaven_Base = "io.deephaven" % "deephaven-Base" % versionOfDeephaven % "runtime" // TODO must import as coursier dependency ??
		val deephaven_EngineTable = "io.deephaven" % "deephaven-engine-table" % versionOfDeephaven % "runtime"
		val deephaven_EngineFunction = "io.deephaven" % "deephaven-engine-function" % versionOfDeephaven % "runtime"
		val deephaven_EngineTime = "io.deephaven" % "deephaven-engine-time" % versionOfDeephaven % "runtime"
		val deephaven_EngineTuple = "io.deephaven" % "deephaven-engine-tuple" % versionOfDeephaven % "runtime"
		val deephaven_Stats = "io.deephaven" % "deephaven-Stats" % versionOfDeephaven % "runtime"
		val deephaven_Util = "io.deephaven" % "deephaven-Util" % versionOfDeephaven
		val deephaven_qst = "io.deephaven" % "deephaven-qst" % versionOfDeephaven
		val deephaven_qsttype = "io.deephaven" % "deephaven-qst-type" % versionOfDeephaven
		val deephaven_csv = "io.deephaven" % "deephaven-csv" % versionOfDeephavenCSV
		// Spark-PIT
		val SparkPITAxelPeterson = ""
		// TODO must import github repo as dependency


	}


//addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.10")
addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full)







// Setting timeout for coursier so snapshot can get picked up
// SOURCE = https://stackoverflow.com/a/67862862
//import scala.concurrent.duration.DurationInt
//import lmcoursier.definitions.CachePolicy
//
//csrConfiguration := csrConfiguration.value
//	.withTtl(0.seconds)
//	.withCachePolicies(Vector(CachePolicy.LocalOnly)





Compile / run := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated


/*ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

This / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.common.**" -> "my_conf.@1").inAll
)

// See here for all of the possible configuration options:
// https://www.scala-sbt.org/sbt-native-packager/formats/docker.html
enablePlugins(JavaAppPackaging, DockerPlugin)
dockerBaseImage := s"docker.io/library/adoptopenjdk:$dockerJavaVersion-jre-hotspot"
dockerExposedPorts := Seq(4040)
dockerUsername := sys.props.get("docker.username")
dockerRepository := sys.props.get("docker.registry")
*/



//ThisBuild / dynverSeparator := "-" // TODO gives error must comment out
