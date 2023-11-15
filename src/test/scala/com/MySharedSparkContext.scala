package com



import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.concurrent.Lock

/**
 * Source 1 (spark tests) =https://github.com/apache/spark/blob/master/core/src/test/scala/org/apache/spark/SharedSparkContext.scala
 *
 * Source 2 (reason for altering original code from spark source) = https://stackoverflow.com/a/35481434
 */


object MySharedSparkContext {
	val lock = new Lock()
}

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait MySharedSparkContext extends BeforeAndAfterAll with BeforeAndAfterEach {
	self: Suite =>

	@transient private var _sc: SparkContext = _

	def sc: SparkContext = _sc

	var conf = new SparkConf(false)

	override def beforeAll() {
		MySharedSparkContext.lock.acquire()
		_sc = new SparkContext("local[4]", "test", conf)
		super.beforeAll()
	}

	override def afterAll() {
		if (_sc != null) {
			_sc.stop()
		}
		MySharedSparkContext.lock.release()

		// To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
		System.clearProperty("spark.driver.port")

		_sc = null
		super.afterAll()
	}

	/*@transient private var _sc: SparkContext = _

	def sc: SparkContext = _sc

	val conf = new SparkConf(false)

	/**
	 * Initialize the [[SparkContext]].  Generally, this is just called from beforeAll; however, in
	 * test using styles other than FunSuite, there is often code that relies on the session between
	 * test group constructs and the actual tests, which may need this session.  It is purely a
	 * semantic difference, but semantically, it makes more sense to call 'initializeContext' between
	 * a 'describe' and an 'it' call than it does to call 'beforeAll'.
	 */
	protected def initializeContext(): Unit = {
		if (null == _sc) {
			_sc = new SparkContext(
				"local[4]", "test", conf.set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName))
		}
	}

	override def beforeAll(): Unit = {
		super.beforeAll()
		initializeContext()
	}

	override def afterAll(): Unit = {
		try {
			LocalSparkContext.stop(_sc)
			_sc = null
		} finally {
			super.afterAll()
		}
	}

	protected override def beforeEach(): Unit = {
		super.beforeEach()
		DebugFilesystem.clearOpenStreams()
	}

	protected override def afterEach(): Unit = {
		super.afterEach()
		DebugFilesystem.assertNoOpenStreams()
	}*/
}
