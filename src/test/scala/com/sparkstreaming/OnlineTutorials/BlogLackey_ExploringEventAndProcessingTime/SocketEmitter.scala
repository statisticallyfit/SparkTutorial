package com.sparkstreaming.OnlineTutorials.BlogLackey_ExploringEventAndProcessingTime


import java.io.{OutputStreamWriter, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.util.Date
import scala.io.Source

/**
 * Source = https://github.com/buildlackey/spark-streaming-group-by-event-time/blob/master/src/test/scala/SocketEmitter.scala
 */
object SocketEmitter {
	/**
	 * Input params
	 *
	 * port -- we create a socket on this port
	 * fileName -- emit lines from this file ever sleep seconds
	 * sleepSecsBetweenWrites -- amount to sleep before emitting
	 */
	def main(args: Array[String]) {

		val PATH: String = """/development/projects/statisticallyfit/github/learningspark/SparkTutorial/src/main/scala/com/sparkstreaming/OnlineTutorials/BlogLackey_ExploringEventAndProcessingTime/"""
		val inputDataName: String = "inputAnimalTimes.txt"
		val myArgs: (String, String, Int) = ("9999", PATH + inputDataName, 1) // TODO how long sleep? 4*5*1000?

		myArgs /*args*/ match {
			case (port, fileName, sleepSecs) => {

				val serverSocket: ServerSocket = new ServerSocket(port.toInt)
				val socket: Socket = serverSocket.accept()
				val out: PrintWriter =
					new PrintWriter(
						new OutputStreamWriter(socket.getOutputStream))

				for (line <- Source.fromFile(fileName).getLines()) {
					println(s"input line: ${line.toString}")
					var items: Array[String] = line.split("\\|")
					items.foreach { item =>
						val output = s"$item"
						println(
							s"emitting to socket at ${new Date().toString()}: $output")
						out.println(output)
					}
					out.flush()
					Thread.sleep(1000 * sleepSecs.toInt)
				}
			}
			case _ =>
				throw new RuntimeException(
					"USAGE socket <portNumber> <fileName> <sleepSecsBetweenWrites>")
		}

		// Thread.sleep(9999999 * 1000) // sleep until we are killed
	}

}
