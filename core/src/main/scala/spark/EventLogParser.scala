package spark

import scala.collection.mutable.ArrayBuffer
import spark.SparkContext._
import java.io._

object EventLogParser {
  def readRDDs(sc: SparkContext): Seq[RDD[_]] = {
    println("Test")
    val ois = new ObjectInputStream(new FileInputStream(System.getProperty("spark.logging.eventLog")))
    val rdds = new ArrayBuffer[RDD[_]]
    try {
      while (true) {
        println("Reading")
        ois.readObject.asInstanceOf[EventReporterMessage] match {
          case ReportException(exception) => {}
          case ReportRDDCreation(rdd) =>
            println("Read an RDD creation: %s".format(rdd))
            rdd.setContext(sc)
            rdds += rdd
          case ReportRDDChecksum(rdd, split, checksum) => {}
        }
      }
    } catch {
      case e: EOFException => {}
    }
    rdds
  }
}
