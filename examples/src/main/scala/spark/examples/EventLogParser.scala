package spark.examples

import spark._
import spark.SparkContext._
import java.io._

object EventLogParser {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: EventLogParser <host>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "EventLogParser")
    val eventLog =
      try {
        Some(new FileInputStream(System.getProperty("spark.logging.eventLog")))
      } catch {
        case e: FileNotFoundException =>
          System.err.println("Can't read %s: %s".format(System.getProperty("spark.logging.eventLog"), e))
        None
      }
    for (l <- eventLog) {
      val ois = new ObjectInputStream(l)
      try {
        while (true) {
          var rdd = ois.readObject.asInstanceOf[RDD[_]]
          println(rdd)
        }
      } catch {
        case e: EOFException =>
          System.exit(0)
        case e: Exception =>
          System.err.println(e)
          System.exit(0)
      }
    }
  }
}
