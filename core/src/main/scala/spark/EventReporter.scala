package spark

import akka.actor.Actor
import akka.actor.Actor._
import akka.actor.ActorRef
import java.io.{FileOutputStream, FileNotFoundException}

sealed trait EventReporterMessage
case class ReportException(exception: Throwable) extends EventReporterMessage
case class ReportRDDCreation(rdd: RDD[_]) extends EventReporterMessage
case class ReportRDDChecksum(rdd: RDD[_], split: Split, checksum: Int) extends EventReporterMessage

class EventReporterActor extends Actor with Logging {
  val eventLog =
    try {
      Some(new FileOutputStream(System.getProperty("spark.logging.eventLog"), true))
    } catch {
      case e: FileNotFoundException =>
        logWarning("Can't write to %s: %s".format(System.getProperty("spark.logging.eventLog"), e))
        None
    }

  def receive = {
    case ReportException(exception) =>
      logInfo("Received exception: %s".format(exception))

    case ReportRDDCreation(rdd) =>
      for (l <- eventLog) {
        // TODO: avoid serializing the same RDD once for each time it's referenced as a dependency
        l.write(Utils.serialize(rdd))
      }

    case ReportRDDChecksum(rdd, split, checksum) =>
      logInfo("Received checksum for split %d of RDD %s: %d".format(split.index, rdd.id, checksum))
  }
}

class EventReporter(isMaster: Boolean) extends Logging {
  val host = System.getProperty("spark.master.host")
  val port = System.getProperty("spark.master.akkaPort").toInt

  // Remote reference to the actor on workers
  var reporterActor: ActorRef = {
    if (isMaster) {
      remote.start(host, port).register("EventReporter", actorOf[EventReporterActor])
    }
    remote.actorFor("EventReporter", host, port)
  }

  def reportException(exception: Throwable) {
    reporterActor ! ReportException(exception)
  }

  def reportRDDCreation(rdd: RDD[_]) {
    reporterActor ! ReportRDDCreation(rdd)
  }

  def reportRDDChecksum(rdd: RDD[_], split: Split, checksum: Int) {
    reporterActor ! ReportRDDChecksum(rdd, split, checksum)
  }
}
