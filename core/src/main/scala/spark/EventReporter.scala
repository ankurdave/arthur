package spark

import akka.actor.Actor
import akka.actor.Actor._
import akka.actor.ActorRef

sealed trait EventReporterMessage
case class ReportException(exception: Throwable) extends EventReporterMessage
case class ReportRDDCreation(rdd: RDD[_]) extends EventReporterMessage
case class ReportRDDChecksum(rdd: RDD[_], split: Split, checksum: Int) extends EventReporterMessage

class EventReporterActor extends Actor with Logging {
  def receive = {
    case ReportException(exception) =>
      logInfo("Received exception: %s".format(exception))

    case ReportRDDCreation(rdd) =>
      logInfo("Received RDD creation: %s".format(rdd))

    case ReportRDDChecksum(rdd, split, checksum) =>
      logInfo("Received checksum for split %d of RDD %s: %d".format(split.index, rdd, checksum))
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
