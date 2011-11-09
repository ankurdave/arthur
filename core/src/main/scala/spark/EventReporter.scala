package spark

import akka.actor.Actor
import akka.actor.Actor._
import akka.actor.ActorRef
import akka.dispatch.MessageDispatcher
import java.io._

sealed trait EventReporterMessage
case class ReportException(exception: Throwable) extends EventReporterMessage
case class ReportRDDCreation(rdd: RDD[_]) extends EventReporterMessage
case class ReportParallelCollectionCreation[T](data: Seq[T], numSlices: Int) extends EventReporterMessage
case class ReportRDDChecksum(rdd: RDD[_], split: Split, checksum: Int) extends EventReporterMessage

class EventReporterActor(dispatcher: MessageDispatcher) extends Actor with Logging {
  self.dispatcher = dispatcher

  val eventLog =
    try {
      val file = new File(System.getProperty("spark.logging.eventLog"))
      if (!file.exists) {
        Some(new ObjectOutputStream(new FileOutputStream(file)))
      } else {
        logWarning("Event log %s already exists".format(System.getProperty("spark.logging.eventLog")))
        None
      }
    } catch {
      case e: FileNotFoundException =>
        logWarning("Can't write to %s: %s".format(System.getProperty("spark.logging.eventLog"), e))
        None
    }

  def receive = {
    case ReportRDDCreation(rdd) => rdd match {
      case pc: ParallelCollection[_] =>
        for (l <- eventLog)
          l.writeObject(ReportParallelCollectionCreation(pc.data, pc.numSlices))
      case _ =>
        for (l <- eventLog)
          l.writeObject(ReportRDDCreation(rdd))
    }

    case otherMsg: EventReporterMessage =>
      for (l <- eventLog) l.writeObject(otherMsg)
  }
}

class EventReporter(isMaster: Boolean) extends Logging {
  val host = System.getProperty("spark.master.host")
  val port = System.getProperty("spark.master.akkaPort").toInt

  // Remote reference to the actor on workers
  var reporterActor: ActorRef = {
    if (isMaster) {
      val dispatcher = new DaemonDispatcher("mydispatcher")
      remote.start(host, port).register("EventReporter", actorOf(new EventReporterActor(dispatcher)))
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
