package spark

import akka.actor.Actor
import akka.actor.Actor._
import akka.actor.ActorRef

sealed trait EventReporterMessage
case class ReportException(exception: Throwable) extends EventReporterMessage
case object StopEventReporter extends EventReporterMessage

class EventReporterActor extends Actor with Logging {
  def receive = {
    case ReportException(exception: Throwable) =>
      logInfo("Received exception: %s".format(exception))

    case StopEventReporter =>
      self.reply('OK)
      exit()
  }
}

class EventReporter(isMaster: Boolean) extends Logging {
  val host = System.getProperty("spark.master.host")
  val port = System.getProperty("spark.master.akkaPort").toInt

  // Remote reference to the actor on workers
  var reporterActor: Option[ActorRef] =
    if (isMaster) {
      remote.start(host, port).register("EventReporter", actorOf[EventReporterActor])
      // TODO: need to unregister this actor; see register() in
      // http://akka.io/api/akka/1.2/akka/remoteinterface/RemoteServerModule.html
      None
    } else {
      Some(remote.actorFor("EventReporter", host, port))
    }

  def reportException(exception: Throwable) {
    for (actor <- reporterActor)
      actor ! ReportException(exception)
  }

  def stop() {
    for (actor <- reporterActor)
      actor ! StopEventReporter
    reporterActor = None
  }
}
