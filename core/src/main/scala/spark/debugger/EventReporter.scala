package spark.debugger

import java.io._

import scala.util.MurmurHash
import scala.collection.mutable.ArrayBuffer

import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.dispatch.Await

import spark.SparkException
import spark.Logging
import spark.scheduler.Task
import spark.RDD
import spark.scheduler.TaskResult
import spark.scheduler.ResultTask
import spark.scheduler.ShuffleMapTask
import spark.Utils

sealed trait EventReporterMessage
case class LogEvent(entry: EventLogEntry) extends EventReporterMessage
case class StopEventReporter() extends EventReporterMessage

class EventReporterActor(eventLogWriter: EventLogWriter) extends Actor with Logging {
  def receive = {
    case LogEvent(entry) =>
      eventLogWriter.log(entry)

    case StopEventReporter =>
      logInfo("Stopping EventReporterActor")
      sender ! true
      context.stop(self)
  }
}

trait EventReporter {
  // Reports an exception when running a task from a slave.
  def reportException(exception: Throwable, task: Task[_])
  // Reports an exception when running a task locally using LocalScheduler. Can only be called from
  // the master.
  def reportLocalException(exception: Throwable, task: Task[_])
  // Reports the creation of an RDD. Can only be called from the master.
  def registerRDD(rdd: RDD[_])
  // Reports the creation of a task. Can only be called from the master.
  def registerTasks(tasks: Seq[Task[_]])
  // Reports the checksum of a task's results.
  def reportTaskChecksum(task: Task[_], result: TaskResult[_], serializedResult: Array[Byte])
  // Reports the checksum of a block, which is typically created as the output of a task.
  def reportBlockChecksum(blockId: String, blockBytes: Array[Byte])
  // Allows subscription to event as they are logged. Can only be called from the master.
  def subscribe(callback: EventLogEntry => Unit)
}

class MockEventReporter extends EventReporter {
  def reportException(exception: Throwable, task: Task[_]) {}
  def reportLocalException(exception: Throwable, task: Task[_]) {}
  def registerRDD(rdd: RDD[_]) {}
  def registerTasks(tasks: Seq[Task[_]]) {}
  def reportTaskChecksum(task: Task[_], result: TaskResult[_], serializedResult: Array[Byte]) {}
  def reportBlockChecksum(blockId: String, blockBytes: Array[Byte]) {}
  def subscribe(callback: EventLogEntry => Unit) {}
}

/**
 * Manages event reporting on the master and slaves.
 */
class ActorBasedEventReporter(
  actorSystem: ActorSystem, isMaster: Boolean) extends EventReporter with Logging {

  val ip: String = System.getProperty("spark.master.host", "localhost")
  val port: Int = System.getProperty("spark.master.port", "7077").toInt
  val actorName: String = "EventReporter"
  val enableChecksumming = System.getProperty("spark.debugger.checksum", "true").toBoolean
  val timeout = 10.seconds
  var eventLogWriter: Option[EventLogWriter] =
    if (isMaster) {
      Some(new EventLogWriter)
    } else {
      None
    }
  val subscribers = new ArrayBuffer[EventLogEntry => Unit]

  // Remote reference to the actor on workers.
  var reporterActor: ActorRef = if (isMaster) {
    val actor = actorSystem.actorOf(
      Props(new EventReporterActor(eventLogWriter.get)), name = actorName)
    logInfo("Registered EventReporterActor actor")
    actor
  } else {
    val url = "akka://spark@%s:%s/user/%s".format(ip, port, actorName)
    actorSystem.actorFor(url)
  }

  override def reportException(exception: Throwable, task: Task[_]) {
    // TODO: The task may refer to an RDD, so sending it through the actor will interfere with RDD
    // back-referencing, causing a duplicate version of the referenced RDD to be serialized. If
    // tasks had IDs, we could just send those.
    report(LogEvent(ExceptionEvent(exception, task)))
  }

  override def reportLocalException(exception: Throwable, task: Task[_]) {
    report(ExceptionEvent(exception, task))
  }

  override def registerRDD(rdd: RDD[_]) {
    report(RDDCreation(rdd, rdd.creationLocation))
  }

  override def registerTasks(tasks: Seq[Task[_]]) {
    report(TaskSubmission(tasks))
  }

  override def reportTaskChecksum(
      task: Task[_],
      result: TaskResult[_],
      serializedResult: Array[Byte]) {
    if (enableChecksumming) {
      val checksum = new MurmurHash[Byte](42) // constant seed so checksum is reproducible
      task match {
        case rt: ResultTask[_,_] =>
          for (byte <- serializedResult) checksum(byte)
          val serializedFunc = Utils.serialize(rt.func)
          val funcChecksum = new MurmurHash[Byte](42)
          for (byte <- serializedFunc) funcChecksum(byte)
          report(LogEvent(ResultTaskChecksum(
            rt.rdd.id, rt.partition, funcChecksum.hash, checksum.hash)))
        case smt: ShuffleMapTask =>
          // Don't serialize the output of a ShuffleMapTask, only its
          // accumulator updates. The output is a URI that may change.
          val serializedAccumUpdates = Utils.serialize(result.accumUpdates)
          for (byte <- serializedAccumUpdates) checksum(byte)
          report(LogEvent(ShuffleMapTaskChecksum(smt.rdd.id, smt.partition, checksum.hash)))
        case _ =>
          logWarning("unknown task type: " + task)
      }
    }
  }

  override def reportBlockChecksum(blockId: String, blockBytes: Array[Byte]) {
    if (enableChecksumming) {
      val blockChecksum = new MurmurHash[Byte](42)
      for (byte <- blockBytes) blockChecksum(byte)
      report(LogEvent(BlockChecksum(blockId, blockChecksum.hash)))
    }
  }

  override def subscribe(callback: EventLogEntry => Unit) {
    subscribers.append(callback)
  }

  // Stops the reporter actor and the event log writer.
  def stop() {
    if (askReporter(StopEventReporter) != true) {
      throw new SparkException("Error reply received from EventReporter")
    }
    for (elw <- eventLogWriter) {
      elw.stop()
    }
    eventLogWriter = None
    reporterActor = null
  }

  // Used for reporting from either the master or a slave.
  private def report(message: EventReporterMessage) {
    reporterActor.tell(message)
  }

  // Used only for reporting from the master.
  private def report(entry: EventLogEntry) {
    for (elw <- eventLogWriter) {
      elw.log(entry)
      for (s <- subscribers) {
        s(entry)
      }
    }
  }

  // Send a message to the reporterActor and get its result within a default timeout, or
  // throw a SparkException if this fails.
  private def askReporter(message: Any): Any = {
    try {
      val future = reporterActor.ask(message)(timeout)
      return Await.result(future, timeout)
    } catch {
      case e: Exception =>
        throw new SparkException("Error communicating with EventReporter", e)
    }
  }
}
