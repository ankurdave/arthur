package spark.debugger

import java.io._

import scala.collection.JavaConversions._
import scala.collection.mutable

import spark.Logging
import spark.RDD
import spark.ShuffleDependency
import spark.SparkContext
import spark.scheduler.ResultTask
import spark.scheduler.ShuffleMapTask
import spark.scheduler.Task

/**
 * Reads events from an event log and provides replay debugging.
 */
class EventLogReader(sc: SparkContext, eventLogPath: Option[String] = None) extends Logging {
  private val objectInputStream = for {
    elp <- eventLogPath orElse { Option(System.getProperty("spark.debugger.logPath")) }
    file = new File(elp)
    if file.exists
  } yield new EventLogInputStream(new FileInputStream(file), sc)

  private val events_ = new mutable.ArrayBuffer[EventLogEntry]
  private val checksumVerifier = new ChecksumVerifier
  private val rdds = new mutable.HashMap[Int, RDD[_]]

  // Receive new events as they occur
  sc.env.eventReporter.subscribe(addEvent _)

  loadNewEvents()

  /** Looks up an RDD by ID. */
  def rdd(id: Int): RDD[_] = rdds(id)

  /** Set of RDD IDs. */
  def rddIds: scala.collection.Set[Int] = rdds.keySet

  /** Sequence of events in the event log. */
  def events: Seq[EventLogEntry] = events_.readOnly

  /** List of checksum mismatches. */
  def checksumMismatches: Seq[ChecksumEvent] = checksumVerifier.mismatches

  /** Prints a human-readable list of RDDs. */
  def printRDDs() {
    for (RDDRegistration(rdd) <- events) {
      println("#%02d: %-20s %s".format(
        rdd.id, rddType(rdd), firstExternalElement(rdd.creationLocation)))
    }
  }

  /** Reads any new events from the event log. */
  def loadNewEvents() {
    for (ois <- objectInputStream) {
      try {
        while (true) {
          val event = ois.readObject.asInstanceOf[EventLogEntry]
          addEvent(event)
        }
      } catch {
        case e: EOFException => {}
      }
    }
  }

  private def addEvent(event: EventLogEntry) {
    events_ += event
    event match {
      case RDDRegistration(rdd) =>
        // TODO(ankurdave): Check that the RDD ID and shuffle IDs aren't already in use. This may
        // happen if the EventLogReader is passed a SparkContext that has previously been used for
        // some computation.
        sc.updateRddId(rdd.id)
        for (dep <- rdd.dependencies) dep match {
          case shufDep: ShuffleDependency[_,_,_] =>
            sc.updateShuffleId(shufDep.shuffleId)
          case _ => {}
        }
        rdds(rdd.id) = rdd
      case c: ChecksumEvent =>
        checksumVerifier.verify(c)
      case _ => {}
    }
  }

  private def firstExternalElement(location: Array[StackTraceElement]) =
    (location.tail.find(!_.getClassName.matches("""spark\.[A-Z].*"""))
      orElse { location.headOption }
      getOrElse { "" })

  private def rddType(rdd: RDD[_]): String =
    rdd.getClass.getName.replaceFirst("""^spark\.""", "")
}
