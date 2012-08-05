package spark.debugger

import java.io._

import scala.collection.JavaConversions._
import scala.collection.mutable

import spark.Logging
import spark.SparkContext
import spark.RDD
import spark.scheduler.Task
import spark.scheduler.ResultTask
import spark.scheduler.ShuffleMapTask
import spark.ShuffleDependency

/**
 * Reads events from an event log on disk and processes them.
 */
class EventLogReader(sc: SparkContext, eventLogPath: Option[String] = None) extends Logging {
  val objectInputStream = for {
    elp <- eventLogPath orElse { Option(System.getProperty("spark.debugger.logPath")) }
    file = new File(elp)
    if file.exists
  } yield new EventLogInputStream(new FileInputStream(file), sc)

  val events = new mutable.ArrayBuffer[EventLogEntry]

  /** List of RDDs indexed by their canonical ID. */
  private val _rdds = new mutable.ArrayBuffer[RDD[_]]

  /** Map of RDD ID to canonical RDD ID (reverse of _rdds). */
  private val rddIdToCanonical = new mutable.HashMap[Int, Int]
  loadNewEvents()

  val checksumVerifier = new ChecksumVerifier

  // Receive new events as they occur
  sc.env.eventReporter.subscribe(addEvent _)

  /** List of RDDs from the event log, indexed by their IDs. */
  def rdds = _rdds.readOnly

  /** List of checksum mismatches. */
  def checksumMismatches: Seq[ChecksumEvent] = checksumVerifier.mismatches

  /** Prints a human-readable list of RDDs. */
  def printRDDs() {
    for (RDDCreation(rdd, location) <- events) {
      println("#%02d: %-20s %s".format(rdd.id, rddType(rdd), firstExternalElement(location)))
    }
  }

  /** Returns the path of a PDF file containing a visualization of the RDD graph. */
  def visualizeRDDs(): String = {
    val file = File.createTempFile("spark-rdds-", "")
    val dot = new java.io.PrintWriter(file)
    dot.println("digraph {")
    for (RDDCreation(rdd, location) <- events) {
      dot.println("  %d [label=\"%d %s\"]".format(rdd.id, rdd.id, rddType(rdd)))
      for (dep <- rdd.dependencies) {
        dot.println("  %d -> %d;".format(rdd.id, dep.rdd.id))
      }
    }
    dot.println("}")
    dot.close()
    Runtime.getRuntime.exec("dot -Grankdir=BT -Tpdf " + file + " -o " + file + ".pdf")
    file + ".pdf"
  }

  /** List of all tasks. */
  def tasks: Seq[Task[_]] =
    for {
      TaskSubmission(tasks) <- events
      task <- tasks
    } yield task


  /** Finds the tasks that were run to compute the given RDD. */
  def tasksForRDD(rdd: RDD[_]): Seq[Task[_]] =
    for {
      task <- tasks
      taskRDD <- task match {
        case rt: ResultTask[_, _] => Some(rt.rdd)
        case smt: ShuffleMapTask => Some(smt.rdd)
        case _ => None
      }
      if taskRDD.id == rdd.id
    } yield task

  /** Finds the task for the given stage ID and partition. */
  def taskWithId(stageId: Int, partition: Int): Option[Task[_]] =
    (for {
      task <- tasks
      (taskStageId, taskPartition) <- task match {
        case rt: ResultTask[_, _] => Some((rt.stageId, rt.partition))
        case smt: ShuffleMapTask => Some((smt.stageId, smt.partition))
        case _ => None
      }
      if taskStageId == stageId && taskPartition == partition
    } yield task).headOption

  /** Reads any new events from the event log. */
  def loadNewEvents() {
    for (ois <- objectInputStream) {
      try {
        while (true) {
          val event = ois.readObject.asInstanceOf[EventLogEntry]
          addEvent(event)

          event match {
            case c: ChecksumEvent => checksumVerifier.verify(c)
            case _ => {}
          }
        }
      } catch {
        case e: EOFException => {}
      }
    }
  }

  private def addEvent(event: EventLogEntry) {
    events += event
    event match {
      case RDDCreation(rdd, location) =>
        sc.updateRddId(rdd.id)
        for (dep <- rdd.dependencies) dep match {
          case shufDep: ShuffleDependency[_,_,_] =>
            sc.updateShuffleId(shufDep.shuffleId)
          case _ => {}
        }
        _rdds += rdd
        rddIdToCanonical(rdd.id) = rdd.id
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
