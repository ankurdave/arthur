package spark.debugger

import java.io._

import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

  private val events_ = new ArrayBuffer[EventLogEntry]
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
    logDebug("Loading new events from " + eventLogPath)
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

  /**
   * Selects the elements in startRDD that match p, traces them forward until endRDD, and returns
   * the resulting members of endRDD.
   */
  def traceForward[T, U: ClassManifest](
      startRDD: RDD[T], p: T => Boolean, endRDD: RDD[U]): RDD[U] = {
    val taggedEndRDD: RDD[Tagged[U]] = tagRDD[U, T](
      endRDD, startRDD, startRDD.map((t: T) => Tagged(t, new BooleanTag(p(t)))))
    taggedEndRDD.filter(tu => tu.tag.isTagged).map(tu => tu.elem)
  }

  /**
   * Traces the given element elem from startRDD forward until endRDD and returns the resulting
   * members of endRDD.
   */
  def traceForward[T, U: ClassManifest](startRDD: RDD[T], elem: T, endRDD: RDD[U]): RDD[U] =
    traceForward(startRDD, { (x: T) => x == elem }, endRDD)

  /**
   * Selects the elements in endRDD that match p, traces them backward until startRDD, and returns
   * the resulting members of startRDD.
   */
  def traceBackward[T: ClassManifest, U: ClassManifest](
      startRDD: RDD[T], p: U => Boolean, endRDD: RDD[U]): RDD[T] = {
    val taggedEndRDD: RDD[Tagged[U]] = tagRDD[U, T](
      endRDD, startRDD, tagElements(startRDD, (t: T) => true))
    val tags = sc.broadcast(
      taggedEndRDD.filter(tu => p(tu.elem)).map(tu => tu.tag).fold(new IntSetTag())(_ union _))
    val taggedStartRDD = new UniquelyTaggedRDD(startRDD)
    taggedStartRDD.filter(tt => (tags.value intersect tt.tag).isTagged).map(tt => tt.elem)
  }

  /**
   * Traces the given element elem from endRDD backward until startRDD and returns the resulting
   * members of startRDD.
   */
  def traceBackward[T: ClassManifest, U: ClassManifest](
      startRDD: RDD[T], elem: U, endRDD: RDD[U]): RDD[T] =
    traceBackward(startRDD, { (x: U) => x == elem }, endRDD)

  private def tagRDD[A, T](
      rdd: RDD[A],
      startRDD: RDD[T],
      taggedStartRDD: RDD[Tagged[T]]): RDD[Tagged[A]] = {
    if (rdd.id == startRDD.id) {
      // (rdd: RDD[A]) is the same as (startRDD: RDD[T]), so T is the same as A, so we can cast
      // RDD[Tagged[T]] to RDD[Tagged[A]]
      taggedStartRDD.asInstanceOf[RDD[Tagged[A]]]
    } else {
      rdd.tagged(new RDDTagger {
        def apply[B](prev: RDD[B]): RDD[Tagged[B]] = {
          tagRDD[B, T](prev, startRDD, taggedStartRDD)
        }
      })
    }
  }

  private def tagElements[T](rdd: RDD[T], p: T => Boolean): RDD[Tagged[T]] = {
    new UniquelyTaggedRDD(rdd).map {
      case Tagged(elem, tag) => Tagged(elem, if (p(elem)) tag else IntSetTag.empty)
    }
  }

  private def addEvent(event: EventLogEntry) {
    events_ += event
    event match {
      case RDDRegistration(rdd) =>
        // TODO(ankurdave): Check that the RDD ID and shuffle IDs aren't already in use. This may
        // happen if the EventLogReader is passed a SparkContext that has previously been used for
        // some computation.
        logDebug("Updating RDD ID to be greater than " + rdd.id)
        sc.updateRddId(rdd.id)
        rdd.dependencies.collect {
          case shufDep: ShuffleDependency[_,_,_] => shufDep.shuffleId
        } match {
          case Seq() => {}
          case shuffleIds =>
            val maxShuffleId = shuffleIds.max
            logDebug("Updating shuffle ID to be greater than " + maxShuffleId)
            sc.updateShuffleId(maxShuffleId)
        }
        rdds(rdd.id) = rdd
      case c: ChecksumEvent =>
        checksumVerifier.verify(c)
      case t: TaskSubmission =>
        t.tasks.map(_.stageId) match {
          case Seq() => {}
          case stageIds =>
            val maxStageId = stageIds.max
            logDebug("Updating stage ID to be greater than " + maxStageId)
            sc.updateStageId(maxStageId)
        }
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
