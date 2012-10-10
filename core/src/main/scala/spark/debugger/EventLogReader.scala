package spark.debugger

import java.io._

import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import spark.Logging
import spark.RDD
import spark.Dependency
import spark.ShuffleDependency
import spark.SparkContext
import spark.scheduler.ResultTask
import spark.scheduler.ShuffleMapTask
import spark.scheduler.Task

/**
 * Reads events from an event log and provides replay debugging.
 */
class EventLogReader(sc: SparkContext, eventLogPath: Option[String] = None) extends Logging {
  private val events_ = new ArrayBuffer[EventLogEntry]
  private val checksumVerifier = new ChecksumVerifier
  private val rdds = new mutable.HashMap[Int, RDD[_]]

  private def getEventLogPath(): String =
    eventLogPath orElse { Option(System.getProperty("spark.debugger.logPath")) } match {
      case Some(elp) => elp
      case None => throw new UnsupportedOperationException("No event log path provided")
    }
  private var objectInputStream: EventLogInputStream = {
    val file = new File(getEventLogPath())
    if (file.exists) {
      new EventLogInputStream(new FileInputStream(file), sc)
    } else {
      throw new UnsupportedOperationException("Event log %s does not exist")
    }
  }
  loadNewEvents()

  // Receive new events as they occur
  sc.env.eventReporter.subscribe(addEvent _)

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
    logDebug("Loading new events from " + getEventLogPath())
    try {
      while (true) {
        val event = objectInputStream.readObject.asInstanceOf[EventLogEntry]
        addEvent(event)
      }
    } catch {
      case e: EOFException => {}
    }
  }

  /**
   * Selects the elements in startRDD that match p, traces them forward until endRDD, and returns
   * the resulting members of endRDD.
   */
  def traceForward[T, U: ClassManifest](
      startRDD: RDD[T], p: T => Boolean, endRDD: RDD[U]): RDD[U] = {
    val taggedEndRDD: RDD[Tagged[U]] = tagRDD[U, T](
      endRDD, startRDD, startRDD.map((t: T) => Tagged(t, BooleanTag(p(t)))))
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
      startRDD: RDD[T],
      p: U => Boolean,
      endRDD: RDD[U]): RDD[T] = {
    if (endRDD.id == startRDD.id) {
      // Casts between RDD[U] and RDD[T] are legal because startRDD is the same as endRDD, so T is
      // the same as U
      startRDD.asInstanceOf[RDD[U]].filter(p).asInstanceOf[RDD[T]]
    } else {
      val (taggedEndRDD, firstRDDInStage) = tagRDDWithinStage(
        endRDD, startRDD, getParentStageRDDs(endRDD))
      // TODO: find the set of partitions of endRDD that contain elements that match p
      val tags = sc.broadcast(taggedEndRDD.filter(tu => p(tu.elem)).map(tu => tu.tag)
        .fold(IntSetTag.empty)(_ union _))
      val sourceElems = new UniquelyTaggedRDD(firstRDDInStage)
        .filter(taggedElem => (tags.value intersect taggedElem.tag).isTagged)
        .map((tx: Tagged[_]) => tx.elem).collect()
      // Casting from RDD[_] to RDD[Any] is legal because RDD is essentially covariant
      traceBackward[T, Any](
        startRDD,
        (x: Any) => sourceElems.contains(x),
        firstRDDInStage.asInstanceOf[RDD[Any]])
    }
  }

  /**
   * Traces the given element elem from endRDD backward until startRDD and returns the resulting
   * members of startRDD.
   */
  def traceBackward[T: ClassManifest, U: ClassManifest](
      startRDD: RDD[T], elem: U, endRDD: RDD[U]): RDD[T] =
    traceBackward(startRDD, { (x: U) => x == elem }, endRDD)

  private def tagRDDWithinStage[A, T](
      rdd: RDD[A],
      startRDD: RDD[T],
      parentStageRDDs: Set[RDD[_]]): (RDD[Tagged[A]], RDD[_]) = {
    if (!rddPathExists(startRDD, rdd)) {
      (rdd.map(elem => Tagged(elem, IntSetTag.empty)), startRDD)
    } else if (rdd.id == startRDD.id || parentStageRDDs.contains(rdd)) {
      (new UniquelyTaggedRDD(rdd), rdd)
    } else {
      val dependencyResults = new ArrayBuffer[RDD[_]]
      val taggedRDD = rdd.tagged(new RDDTagger {
        def apply[B](prev: RDD[B]): RDD[Tagged[B]] = {
          val (taggedPrev, firstRDDInStage) =
            tagRDDWithinStage[B, T](prev, startRDD, parentStageRDDs)
          dependencyResults += firstRDDInStage
          taggedPrev
        }
      })
      (taggedRDD, dependencyResults.max(new Ordering[RDD[_]] {
        def compare(x: RDD[_], y: RDD[_]): Int = x.id - y.id
      }))
    }
  }

  private def rddPathExists(startRDD: RDD[_], endRDD: RDD[_]): Boolean = {
    if (startRDD.id == endRDD.id) {
      true
    } else {
      (for (dep <- endRDD.dependencies; rdd = dep.rdd) yield rdd).foldLeft(false) {
        (acc, rdd) => acc || rddPathExists(startRDD, rdd)
      }
    }
  }

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

  /** Takes an RDD and returns a set of RDDs representing the parent stages. */
  private def getParentStageRDDs(rdd: RDD[_]): Set[RDD[_]] = {
    val ancestorDeps = new mutable.HashSet[Dependency[_]]
    val visited = new mutable.HashSet[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_,_,_] =>
              ancestorDeps.add(shufDep)
            case _ =>
              visit(dep.rdd)
          }
        }
      }
    }
    visit(rdd)
    // toSet is necessary because for some reason Scala doesn't think a mutable.HashSet[RDD[_]] is a
    // Set[RDD[_]]
    ancestorDeps.map(_.rdd).toSet
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
        if (rdd.dependencies != null) {
          rdd.dependencies.collect {
            case shufDep: ShuffleDependency[_,_,_] => shufDep.shuffleId
          } match {
            case Seq() => {}
            case shuffleIds =>
              val maxShuffleId = shuffleIds.max
              logDebug("Updating shuffle ID to be greater than " + maxShuffleId)
              sc.updateShuffleId(maxShuffleId)
          }
        } else {
          logError("Dependency list for RDD %d (%s) is null".format(rdd.id, rdd))
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
