package spark

import com.google.common.io.Files
import java.io._
import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import spark.SparkContext._
import spark.Tagged._

/**
 * Reads events from an event log on disk and processes them.
 */
class EventLogReader(sc: SparkContext, eventLogPath: Option[String] = None) extends Logging {
  val objectInputStream = for {
    elp <- eventLogPath orElse { Option(System.getProperty("spark.arthur.logPath")) }
    file = new File(elp)
    if file.exists
  } yield new EventLogInputStream(new FileInputStream(file), sc)
  val events = new ArrayBuffer[EventLogEntry]
  /** List of RDDs indexed by their canonical ID. */
  private val _rdds = new ArrayBuffer[RDD[_]]
  /** Map of RDD ID to canonical RDD ID (reverse of _rdds). */
  private val rddIdToCanonical = new mutable.HashMap[Int, Int]
  loadNewEvents()

  // Receive new events directly from EventLogWriter, as they occur
  for (w <- sc.env.eventReporter.eventLogWriter)
    w.registerEventLogReader(this)

  /** List of RDDs from the event log, indexed by their IDs. */
  def rdds = _rdds.readOnly

  /** List of checksum mismatches. */
  def checksumMismatches: Seq[ChecksumEvent] =
    for (w <- sc.env.eventReporter.eventLogWriter.toList; m <- w.checksumMismatches) yield m

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

  /**
   * Inserts a lazily-checked element assertion on the specific RDD into the RDD graph. Returns the
   * RDD with the assertion applied.
   *
   * The given RDD, and any RDDs that depend on it, will be replaced. Make sure to get the new
   * version of all RDDs using rdds.
   */
  def assert[T: ClassManifest](rdd: RDD[T], assertion: T => Boolean): RDD[T] = {
    val rddId = rdd.id
    val newRDD = new ElementAssertionRDD(rdd, { (x: T, _: Split) =>
      if (!assertion(x)) Some(ElementAssertionFailure(rddId, x))
      else None
    })
    replace(rdd, newRDD)
    newRDD
  }

  /**
   * Inserts a lazily-checked reduce assertion on the specific RDD into the RDD graph. Returns the
   * RDD with the assertion applied. The reducer operates on each partition independently, and only
   * checks the assertion after the entire partition has been recomputed.
   *
   * The given RDD, and any RDDs that depend on it, will be replaced. Make sure to get the new
   * version of all RDDs using rdds.
   */
  def assert[T: ClassManifest](rdd: RDD[T], reducer: (T, T) => T, assertion: T => Boolean): RDD[T] = {
    // After the given RDD, insert a transformation that checks the assertion
    val rddId = rdd.id
    val newRDD = new ReduceAssertionRDD(rdd, reducer, { (x: T, split: Split) =>
      if (!assertion(x)) Some(ReduceAssertionFailure(rddId, split.index, x))
      else None
    })
    replace(rdd, newRDD)
    newRDD
  }

  def traceForward[T, U: ClassManifest](startRDD: RDD[T], p: T => Boolean, endRDD: RDD[U]): RDD[U] = {
    val taggedRDDs = tagAllRDDs { (origRDD, taggedRDD) =>
      if (origRDD.id == startRDD.id) {
        tagElements(startRDD, p).asInstanceOf[RDD[Tagged[_]]]
      } else {
        taggedRDD
      }
    }

    taggedRDDs(endRDD.id).asInstanceOf[RDD[Tagged[U]]].filter(tu => tu.tag.nonEmpty).map(tu => tu.elem)
  }

  def traceForward[T, U: ClassManifest](startRDD: RDD[T], elem: T, endRDD: RDD[U]): RDD[U] =
    traceForward(startRDD, { (x: T) => x == elem }, endRDD)

  def traceBackward[T: ClassManifest, U: ClassManifest](startRDD: RDD[T], p: U => Boolean, endRDD: RDD[U]): RDD[T] = {
    val taggedStartRDD = new UniquelyTaggedRDD(startRDD)
    val taggedRDDs: Seq[RDD[Tagged[_]]] = tagAllRDDs {
      (origRDD, taggedRDD) =>
        if (origRDD.id == startRDD.id) taggedStartRDD.asInstanceOf[RDD[Tagged[_]]]
        else taggedRDD
    }
    val taggedEndRDD = taggedRDDs(endRDD.id).asInstanceOf[RDD[Tagged[U]]]

    // For each element in elementsInEndRDD, find it in taggedEndRDD, and extract its tag
    val tags: Seq[TagSet] = taggedEndRDD.filter(tu => p(tu.elem)).map(tu => tu.tag).collect()
    val tagsBroadcast = sc.broadcast(tags)

    // For each tag in tags, find the element with that tag in taggedStartRDD, and extract its element
    taggedStartRDD.filter(tt => {
      tagsBroadcast.value.foldLeft(false) {
        (acc, tag) => acc || tag.intersection(tt.tag).nonEmpty
      }
    }).map(tt => tt.elem)
  }

  def traceBackward[T: ClassManifest, U: ClassManifest](startRDD: RDD[T], elem: U, endRDD: RDD[U]): RDD[T] =
    traceBackward(startRDD, { (x: U) => x == elem }, endRDD)

  private def replaceParent[T](a: RDD[T], b: RDD[Tagged[T]], taggedRDDs: Seq[RDD[Tagged[_]]]) = new RDDTagger {
    def apply[A](prev: RDD[A]): RDD[Tagged[A]] =
      if (prev.id == a.id) b.asInstanceOf[RDD[Tagged[A]]]
      else taggedRDDs(prev.id).asInstanceOf[RDD[Tagged[A]]]
  }

  private def tagElements[T](rdd: RDD[T], p: T => Boolean): RDD[Tagged[T]] = {
    new UniquelyTaggedRDD(rdd).map {
      case Tagged(elem, tag) => Tagged(elem, if (p(elem)) tag else Tagged.TagSet())
    }
  }

  private def rddPath(startRDD: RDD[_], endRDD: RDD[_]): Option[List[RDD[_]]] = {
    if (startRDD.id == endRDD.id) {
      Some(List(endRDD))
    } else {
      for (dep <- endRDD.dependencies; rdd = dep.rdd) {
        rddPath(startRDD, rdd) match {
          case Some(restOfPath) => return Some(endRDD :: restOfPath)
          case None => {}
        }
      }
      None
    }
  }

  private def tagAllRDDs(f: (RDD[_], RDD[Tagged[_]]) => RDD[Tagged[_]]): ArrayBuffer[RDD[Tagged[_]]] = {
    val taggedRDDs = new ArrayBuffer[RDD[Tagged[_]]]
    for (rdd <- _rdds) {
      try {
        val taggedRDD = rdd.tagged(new RDDTagger {
          def apply[A](prev: RDD[A]): RDD[Tagged[A]] = {
            if (taggedRDDs(prev.id) == null) {
              throw new UnsupportedOperationException("not tagging the child of an untaggable RDD")
            } else {
              taggedRDDs(prev.id).asInstanceOf[RDD[Tagged[A]]]
            }
          }
        })
        taggedRDDs += f(rdd, taggedRDD.asInstanceOf[RDD[Tagged[_]]])
      } catch {
        case _: UnsupportedOperationException =>
          taggedRDDs += null
      }
    }
    taggedRDDs
  }

  /**
   * Runs the specified task locally in a new JVM with the given options, and blocks until the task
   * has completed. While the task is running, it takes over the input and output streams.
   */
  def debugTask(taskStageId: Int, taskPartition: Int, debugOpts: Option[String] = None) {
    for {
      elp <- eventLogPath orElse { Option(System.getProperty("spark.arthur.logPath")) }
      sparkHome <- Option(sc.sparkHome) orElse { Option("") }
      task <- taskWithId(taskStageId, taskPartition)
      (rdd, partition) <- task match {
        case rt: ResultTask[_, _] => Some((rt.rdd, rt.partition))
        case smt: ShuffleMapTask => Some((smt.rdd, smt.partition))
        case _ => None
      }
      debugOptsString <- debugOpts orElse {
        Option("-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000")
      }
    } try {
      // Precompute the parent stage RDDs so the task will be able to run immediately
      val parentStageDeps = getParentStageDeps(rdd)
      if (parentStageDeps.nonEmpty) {
        val parentStageRddIds = parentStageDeps.map(_.rdd.id)
        logInfo("Precomputing parent stages (RDD %s) for %s".format(parentStageRddIds.mkString(", "), task))
        for (dep <- parentStageDeps) new DummyShuffledRDD(dep).foreach(x => {})
      }

      val tempDir = Files.createTempDir()
      val file = new File(tempDir, "debugTask-%d-%d".format(taskStageId, taskPartition))
      logInfo("Serializing task %s into %s".format(task, file))
      val ser = sc.env.serializer.newInstance()
      val out = ser.outputStream(new BufferedOutputStream(new FileOutputStream(file)))
      out.writeObject(task)
      out.close()

      // Launch the task in a separate JVM with debug options set
      logInfo("Running task " + task)
      val pb = new ProcessBuilder(List("./run", "spark.DebuggingTaskRunner", file.getPath))
      val propertiesToCopy = List("spark.master.host", "spark.master.port")
      val props = for (p <- propertiesToCopy) yield "-D%s=%s".format(p, System.getProperty(p))
      pb.environment.put("SPARK_DEBUG_OPTS", debugOptsString + " " + props.mkString(" "))
      pb.redirectErrorStream(true)
      val proc = pb.start()

      // Pipe the task's stdout and stderr to our own
      new Thread {
        override def run {
          val procStdout = proc.getInputStream
          var byte: Int = procStdout.read()
          while (byte != -1) {
            System.out.write(byte)
            byte = procStdout.read()
          }
        }
      }.start()
      proc.waitFor()
      logInfo("Finished running task " + task)
    } catch {
      case ex => logError("Failed to run task %s".format(task), ex)
    }
  }

  /** Runs the task that caused the specified exception locally. See debugTask. */
  def debugException(event: ExceptionEvent, debugOpts: Option[String] = None) {
    for ((taskStageId, taskPartition) <- event.task match {
      case rt: ResultTask[_, _] => Some((rt.stageId, rt.partition))
      case smt: ShuffleMapTask => Some((smt.stageId, smt.partition))
      case _ => None
    }) {
      debugTask(taskStageId, taskPartition, debugOpts)
    }
  }

  /** Reads any new events from the event log. */
  def loadNewEvents() {
    for (ois <- objectInputStream) {
      try {
        while (true) {
          val event = ois.readObject.asInstanceOf[EventLogEntry]
          addEvent(event)

          // Tell EventLogWriter about checksum events so it can do
          // checksum verification
          event match {
            case c: ChecksumEvent =>
              for (w <- sc.env.eventReporter.eventLogWriter) {
                w.processChecksumEvent(c)
              }
            case _ => {}
          }
        }
      } catch {
        case e: EOFException => {}
      }
    }
  }

  private[spark] def addEvent(event: EventLogEntry) {
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

  /** Replaces rdd with newRDD in the dependency graph. */
  private def replace[T](rdd: RDD[T], newRDD: RDD[T]) {
    val canonicalId = rddIdToCanonical(rdd.id)
    _rdds(canonicalId) = newRDD
    rddIdToCanonical(newRDD.id) = canonicalId

    for (descendantRddIndex <- (canonicalId + 1) until _rdds.length) {
      val updatedRDD = _rdds(descendantRddIndex).mapDependencies(new (RDD ~> RDD) {
        def apply[U](dependency: RDD[U]): RDD[U] = {
          _rdds(rddIdToCanonical(dependency.id)).asInstanceOf[RDD[U]]
        }
      })
      _rdds(descendantRddIndex) = updatedRDD
      rddIdToCanonical(updatedRDD.id) = descendantRddIndex
    }
  }

  /**
   * Returns a list of dependencies on ancestors to the given RDD such
   * that once all ancestors have been computed, the contents of the
   * given RDD can be computed without performing any more shuffles.
   */
  private def getParentStageDeps(rdd: RDD[_]): List[ShuffleDependency[_,_,_]] = {
    val ancestorDeps = new mutable.HashSet[ShuffleDependency[_,_,_]]
    val visited = new mutable.HashSet[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_,_,_] =>
              ancestorDeps += shufDep
            case _ =>
              visit(dep.rdd)
          }
        }
      }
    }
    visit(rdd)
    ancestorDeps.toList
  }


  private def firstExternalElement(location: Array[StackTraceElement]) =
    (location.tail.find(!_.getClassName.matches("""spark\.[A-Z].*"""))
      orElse { location.headOption }
      getOrElse { "" })

  private def rddType(rdd: RDD[_]): String =
    rdd.getClass.getName.replaceFirst("""^spark\.""", "")
}
