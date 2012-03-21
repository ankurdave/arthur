package spark

import java.io.File

import scala.io.Source

import com.google.common.io.Files
import org.scalatest.FunSuite
import org.apache.hadoop.io._

import SparkContext._

class EventLoggingSuite extends FunSuite {
  /**
   * Enables event logging for the current SparkEnv without setting spark.arthur.logPath. This is
   * useful for unit tests, where setting a property affects other tests as well.
   */
  def initializeEventLogging(eventLog: File, enableArthur: Boolean, enableChecksumming: Boolean) {
    SparkEnv.get.eventReporter.enableArthur = enableArthur
    SparkEnv.get.eventReporter.enableChecksumming = enableChecksumming
    SparkEnv.get.eventReporter.init()
    for (w <- SparkEnv.get.eventReporter.eventLogWriter) {
      w.setEventLogPath(Some(eventLog.getAbsolutePath))
    }
  }

  def makeSparkContextWithoutEventLogging(): SparkContext = {
    new SparkContext("local", "test")
  }

  def makeSparkContext(
    eventLog: File,
    enableArthur: Boolean = true,
    enableChecksumming: Boolean = true
  ): SparkContext = {
    val sc = makeSparkContextWithoutEventLogging()
    initializeEventLogging(eventLog, enableArthur, enableChecksumming)
    sc
  }

  test("restore ParallelCollection from log") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD
    val sc = makeSparkContext(eventLog)
    val nums = sc.makeRDD(1 to 4)
    sc.stop()

    // Read it back from the event log
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.rdds.length === 1)
    assert(r.rdds(0).collect.toList === (1 to 4).toList)
    sc2.stop()
  }

  test("interactive event log reading") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD
    val sc = makeSparkContext(eventLog)
    val nums = sc.makeRDD(1 to 4)
    for (w <- SparkEnv.get.eventReporter.eventLogWriter)
      w.flush()

    // Read the RDD back from the event log
    val sc2 = makeSparkContextWithoutEventLogging()
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.rdds.length === 1)
    assert(r.rdds(0).collect.toList === List(1, 2, 3, 4))

    // Make another RDD
    SparkEnv.set(sc.env)
    val nums2 = sc.makeRDD(1 to 5)
    for (w <- SparkEnv.get.eventReporter.eventLogWriter)
      w.flush()

    // Read it back from the event log
    SparkEnv.set(sc2.env)
    r.loadNewEvents()
    assert(r.rdds.length === 2)
    assert(r.rdds(1).collect.toList === List(1, 2, 3, 4, 5))

    sc.stop()
    sc2.stop()
  }

  test("set nextRddId after restoring") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make some RDDs
    val sc = makeSparkContext(eventLog)
    val nums = sc.makeRDD(1 to 4)
    val numsMapped = nums.map(x => x + 1)
    sc.stop()

    // Read them back from the event log
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.rdds.length === 2)

    // Make a new RDD and check for ID conflicts
    val nums2 = sc2.makeRDD(1 to 5)
    assert(nums2.id != r.rdds(0).id)
    assert(nums2.id != r.rdds(1).id)
    sc2.stop()
  }

  test("checksum verification") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make some RDDs that use Math.random
    val sc = makeSparkContext(eventLog)
    val nums = sc.makeRDD(1 to 4)
    val numsNondeterministic = nums.map(x => Math.random)
    val collected = numsNondeterministic.collect
    sc.stop()

    // Read them back from the event log
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.rdds.length === 2)
    assert(r.rdds(0).collect.toList === (1 to 4).toList)
    assert(r.rdds(1).collect.toList != collected.toList)
    // Ensure that all checksums have gone through
    sc2.stop()

    // Make sure we found a checksum mismatch
    assert(r.checksumMismatches.nonEmpty)
  }

  test("task submission logging") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD and do some computation to run tasks
    val sc = makeSparkContext(eventLog)
    val nums = sc.makeRDD(List(1, 2, 3))
    val nums2 = nums.map(_ * 2)
    val nums2List = nums2.collect.toList
    sc.stop()

    // Verify that tasks were logged
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val tasks = r.events.collect { case t: TaskSubmission => t }
    assert(tasks.nonEmpty)
    sc2.stop()
  }

  test("assertions") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD and transform it
    val sc = makeSparkContext(eventLog)
    sc.makeRDD(List(1, -2, 3)).map(x => math.sqrt(x)).map(_ * 2).collect()
    sc.stop()

    // Restore one of the RDDs from the event log and assert something
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    r.assert(r.rdds(1).asInstanceOf[RDD[Double]], (x: Double) => !x.isNaN)
    r.assert(r.rdds(2).asInstanceOf[RDD[Double]], (x: Double, y: Double) => x + y, (x: Double) => x > 0)
    r.rdds(2).foreach(x => {}) // Force assertions to be checked
    sc2.stop()

    // Verify that assertion failures were logged
    val failures = r.events.collect { case t: AssertionFailure => t }
    assert(failures.size === 2)
    failures(0) match {
      case ElementAssertionFailure(rddId, element: Double) =>
        assert(rddId === 1)
        assert(element.isNaN)
      case _ => fail()
    }
    assert(failures(1).isInstanceOf[ReduceAssertionFailure])
  }

  test("disable Arthur") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD
    val sc = makeSparkContext(eventLog, enableArthur = false)
    val nums = sc.makeRDD(1 to 4)
    sc.stop()

    // Make sure the event log is empty
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.events.isEmpty)
    sc2.stop()
  }

  test("disable checksumming") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD
    val sc = makeSparkContext(eventLog, enableChecksumming = false)
    val nums = sc.makeRDD(1 to 4).collect
    sc.stop()

    // Make sure the event log has no checksums
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.events.collect { case e: ChecksumEvent => e }.isEmpty)
    sc2.stop()
  }
}
