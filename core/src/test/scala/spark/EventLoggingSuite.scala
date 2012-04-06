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

    // Make some RDDs that have nondeterministic transformations
    val sc = makeSparkContext(eventLog)
    val nums = sc.makeRDD(1 to 4)
    val numsNondeterministic = nums.map(x => math.random)
    val collected = numsNondeterministic.collect
    sc.stop()

    // Read them back from the event log
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.rdds.length === 2)
    assert(r.rdds(0).collect.toList === (1 to 4).toList)
    assert(r.rdds(1).collect.toList != collected.toList)
    val eventLogWriter = sc2.env.eventReporter.eventLogWriter.get
    // Ensure that all checksums have gone through
    sc2.stop()

    // Make sure we found a checksum mismatch
    assert(eventLogWriter.checksumMismatches.nonEmpty)
  }

  test("checksum verification - no false positives") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make some RDDs that don't have any nondeterministic transformations
    val sc = makeSparkContext(eventLog)
    val rdd = sc.makeRDD(1 to 4).map(x => (x, x * x)).partitionBy(new HashPartitioner(4))
    rdd.collect()
    sc.stop()

    // Read them back from the event log and recompute them
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    r.rdds(r.rdds.length - 1).collect()
    val eventLogWriter = sc2.env.eventReporter.eventLogWriter.get
    // Ensure that all checksums have gone through
    sc2.stop()

    // Make sure we didn't find any checksum mismatches
    assert(eventLogWriter.checksumMismatches.isEmpty)
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
    r.assert(r.rdds(1).asInstanceOf[RDD[Double]], (x: Double) => true) // another assertion on the same RDD
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

  test("forward tracing") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD and transform it
    val sc = makeSparkContext(eventLog)
    sc.makeRDD(List(1, 2, 3, 4, 5, 6)).map(_ - 1).flatMap(x => List.tabulate(x) { i => x }).filter(_ % 2 == 0).collect()
    sc.stop()

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val startRDD = r.rdds(1).asInstanceOf[RDD[Int]]
    val endRDD = r.rdds(3).asInstanceOf[RDD[Int]]
    val descendantsOf4 = r.traceForward(startRDD, 4, endRDD).collect()
    assert(descendantsOf4 === Array(4, 4, 4, 4))
    val descendantsOf5 = r.traceForward(startRDD, 5, endRDD).collect()
    assert(descendantsOf5 === Array())
    sc2.stop()
  }

  test("forward tracing on CoGroupedRDD") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make a CoGroupedRDD and transform it
    val sc = makeSparkContext(eventLog)
    val rdd0 = sc.makeRDD(List((1, 2), (3, 4), (5, 6)))
    val rdd1 = sc.makeRDD(List((1, 1), (3, 3), (6, 6)))
    val cogrouped = rdd0.groupWith(rdd1)
    cogrouped.collect()
    sc.stop()

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val descendantsOf12 = r.traceForward(rdd0, (1, 2), cogrouped).collect()
    assert(descendantsOf12 === Array((1, (List(2), List(1)))))
    val descendantsOf66 = r.traceForward(rdd1, (6, 6), cogrouped).collect()
    assert(descendantsOf66 === Array((6, (List(), List(6)))))
    sc2.stop()
  }

  test("forward tracing on SortedRDD") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make a CoGroupedRDD and transform it
    val sc = makeSparkContext(eventLog)
    val rdd = sc.makeRDD(List((3, 1), (2, 1), (1, 1)))
    val sorted = rdd.sortByKey()
    sorted.collect()
    sc.stop()

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val descendantsOf31 = r.traceForward(rdd, (3, 1), sorted).collect()
    assert(descendantsOf31 === Array((3, 1)))
    sc2.stop()
  }

  test("forward tracing on (Flat)MappedValuesRDD") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make a MappedValuesRDD and a FlatMappedValuesRDD
    val sc = makeSparkContext(eventLog)
    val rdd = sc.makeRDD(List((3, 1), (2, 2), (1, 1)))
    val fmv = rdd.mapValues(v => v + 1).flatMapValues(v => List.tabulate(v) { i => v })
    fmv.collect()
    sc.stop()

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val descendantsOf22 = r.traceForward(rdd, (2, 2), fmv).collect()
    assert(descendantsOf22 === Array((2, 3), (2, 3), (2, 3)))
    sc2.stop()
  }

  test("forward tracing on ShuffledRDD") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make a ShuffledRDD
    val sc = makeSparkContext(eventLog)
    val rdd = sc.makeRDD(List((3, 1), (2, 2), (1, 1), (1, 2)))
    val shuffled = rdd.reduceByKey(_ + _, 4)
    shuffled.collect()
    sc.stop()

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val descendantsOf11 = r.traceForward(rdd, (1, 1), shuffled).collect()
    assert(descendantsOf11 === Array((1, 3)))
    val descendantsOf12 = r.traceForward(rdd, (1, 2), shuffled).collect()
    assert(descendantsOf12 === Array((1, 3)))
    sc2.stop()
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
