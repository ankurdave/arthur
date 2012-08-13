package spark.debugger

import com.google.common.io.Files

import java.io.File

import org.apache.hadoop.io._

import org.scalatest.FunSuite
import org.scalatest.PrivateMethodTester

import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import spark.RDD
import spark.HashPartitioner
import spark.SparkEnv
import spark.ShuffleDependency
import spark.SparkContext
import spark.SparkContext._

class EventLoggingSuite extends FunSuite with PrivateMethodTester {
  /**
   * Enables event logging for the current SparkEnv without setting spark.arthur.logPath. This is
   * useful for unit tests, where setting a property affects other tests as well.
   */
  def initializeEventLogging(eventLog: File, enableChecksumming: Boolean) {
    SparkEnv.get.eventReporter match {
      case r: ActorBasedEventReporter =>
        r.eventLogWriter.get.setEventLogPath(Some(eventLog.getAbsolutePath))
        r.enableChecksumming = enableChecksumming
      case _ => {}
    }
  }

  def makeSparkContextWithoutEventLogging(): SparkContext = {
    new SparkContext("local", "test")
  }

  def makeSparkContext(
    eventLog: File,
    enableChecksumming: Boolean = true
  ): SparkContext = {
    val sc = makeSparkContextWithoutEventLogging()
    initializeEventLogging(eventLog, enableChecksumming)
    sc
  }

  test("restore ParallelCollection from log") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD
    val sc = makeSparkContext(eventLog)
    val nums = sc.makeRDD(1 to 4)
    // Run an operation on the RDD so the scheduler becomes aware of it
    nums.collect()
    sc.stop()

    // Read it back from the event log
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.rdds.size === 1)
    assert(r.rdd(0).collect.toList === (1 to 4).toList)
    sc2.stop()
  }

  // TODO(ankurdave): Uncomment this test when we can have 2 SparkEnvs simultaneously.
/*  test("interactive event log reading") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD
    val sc = makeSparkContext(eventLog)
    val nums = sc.makeRDD(1 to 4)
    nums.collect()
    SparkEnv.get.eventReporter match {
      case r: ActorBasedEventReporter =>
        r.eventLogWriter.get.flush()
      case _ => {}
    }

    // Read the RDD back from the event log
    val sc2 = makeSparkContextWithoutEventLogging()
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.rdds.length === 1)
    assert(r.rdds(0).collect.toList === List(1, 2, 3, 4))

    // Make another RDD
    SparkEnv.set(sc.env)
    val nums2 = sc.makeRDD(1 to 5)
    SparkEnv.get.eventReporter match {
      case r: ActorBasedEventReporter =>
        r.eventLogWriter.get.flush()
      case _ => {}
    }

    // Read it back from the event log
    SparkEnv.set(sc2.env)
    r.loadNewEvents()
    assert(r.rdds.length === 2)
    assert(r.rdds(1).collect.toList === List(1, 2, 3, 4, 5))

    sc.stop()
    sc2.stop()
  }
*/

  test("set nextRddId and nextShuffleId after restoring") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make some RDDs
    val sc = makeSparkContext(eventLog)
    val nums = sc.makeRDD(1 to 4)
    val numsMapped = nums.map(x => (x, x + 1))
    val numsReduced = numsMapped.reduceByKey(_ + _)
    numsReduced.collect()
    sc.stop()

    // Read them back from the event log
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))

    // Make a new RDD and check for ID conflicts
    val nums2 = sc2.makeRDD(Seq((1, 2), (3, 4)))
    val nums2Reduced = nums2.reduceByKey(_ + _)
    for (rddId <- r.rdds.keys if rddId <= numsReduced.id) {
      assert(nums2.id != rddId)
      assert(nums2Reduced.id != rddId)
    }
    for (dep1 <- numsReduced.dependencies; dep2 <- nums2Reduced.dependencies) (dep1, dep2) match {
      case (a: ShuffleDependency[_,_,_], b: ShuffleDependency[_,_,_]) =>
        assert(a.shuffleId != b.shuffleId)
      case _ =>
        fail()
    }
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
    assert(r.rdds.size === 2)
    assert(r.rdd(0).collect.toList === (1 to 4).toList)
    assert(r.rdd(1).collect.toList != collected.toList)
    // Ensure that all checksums have gone through
    sc2.stop()

    // Make sure we found a checksum mismatch
    assert(r.checksumVerifier.mismatches.nonEmpty)
  }

/*  test("checksum verification - no false positives") {
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
    // Ensure that all checksums have gone through
    sc2.stop()

    // Make sure we didn't find any checksum mismatches
    assert(r.checksumVerifier.mismatches.isEmpty)
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

  test("disable Arthur") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD
    System.setProperty("spark.debugger.enable", "false")
    val sc = makeSparkContext(eventLog)
    val nums = sc.makeRDD(1 to 4)
    sc.stop()

    // Make sure the event log is empty
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.events.isEmpty)
    sc2.stop()
    System.setProperty("spark.debugger.enable", "true")
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
*/
}
