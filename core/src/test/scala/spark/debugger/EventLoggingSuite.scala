package spark.debugger

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import java.io.File
import java.util.Properties

import com.google.common.io.Files

import org.apache.hadoop.io._

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

import spark.HashPartitioner
import spark.RDD
import spark.ShuffleDependency
import spark.SparkContext
import spark.SparkContext._
import spark.SparkEnv

class EventLoggingSuite extends FunSuite with BeforeAndAfter {
  // Reset the system properties after running each test.
  val prevProperties: mutable.Map[String, String] = new mutable.HashMap[String, String]
  val propertiesToRestore: immutable.Set[String] = immutable.HashSet(
    "spark.debugger.enable", "spark.debugger.logPath", "spark.debugger.checksum")
  after {
    for (prop <- propertiesToRestore; value <- prevProperties.get(prop)) {
      if (value == null) {
        System.clearProperty(prop)
      } else {
        System.setProperty(prop, value)
      }
    }
  }

  def makeSparkContext(
    eventLog: File,
    enableDebugging: Boolean = true,
    enableChecksumming: Boolean = true
  ): SparkContext = {
    val newProperties = Map(
      "spark.debugger.enable" -> enableDebugging.toString,
      "spark.debugger.logPath" -> eventLog.getPath,
      "spark.debugger.checksum" -> enableChecksumming.toString)
    for ((prop, newValue) <- newProperties) {
      val oldValue = System.setProperty(prop, newValue)
      prevProperties.put(prop, oldValue)
    }
    new SparkContext("local", "test")
  }

  def setSystemProperty(property: String, value: String) {
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
    assert(r.rdd(nums.id).collect.toList === (1 to 4).toList)
    sc2.stop()
  }

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
    for (rddId <- r.rddIds if rddId <= numsReduced.id) {
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
    assert(r.rdd(nums.id).collect.toList === (1 to 4).toList)
    assert(r.rdd(numsNondeterministic.id).collect.toList != collected.toList)
    // Ensure that all checksums have gone through
    sc2.stop()

    // Make sure we found a checksum mismatch
    assert(r.checksumMismatches.nonEmpty)
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
    r.rdd(rdd.id).collect()
    // Ensure that all checksums have gone through
    sc2.stop()

    // Make sure we didn't find any checksum mismatches
    assert(r.checksumMismatches.isEmpty)
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

  test("disable debugging") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD
    val sc = makeSparkContext(eventLog, enableDebugging = false)
    val nums = sc.makeRDD(1 to 4)
    nums.collect()
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
    val nums = sc.makeRDD(1 to 4).collect()
    sc.stop()

    // Make sure the event log has no checksums
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.events.collect { case e: ChecksumEvent => e }.isEmpty)
    sc2.stop()
  }
}
