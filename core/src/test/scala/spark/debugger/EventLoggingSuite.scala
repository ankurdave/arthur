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

  def stopSparkContext(sc: SparkContext) {
    sc.stop()
    for (prop <- propertiesToRestore; value <- prevProperties.get(prop)) {
      if (value == null) {
        System.clearProperty(prop)
      } else {
        System.setProperty(prop, value)
      }
    }
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
    stopSparkContext(sc)

    // Read it back from the event log
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.rdd(nums.id).collect.toList === (1 to 4).toList)
    stopSparkContext(sc2)
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
    stopSparkContext(sc)

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
    stopSparkContext(sc2)
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
    stopSparkContext(sc)

    // Read them back from the event log
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.rdd(nums.id).collect.toList === (1 to 4).toList)
    assert(r.rdd(numsNondeterministic.id).collect.toList != collected.toList)
    // Ensure that all checksums have gone through
    stopSparkContext(sc2)

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
    stopSparkContext(sc)

    // Read them back from the event log and recompute them
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    r.rdd(rdd.id).collect()
    // Ensure that all checksums have gone through
    stopSparkContext(sc2)

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
    stopSparkContext(sc)

    // Verify that tasks were logged
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val tasks = r.events.collect { case t: TaskSubmission => t }
    assert(tasks.nonEmpty)
    stopSparkContext(sc2)
  }

  test("disable debugging") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD
    val sc = makeSparkContext(eventLog, enableDebugging = false)
    val nums = sc.makeRDD(1 to 4)
    nums.collect()
    stopSparkContext(sc)

    // Make sure the event log is empty
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.events.isEmpty)
    stopSparkContext(sc2)
  }

  test("disable checksumming") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD
    val sc = makeSparkContext(eventLog, enableChecksumming = false)
    val nums = sc.makeRDD(1 to 4).collect()
    stopSparkContext(sc)

    // Make sure the event log has no checksums
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.events.collect { case e: ChecksumEvent => e }.isEmpty)
    stopSparkContext(sc2)
  }

  test("forward tracing (simple)") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD and transform it
    val sc = makeSparkContext(eventLog)
    val a = sc.makeRDD(List(1, 2, 3, 4, 5, 6)).map(_ - 1)
    val b = a.flatMap(x => List.tabulate(x) { i => x }).filter(_ % 2 == 0)
    b.collect()
    stopSparkContext(sc)

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val startRDD = r.rdd(a.id).asInstanceOf[RDD[Int]]
    val endRDD = r.rdd(b.id).asInstanceOf[RDD[Int]]
    val descendantsOf4 = r.traceForward(startRDD, 4, endRDD).collect()
    assert(descendantsOf4 === Array(4, 4, 4, 4))
    val descendantsOf5 = r.traceForward(startRDD, 5, endRDD).collect()
    assert(descendantsOf5 === Array())
    stopSparkContext(sc2)
  }

  test("forward tracing (CoGroupedRDD)") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make a CoGroupedRDD and transform it
    val sc = makeSparkContext(eventLog)
    val a = sc.makeRDD(List((1, 2), (3, 4), (5, 6)))
    val b = sc.makeRDD(List((1, 1), (3, 3), (6, 6)))
    val c = a.cogroup(b)
    c.collect()
    stopSparkContext(sc)

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val a2 = r.rdd(a.id).asInstanceOf[RDD[(Int, Int)]]
    val b2 = r.rdd(b.id).asInstanceOf[RDD[(Int, Int)]]
    val c2 = r.rdd(c.id).asInstanceOf[RDD[(Int, (Seq[Int], Seq[Int]))]]

    // Tag it
    val descendantsOf12 = r.traceForward(a2, (1, 2), c2).collect()
    assert(descendantsOf12 === Array((1, (List(2), List(1)))))
    val descendantsOf66 = r.traceForward(b2, (6, 6), c2).collect()
    assert(descendantsOf66 === Array((6, (List(), List(6)))))
    stopSparkContext(sc2)
  }

  test("forward tracing (SortedRDD)") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make a SortedRDD and transform it
    val sc = makeSparkContext(eventLog)
    val a = sc.makeRDD(List((3, 1), (2, 1), (1, 1)))
    val b = a.sortByKey()
    b.collect()
    stopSparkContext(sc)

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val a2 = r.rdd(a.id).asInstanceOf[RDD[(Int, Int)]]
    val b2 = r.rdd(b.id).asInstanceOf[RDD[(Int, Int)]]
    val descendantsOf31 = r.traceForward(a2, (3, 1), b2).collect()
    assert(descendantsOf31 === Array((3, 1)))
    stopSparkContext(sc2)
  }

  test("forward tracing ([Flat]MappedValuesRDD)") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make a MappedValuesRDD and a FlatMappedValuesRDD
    val sc = makeSparkContext(eventLog)
    val a = sc.makeRDD(List((3, 1), (2, 2), (1, 1)))
    val b = a.mapValues(v => v + 1).flatMapValues(v => List.tabulate(v) { i => v })
    b.collect()
    stopSparkContext(sc)

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val a2 = r.rdd(a.id).asInstanceOf[RDD[(Int, Int)]]
    val b2 = r.rdd(b.id).asInstanceOf[RDD[(Int, Int)]]
    val descendantsOf22 = r.traceForward(a2, (2, 2), b2).collect()
    assert(descendantsOf22 === Array((2, 3), (2, 3), (2, 3)))
    stopSparkContext(sc2)
  }

  test("forward tracing (ShuffledRDD)") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make a ShuffledRDD
    val sc = makeSparkContext(eventLog)
    val a = sc.makeRDD(List((3, 1), (2, 2), (1, 1), (1, 2)))
    val b = a.reduceByKey(_ + _, 4)
    b.collect()
    stopSparkContext(sc)

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val a2 = r.rdd(a.id).asInstanceOf[RDD[(Int, Int)]]
    val b2 = r.rdd(b.id).asInstanceOf[RDD[(Int, Int)]]
    val descendantsOf11 = r.traceForward(a2, (1, 1), b2).collect()
    assert(descendantsOf11 === Array((1, 3)))
    val descendantsOf12 = r.traceForward(a2, (1, 2), b2).collect()
    assert(descendantsOf12 === Array((1, 3)))
    stopSparkContext(sc2)
  }

  test("backward tracing (simple)") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD and transform it
    val sc = makeSparkContext(eventLog)
    val a = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    val b = a.map(_ - 1).flatMap(x => List.tabulate(x) { i => x }).filter(_ % 2 == 0)
    b.collect()
    sc.stop()

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val a2 = r.rdd(a.id).asInstanceOf[RDD[Int]]
    val b2 = r.rdd(b.id).asInstanceOf[RDD[Int]]
    val ancestorsOf4 = r.traceBackward(a2, 4, b2).collect()
    assert(ancestorsOf4.toSet === Set(5))
    val ancestorsOf5 = r.traceBackward(a2, 5, b2).collect()
    assert(ancestorsOf5 === Array())
    sc2.stop()
  }

  test("backward tracing (ShuffledRDD)") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make a ShuffledRDD
    val sc = makeSparkContext(eventLog)
    val a = sc.makeRDD(List((3, 1), (2, 2), (1, 1), (1, 2)))
    val b = a.reduceByKey(_ + _, 4)
    b.collect()
    sc.stop()

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val a2 = r.rdd(a.id).asInstanceOf[RDD[(Int, Int)]]
    val b2 = r.rdd(b.id).asInstanceOf[RDD[(Int, Int)]]
    val ancestorsOf11 = r.traceBackward(a2, (1, 3), b2).collect()
    assert(ancestorsOf11.toSet === Set((1, 1), (1, 2)))
    sc2.stop()
  }

  test("backward tracing ([Flat]MappedValuesRDD)") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make a MappedValuesRDD and a FlatMappedValuesRDD
    val sc = makeSparkContext(eventLog)
    val a = sc.makeRDD(List((3, 1), (2, 2), (1, 1)))
    val b = a.mapValues(v => v + 1).flatMapValues(v => List.tabulate(v) { i => v })
    b.collect()
    sc.stop()

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val a2 = r.rdd(a.id).asInstanceOf[RDD[(Int, Int)]]
    val b2 = r.rdd(b.id).asInstanceOf[RDD[(Int, Int)]]
    val ancestorsOf23 = r.traceBackward(a2, (2, 3), b2).collect()
    assert(ancestorsOf23.toSet === Set((2, 2)))
    sc2.stop()
  }

  test("backward tracing (CoGroupedRDD)") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make a CoGroupedRDD and transform it
    val sc = makeSparkContext(eventLog)
    val a = sc.parallelize(List((1, 2), (3, 4), (5, 6)))
    val b = sc.parallelize(List((1, 1), (3, 3), (6, 6)))
    val c = a.groupWith(b)
    // Take the common-case (Seq(v), Seq(w)) to simply (v, w)
    val d = c.flatMapValues {
      case (vs, ws) => for (v <- vs.headOption; w <- ws.headOption) yield (v, w)
    }
    d.collect()
    sc.stop()

    // Trace some elements and verify the results
    val sc2 = makeSparkContext(eventLog)
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val a2 = r.rdd(a.id).asInstanceOf[RDD[(Int, Int)]]
    val b2 = r.rdd(b.id).asInstanceOf[RDD[(Int, Int)]]
    val c2 = r.rdd(c.id).asInstanceOf[RDD[(Int, (Seq[Int], Seq[Int]))]]
    val d2 = r.rdd(d.id).asInstanceOf[RDD[(Int, (Int, Int))]]
    val result1 = r.traceBackward(a2, (1, (Seq(2), Seq(1))), c2).collect()
    assert(result1.toSet === Set((1, 2)))
    val result2 = r.traceBackward(a2, (1, (2, 1)), d2).collect()
    assert(result2.toSet === Set((1, 2)))
    sc2.stop()
  }
}
