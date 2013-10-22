package spark.debugger

import org.scalatest.FunSuite

import spark.RDD
import spark.SparkContext
import spark.SparkContext._
import spark.SparkEnv

object TaggedRDDSuiteUtils {
  // Needs to be defined in a separate object from TaggedRDDSuite to avoid
  // NotSerializableException: org.scalatest.Engine
  def tagRDD[A](rdd: RDD[A], p: Any => Boolean): TaggedRDD[A, Boolean] = rdd.tagged(
    new RDDTagger[Boolean] {
      def apply[B](b: RDD[B]) = tagRDD[B](b, p)
      def apply[B](b: B) = p(b)
    })
}

class TaggedRDDSuite extends FunSuite {
 
  test("simple RDD tagging") {
    val sc = new SparkContext("local", "test")
    val a = sc.parallelize(List(1,2,3), 2)
    val ta = TaggedRDDSuiteUtils.tagRDD(a, _ == 2)
    assert(ta.collect().toList.sorted == List((1, false), (2, true), (3, false)).sorted)
    sc.stop()
  }

  test("FlatMappedRDD tagging") {
    val sc = new SparkContext("local", "test")
    val a = sc.parallelize(List(1,2,3), 2).flatMap(x => List.make(x, x))
    val ta = TaggedRDDSuiteUtils.tagRDD(a, _ == 2)
    val taExpected = List((1, false), (2, true), (2, true), (3, false), (3, false), (3, false))
    assert(ta.collect().toList.sorted == taExpected.sorted)
    sc.stop()
  }
}
