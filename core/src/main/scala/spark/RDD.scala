package spark

import java.io.EOFException
import java.net.URL
import java.util.concurrent.atomic.AtomicLong
import java.util.HashSet
import java.util.Random
import java.util.Date

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.FileOutputCommitter
import org.apache.hadoop.mapred.HadoopWriter
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCommitter
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapred.TextOutputFormat

import SparkContext._

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable, 
 * partitioned collection of elements that can be operated on in parallel.
 *
 * Each RDD is characterized by five main properties:
 * - A list of splits (partitions)
 * - A function for computing each split
 * - A list of dependencies on other RDDs
 * - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 * - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
 *   HDFS)
 *
 * All the scheduling and execution in Spark is done based on these methods, allowing each RDD to 
 * implement its own way of computing itself.
 *
 * This class also contains transformation methods available on all RDDs (e.g. map and filter). In 
 * addition, PairRDDFunctions contains extra methods available on RDDs of key-value pairs, and 
 * SequenceFileRDDFunctions contains extra methods for saving RDDs to Hadoop SequenceFiles.
 */
abstract class RDD[T: ClassManifest](@transient private var sc: SparkContext) extends Serializable {

  // Methods that must be implemented by subclasses
  def splits: Array[Split]
  def compute(split: Split): Iterator[T]
  val dependencies: List[Dependency[_]]
  /**
   * Returns a new RDD whose parents are the result of applying g to this RDD's parents. This makes
   * it possible to transform the RDD dependency graph.
   */
  def mapDependencies(g: RDD ~> RDD): RDD[T]

  // Optionally overridden by subclasses to specify how they are partitioned
  val partitioner: Option[Partitioner] = None

  // Optionally overridden by subclasses to specify placement preferences
  def preferredLocations(split: Split): Seq[String] = Nil

  /**
   * Records the event of this RDD's creation. Leaf subclasses should call this method at the end of
   * their constructors.
   *
   * If the SparkEnv is null (e.g., because the RDD was created in a new thread and the SparkEnv was
   * not set for that thread), this fails silently.
   *
   * Once SI-4396 is fixed, we should use DelayedInit to run this instead.
   */
  protected def reportCreation() {
    for (env <- Option(SparkEnv.get)) {
      env.eventReporter.reportRDDCreation(this, Thread.currentThread.getStackTrace)
    }
  }

  def context = sc

  // Get a unique ID for this RDD
  val id = sc.newRddId()
  
  // Variables relating to caching
  private var shouldCache = false

  // Change this RDD's caching
  def cache(): RDD[T] = {
    shouldCache = true
    this
  }
  
  // Read this RDD; will read from cache if applicable, or otherwise compute
  final def iterator(split: Split): Iterator[T] = {
    if (shouldCache) {
      SparkEnv.get.cacheTracker.getOrCompute[T](this, split)
    } else {
      compute(split)
    }
  }
  
  // Transformations (return a new RDD)
  
  def map[U: ClassManifest](f: T => U): RDD[U] = new MappedRDD(this, sc.clean(f))
  
  def flatMap[U: ClassManifest](f: T => Traversable[U]): RDD[U] =
    new FlatMappedRDD(this, sc.clean(f))
  
  def filter(f: T => Boolean): RDD[T] = new FilteredRDD(this, sc.clean(f))

  def sample(withReplacement: Boolean, fraction: Double, seed: Int): RDD[T] =
    new SampledRDD(this, withReplacement, fraction, seed)

  def takeSample(withReplacement: Boolean, num: Int, seed: Int): Array[T] = {
  	var fraction = 0.0
  	var total = 0
  	var multiplier = 3.0
  	var initialCount = count()
  	var maxSelected = 0
  	
  	if (initialCount > Integer.MAX_VALUE) {
  	  maxSelected = Integer.MAX_VALUE
  	} else {
  	  maxSelected = initialCount.toInt
  	}
  	
  	if (num > initialCount) {
  		total = maxSelected
    	fraction = Math.min(multiplier * (maxSelected + 1) / initialCount, 1.0)
  	} else if (num < 0) {
  		throw(new IllegalArgumentException("Negative number of elements requested"))
  	} else {
  		fraction = Math.min(multiplier * (num + 1) / initialCount, 1.0)
  		total = num.toInt
  	}
	
    var samples = this.sample(withReplacement, fraction, seed).collect()
	
  	while (samples.length < total) {
  		samples = this.sample(withReplacement, fraction, seed).collect()
  	}
	
  	val arr = samples.take(total)
	
  	return arr
  }

  def union(other: RDD[T]): RDD[T] = new UnionRDD(sc, Array(this, other))

  def ++(other: RDD[T]): RDD[T] = this.union(other)

  def glom(): RDD[Array[T]] = new GlommedRDD(this)

  def cartesian[U: ClassManifest](other: RDD[U]): RDD[(T, U)] = new CartesianRDD(sc, this, other)

  def groupBy[K: ClassManifest](f: T => K, numSplits: Int): RDD[(K, Seq[T])] = {
    val cleanF = sc.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(numSplits)
  }

  def groupBy[K: ClassManifest](f: T => K): RDD[(K, Seq[T])] = groupBy[K](f, sc.defaultParallelism)

  def pipe(command: String): RDD[String] = new PipedRDD(this, command)

  def pipe(command: Seq[String]): RDD[String] = new PipedRDD(this, command)

  def mapPartitions[U: ClassManifest](f: Iterator[T] => Iterator[U]): RDD[U] =
    new MapPartitionsRDD(this, sc.clean(f))

  // Actions (launch a job to return a value to the user program)
  
  def foreach(f: T => Unit) {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }

  def collect(): Array[T] = {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }

  def reduce(f: (T, T) => T): T = {
    val cleanF = sc.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      }else {
        None
      }
    }
    val options = sc.runJob(this, reducePartition)
    val results = new ArrayBuffer[T]
    for (opt <- options; elem <- opt) {
      results += elem
    }
    if (results.size == 0) {
      throw new UnsupportedOperationException("empty collection")
    } else {
      return results.reduceLeft(cleanF)
    }
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using a
   * given associative function and a neutral "zero value". The function op(t1, t2) is allowed to 
   * modify t1 and return it as its result value to avoid object allocation; however, it should not
   * modify t2.
   */
  def fold(zeroValue: T)(op: (T, T) => T): T = {
    val cleanOp = sc.clean(op)
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.fold(zeroValue)(cleanOp))
    return results.fold(zeroValue)(cleanOp)
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using
   * given combine functions and a neutral "zero value". This function can return a different result
   * type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
   * and one operation for merging two U's, as in scala.TraversableOnce. Both of these functions are
   * allowed to modify and return their first argument instead of creating a new U to avoid memory
   * allocation.
   */
  def aggregate[U: ClassManifest](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = {
    val cleanSeqOp = sc.clean(seqOp)
    val cleanCombOp = sc.clean(combOp)
    val results = sc.runJob(this,
        (iter: Iterator[T]) => iter.aggregate(zeroValue)(cleanSeqOp, cleanCombOp))
    return results.fold(zeroValue)(cleanCombOp)
  }
  
  def count(): Long = {
    sc.runJob(this, (iter: Iterator[T]) => {
      var result = 0L
      while (iter.hasNext) {
        result += 1L
        iter.next
      }
      result
    }).sum
  }

  def toArray(): Array[T] = collect()
  
  /**
   * Take the first num elements of the RDD. This currently scans the partitions *one by one*, so
   * it will be slow if a lot of partitions are required. In that case, use collect() to get the
   * whole RDD instead.
   */
  def take(num: Int): Array[T] = {
    if (num == 0) {
      return new Array[T](0)
    }
    val buf = new ArrayBuffer[T]
    var p = 0
    while (buf.size < num && p < splits.size) {
      val left = num - buf.size
      val res = sc.runJob(this, (it: Iterator[T]) => it.take(left).toArray, Array(p), true)
      buf ++= res(0)
      if (buf.size == num)
        return buf.toArray
      p += 1
    }
    return buf.toArray
  }

  def first(): T = take(1) match {
    case Array(t) => t
    case _ => throw new UnsupportedOperationException("empty collection")
  }

  def saveAsTextFile(path: String) {
    this.map(x => (NullWritable.get(), new Text(x.toString)))
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }

  def saveAsObjectFile(path: String) {
    this.glom
      .map(x => (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
      .saveAsSequenceFile(path)
  }

  /** When deserializing an RDD from an event log, sets the SparkContext as well. */
  private def readObject(stream: java.io.ObjectInputStream) {
    stream.defaultReadObject()
    stream match {
      case s: EventLogInputStream =>
        sc = s.sc
      case _ => {}
    }
  }
}

class MappedRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: T => U)
  extends RDD[U](prev.context) {
  
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def mapDependencies(g: RDD ~> RDD) = new MappedRDD(g(prev), f)
  override def compute(split: Split) = prev.iterator(split).map(f)
  reportCreation()
}

class FlatMappedRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: T => Traversable[U])
  extends RDD[U](prev.context) {
  
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def mapDependencies(g: RDD ~> RDD) = new FlatMappedRDD(g(prev), f)
  override def compute(split: Split) = prev.iterator(split).flatMap(f)
  reportCreation()
}

class FilteredRDD[T: ClassManifest](prev: RDD[T], f: T => Boolean) extends RDD[T](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def mapDependencies(g: RDD ~> RDD) = new FilteredRDD(g(prev), f)
  override def compute(split: Split) = prev.iterator(split).filter(f)
  reportCreation()
}

class GlommedRDD[T: ClassManifest](prev: RDD[T]) extends RDD[Array[T]](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def mapDependencies(g: RDD ~> RDD) = new GlommedRDD(g(prev))
  override def compute(split: Split) = Array(prev.iterator(split).toArray).iterator
  reportCreation()
}

class MapPartitionsRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: Iterator[T] => Iterator[U])
  extends RDD[U](prev.context) {
  
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def mapDependencies(g: RDD ~> RDD) = new MapPartitionsRDD(g(prev), f)
  override def compute(split: Split) = f(prev.iterator(split))
  reportCreation()
}
