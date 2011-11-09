package spark

import java.io.EOFException
import java.net.URL
import java.io.ObjectInputStream
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
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.HadoopWriter
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCommitter
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapred.TextOutputFormat

import SparkContext._

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion.
 */
class PairRDDFunctions[K: ClassManifest, V: ClassManifest](self: RDD[(K, V)]) extends Logging with Serializable {
  def reduceByKeyToDriver(func: (V, V) => V): Map[K, V] = {
    def mergeMaps(m1: HashMap[K, V], m2: HashMap[K, V]): HashMap[K, V] = {
      for ((k, v) <- m2) {
        m1.get(k) match {
          case None => m1(k) = v
          case Some(w) => m1(k) = func(w, v)
        }
      }
      return m1
    }
    self.map(pair => HashMap(pair)).reduce(mergeMaps)
  }

  def combineByKey[C](createCombiner: V => C,
                      mergeValue: (C, V) => C,
                      mergeCombiners: (C, C) => C,
                      numSplits: Int,
                      partitioner: Partitioner)
  : RDD[(K, C)] =
  {
    val aggregator = new Aggregator[K, V, C](createCombiner, mergeValue, mergeCombiners)
    new ShuffledRDD(self, aggregator, partitioner)
  }

  def combineByKey[C](createCombiner: V => C,
                      mergeValue: (C, V) => C,
                      mergeCombiners: (C, C) => C,
                      numSplits: Int)
  : RDD[(K, C)] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, numSplits,
                 new HashPartitioner(numSplits))
  }

  def reduceByKey(func: (V, V) => V, numSplits: Int): RDD[(K, V)] = {
    combineByKey[V]((v: V) => v, func, func, numSplits)
  }

  def groupByKey(numSplits: Int): RDD[(K, Seq[V])] = {
    def createCombiner(v: V) = ArrayBuffer(v)
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
    def mergeCombiners(b1: ArrayBuffer[V], b2: ArrayBuffer[V]) = b1 ++= b2
    val bufs = combineByKey[ArrayBuffer[V]](
      createCombiner _, mergeValue _, mergeCombiners _, numSplits)
    bufs.asInstanceOf[RDD[(K, Seq[V])]]
  }

  def groupByKey(partitioner: Partitioner): RDD[(K, Seq[V])] = {
    def createCombiner(v: V) = ArrayBuffer(v)
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
    def mergeCombiners(b1: ArrayBuffer[V], b2: ArrayBuffer[V]) = b1 ++= b2
    val bufs = combineByKey[ArrayBuffer[V]](
      createCombiner _, mergeValue _, mergeCombiners _, partitioner.numPartitions, partitioner)
    bufs.asInstanceOf[RDD[(K, Seq[V])]]
  }

  def partitionBy(partitioner: Partitioner): RDD[(K, V)] = {
    def createCombiner(v: V) = ArrayBuffer(v)
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
    def mergeCombiners(b1: ArrayBuffer[V], b2: ArrayBuffer[V]) = b1 ++= b2
    val bufs = combineByKey[ArrayBuffer[V]](
      createCombiner _, mergeValue _, mergeCombiners _, partitioner.numPartitions, partitioner)
    bufs.flatMapValues(buf => buf)
  }

  def join[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (V, W))] = {
    val vs: RDD[(K, Either[V, W])] = self.map { case (k, v) => (k, Left(v)) }
    val ws: RDD[(K, Either[V, W])] = other.map { case (k, w) => (k, Right(w)) }
    (vs ++ ws).groupByKey(numSplits).flatMap {
      case (k, seq) => {
        val vbuf = new ArrayBuffer[V]
        val wbuf = new ArrayBuffer[W]
        seq.foreach(_ match {
          case Left(v) => vbuf += v
          case Right(w) => wbuf += w
        })
        for (v <- vbuf; w <- wbuf) yield (k, (v, w))
      }
    }
  }

  def leftOuterJoin[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (V, Option[W]))] = {
    val vs: RDD[(K, Either[V, W])] = self.map { case (k, v) => (k, Left(v)) }
    val ws: RDD[(K, Either[V, W])] = other.map { case (k, w) => (k, Right(w)) }
    (vs ++ ws).groupByKey(numSplits).flatMap {
      case (k, seq) => {
        val vbuf = new ArrayBuffer[V]
        val wbuf = new ArrayBuffer[Option[W]]
        seq.foreach(_ match {
          case Left(v) => vbuf += v
          case Right(w) => wbuf += Some(w)
        })
        if (wbuf.isEmpty) {
          wbuf += None
        }
        for (v <- vbuf; w <- wbuf) yield (k, (v, w))
      }
    }
  }

  def rightOuterJoin[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (Option[V], W))] = {
    val vs: RDD[(K, Either[V, W])] = self.map { case (k, v) => (k, Left(v)) }
    val ws: RDD[(K, Either[V, W])] = other.map { case (k, w) => (k, Right(w)) }
    (vs ++ ws).groupByKey(numSplits).flatMap {
      case (k, seq) => {
        val vbuf = new ArrayBuffer[Option[V]]
        val wbuf = new ArrayBuffer[W]
        seq.foreach(_ match {
          case Left(v) => vbuf += Some(v)
          case Right(w) => wbuf += w
        })
        if (vbuf.isEmpty) {
          vbuf += None
        }
        for (v <- vbuf; w <- wbuf) yield (k, (v, w))
      }
    }
  }

  def combineByKey[C](createCombiner: V => C,
                      mergeValue: (C, V) => C,
                      mergeCombiners: (C, C) => C)
  : RDD[(K, C)] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, defaultParallelism)
  }

  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = {
    reduceByKey(func, defaultParallelism)
  }

  def groupByKey(): RDD[(K, Seq[V])] = {
    groupByKey(defaultParallelism)
  }

  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = {
    join(other, defaultParallelism)
  }

  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = {
    leftOuterJoin(other, defaultParallelism)
  }

  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = {
    rightOuterJoin(other, defaultParallelism)
  }

  def defaultParallelism = self.context.defaultParallelism

  def collectAsMap(): Map[K, V] = HashMap(self.collect(): _*)
  
  def mapValues[U](f: V => U): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new MappedValuesRDD(self, cleanF)
  }
  
  def flatMapValues[U](f: V => Traversable[U]): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new FlatMappedValuesRDD(self, cleanF)
  }
  
  def groupWith[W](other: RDD[(K, W)]): RDD[(K, (Seq[V], Seq[W]))] = {
    val part = self.partitioner match {
      case Some(p) => p
      case None => new HashPartitioner(defaultParallelism)
    }
    val cg = new CoGroupedRDD[K](Seq(self.asInstanceOf[RDD[(_, _)]], other.asInstanceOf[RDD[(_, _)]]), part)
    val prfs = new PairRDDFunctions[K, Seq[Seq[_]]](cg)(classManifest[K], Manifests.seqSeqManifest)
    prfs.mapValues {
      case Seq(vs, ws) =>
        (vs.asInstanceOf[Seq[V]], ws.asInstanceOf[Seq[W]])
    }
  }
  
  def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    val part = self.partitioner match {
      case Some(p) => p
      case None => new HashPartitioner(defaultParallelism)
    }
    new CoGroupedRDD[K](
        Seq(self.asInstanceOf[RDD[(_, _)]], 
            other1.asInstanceOf[RDD[(_, _)]], 
            other2.asInstanceOf[RDD[(_, _)]]),
        part).map {
      case (k, Seq(vs, w1s, w2s)) =>
        (k, (vs.asInstanceOf[Seq[V]], w1s.asInstanceOf[Seq[W1]], w2s.asInstanceOf[Seq[W2]]))
    }
  }

  def lookup(key: K): Seq[V] = {
    self.partitioner match {
      case Some(p) =>
        val index = p.getPartition(key)
        def process(it: Iterator[(K, V)]): Seq[V] = {
          val buf = new ArrayBuffer[V]
          for ((k, v) <- it if k == key)
            buf += v
          buf
        }
        val res = self.context.runJob(self, process _, Array(index), false)
        res(0)
      case None =>
        throw new UnsupportedOperationException("lookup() called on an RDD without a partitioner")
    }
  }

  def saveAsHadoopFile [F <: OutputFormat[K, V]] (path: String) (implicit fm: ClassManifest[F]) {
    saveAsHadoopFile(path, getKeyClass, getValueClass, fm.erasure.asInstanceOf[Class[F]])
  }

  def saveAsHadoopFile(path: String,
                       keyClass: Class[_],
                       valueClass: Class[_],
                       outputFormatClass: Class[_ <: OutputFormat[_, _]],
                       conf: JobConf = new JobConf) {
    conf.setOutputKeyClass(keyClass)
    conf.setOutputValueClass(valueClass)
    // conf.setOutputFormat(outputFormatClass) // Doesn't work in Scala 2.9 due to what may be a generics bug
    conf.set("mapred.output.format.class", outputFormatClass.getName)
    conf.setOutputCommitter(classOf[FileOutputCommitter])
    FileOutputFormat.setOutputPath(conf, HadoopWriter.createPathFromString(path, conf))
    saveAsHadoopDataset(conf)
  }
  
  def saveAsHadoopDataset(conf: JobConf) {
    val outputFormatClass = conf.getOutputFormat
    val keyClass = conf.getOutputKeyClass
    val valueClass = conf.getOutputValueClass
    if (outputFormatClass == null)
      throw new SparkException("Output format class not set")
    if (keyClass == null)
      throw new SparkException("Output key class not set")
    if (valueClass == null)
      throw new SparkException("Output value class not set")
    
    logInfo("Saving as hadoop file of type (" + keyClass.getSimpleName+ ", " + valueClass.getSimpleName+ ")")

    val writer = new HadoopWriter(conf)
    writer.preSetup()

    def writeToFile(context: TaskContext, iter: Iterator[(K,V)]): HadoopWriter = {
      writer.setup(context.stageId, context.splitId, context.attemptId)
      writer.open()
      
      var count = 0
      while(iter.hasNext) {
        val record = iter.next
        count += 1
        writer.write(record._1.asInstanceOf[AnyRef], record._2.asInstanceOf[AnyRef])
      }
    
      writer.close()
      return writer
    }

    self.context.runJob(self, writeToFile _ ).foreach(_.commit())
    writer.cleanup()
  }

  def getKeyClass() = implicitly[ClassManifest[K]].erasure

  def getValueClass() = implicitly[ClassManifest[V]].erasure
}

class MappedValuesRDD[K, V, U](
  prev: RDD[(K, V)], f: V => U)
extends RDD[(K, U)](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override val partitioner = prev.partitioner
  override def compute(split: Split) =
    prev.iterator(split).map{case (k, v) => (k, f(v))}
  override def setContext(newContext: SparkContext): MappedValuesRDD[K, V, U] =
    new MappedValuesRDD(prev.setContext(newContext), f)
}

class FlatMappedValuesRDD[K, V, U](
  prev: RDD[(K, V)], f: V => Traversable[U])
extends RDD[(K, U)](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override val partitioner = prev.partitioner
  override def compute(split: Split) = {
    prev.iterator(split).toStream.flatMap { 
      case (k, v) => f(v).map(x => (k, x))
    }.iterator
  }
  override def setContext(newContext: SparkContext): FlatMappedValuesRDD[K, V, U] =
    new FlatMappedValuesRDD(prev.setContext(newContext), f)
}

object Manifests {
  val seqSeqManifest = classManifest[Seq[Seq[_]]]
}
