package spark

import java.io.EOFException
import java.io.ObjectInputStream
import java.net.URL

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import spark.debugger.RDDTagger
import spark.debugger.SamePartitionMappedRDD
import spark.debugger.Tagged

sealed trait CoGroupSplitDep[K, V] extends Serializable
case class NarrowCoGroupSplitDep[K, V](
    rdd: RDD[(K, V1)] forSome { type V1 <: V },
    split: Split)
  extends CoGroupSplitDep[K, V]
case class ShuffleCoGroupSplitDep[K, V](shuffleId: Int) extends CoGroupSplitDep[K, V]

class CoGroupSplit[K, V](idx: Int, val deps: Seq[CoGroupSplitDep[K, V]])
  extends Split with Serializable {

  override val index: Int = idx
  override def hashCode(): Int = idx
}

class CoGroupAggregator[K, V]
  extends Aggregator[K, V, ArrayBuffer[V]] (
    { x => ArrayBuffer(x) },
    { (b, x) => b += x },
    { (b1, b2) => b1 ++ b2 })
  with Serializable

class CoGroupedRDD[K, V](
    @transient @debugger.EventLogSerializable rdds: Seq[RDD[(K, V1)] forSome { type V1 <: V }],
    part: Partitioner)
  extends RDD[(K, Seq[Seq[V1] forSome { type V1 <: V }])](rdds.head.context) with Logging {
  
  val aggr = new CoGroupAggregator[K, V]
  
  @transient @debugger.EventLogSerializable
  override val dependencies: List[Dependency[(K, V1)] forSome { type V1 <: V }] = {
    val deps = new ArrayBuffer[Dependency[(K, V1)] forSome { type V1 <: V }]
    for ((rdd, index) <- rdds.zipWithIndex) {
      if (rdd.partitioner == Some(part)) {
        logInfo("Adding one-to-one dependency with " + rdd)
        deps += new OneToOneDependency(rdd)
      } else {
        logInfo("Adding shuffle dependency with " + rdd)
        // rdd is an RDD[(K, V1)] where V1 is a subclass of V, and any (K, V1) is also a (K, V), so
        // it's safe to cast rdd to RDD[(K, V)].
        deps += new ShuffleDependency(
            context.newShuffleId, rdd.asInstanceOf[RDD[(K, V)]], aggr, part)
      }
    }
    deps.toList
  }
  
  @transient @debugger.EventLogSerializable
  val splits_ : Array[Split] = {
    val firstRdd = rdds.head
    val array = new Array[Split](part.numPartitions)
    for (i <- 0 until array.size) {
      array(i) = new CoGroupSplit(i, rdds.zipWithIndex.map { case (r, j) =>
        (dependencies(j): Dependency[_]) match {
          case s: ShuffleDependency[_, _, _] =>
            new ShuffleCoGroupSplitDep(s.shuffleId): CoGroupSplitDep[K, V]
          case _ =>
            new NarrowCoGroupSplitDep(r, r.splits(i)): CoGroupSplitDep[K, V]
        }
      }.toList)
    }
    array
  }

  override def splits = splits_
  
  override val partitioner = Some(part)
  
  override def preferredLocations(s: Split) = Nil
  
  override def compute(s: Split): Iterator[(K, Seq[Seq[V]])] = {
    val split = s.asInstanceOf[CoGroupSplit[K, V]]
    val numRdds = split.deps.size
    val map = new HashMap[K, Seq[ArrayBuffer[V]]]
    def getSeq(k: K): Seq[ArrayBuffer[V]] = {
      map.getOrElseUpdate(k, Array.fill(numRdds)(new ArrayBuffer[V]))
    }
    for ((dep, depNum) <- split.deps.zipWithIndex) (dep: CoGroupSplitDep[K, V]) match {
      case NarrowCoGroupSplitDep(rdd, itsSplit) => {
        // Read them from the parent
        for ((k, v) <- (rdd: RDD[(K, V1)] forSome { type V1 <: V }).iterator(itsSplit)) {
          getSeq(k: K)(depNum) += (v: V)
        }
      }
      case ShuffleCoGroupSplitDep(shuffleId) => {
        // Read map outputs of shuffle
        def mergePair(k: K, vs: Seq[V1] forSome { type V1 <: V }) {
          val mySeq = getSeq(k)
          for (v <- vs)
            mySeq(depNum) += v
        }
        val fetcher = SparkEnv.get.shuffleFetcher
        fetcher.fetch[K, Seq[V1] forSome { type V1 <: V }](shuffleId, split.index, mergePair)
      }
    }
    map.iterator
  }

  override def tagged(tagger: RDDTagger)
      : RDD[Tagged[(K, Seq[Seq[V1] forSome { type V1 <: V }])]] = {
    val taggedRDDs: Seq[RDD[(K, Tagged[V1])] forSome { type V1 <: V }] =
      rdds.map(rdd => new SamePartitionMappedRDD(
        tagger(rdd),
        (taggedPair: Tagged[(K, V1)] forSome { type V1 <: V }) => taggedPair match {
          case Tagged((k, v), tag) => (k, Tagged(v, tag))
        }
      ))
    val cogrouped: RDD[(K, Seq[Seq[Tagged[V1]] forSome { type V1 <: V }])] =
      new CoGroupedRDD[K, Tagged[V]](taggedRDDs, part)
    new SamePartitionMappedRDD(
      cogrouped,
      (pair: (K, Seq[Seq[Tagged[V1]] forSome { type V1 <: V }])) => pair match {
        case (k, seqSeqTagged) =>
          val tag =
            (for (seqTagged <- seqSeqTagged; tagged <- seqTagged)
             yield tagged.tag).reduce(_ union _)
          val untaggedValues = seqSeqTagged.map(seqTagged => seqTagged.map(tagged => tagged.elem))
          Tagged((k, untaggedValues), tag)
      }
    )
  }
}
