package spark

import java.net.URL
import java.io.EOFException
import java.io.ObjectInputStream
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

sealed trait CoGroupSplitDep[K, V] extends Serializable
case class NarrowCoGroupSplitDep[K, V](rdd: RDD[(K, V1)] forSome { type V1 <: V }, split: Split) extends CoGroupSplitDep[K, V]
case class ShuffleCoGroupSplitDep[K, V](shuffleId: Int) extends CoGroupSplitDep[K, V]

class CoGroupSplit[K, V](idx: Int, val deps: Seq[CoGroupSplitDep[K, V]]) extends Split with Serializable {
  override val index = idx
  override def hashCode(): Int = idx
}

class CoGroupAggregator[K, V]
  extends Aggregator[K, V, ArrayBuffer[V]] (
    { x => ArrayBuffer(x) },
    { (b, x) => b += x },
    { (b1, b2) => b1 ++ b2 })
  with Serializable

class CoGroupedRDD[K, V](rdds: Seq[RDD[(K, V1)] forSome { type V1 <: V }], part: Partitioner)
  extends RDD[(K, Seq[Seq[V1] forSome { type V1 <: V }])](rdds.head.context) with Logging {
  
  override val dependencies: List[Dependency[(K, V1)] forSome { type V1 <: V }] = {
    val deps = new ArrayBuffer[Dependency[(K, V1)] forSome { type V1 <: V }]
    for ((rdd, index) <- rdds.zipWithIndex) {
      if (rdd.partitioner == Some(part)) {
        logInfo("Adding one-to-one dependency with " + rdd)
        deps += new OneToOneDependency(rdd)
      } else {
        logInfo("Adding shuffle dependency with " + rdd)
        deps += new ShuffleDependency(
            context.newShuffleId, rdd.asInstanceOf[RDD[(K, V)]], new CoGroupAggregator[K, V], part)
      }
    }
    deps.toList
  }

  override def mapDependencies(g: RDD ~> RDD) = new CoGroupedRDD[K, V](rdds.map(rdd => g(rdd)), part)

  override def tagged(tagger: RDDTagger): RDD[Tagged[(K, Seq[Seq[V1] forSome { type V1 <: V }])]] = {
    val taggedRDDs: Seq[RDD[(K, Tagged[V1])] forSome { type V1 <: V }] =
      rdds.map(rdd => new SamePartitionMappedRDD(tagger(rdd), (tp: Tagged[(K, V1)] forSome { type V1 <: V }) => tp match {
        case Tagged((k, v), tag) => (k, Tagged(v, tag))
      }))
    val cogrouped: RDD[(K, Seq[Seq[Tagged[V1]] forSome { type V1 <: V }])] =
      new CoGroupedRDD[K, Tagged[V]](taggedRDDs, part)
    new SamePartitionMappedRDD(cogrouped, (pair: (K, Seq[Seq[Tagged[V1]] forSome { type V1 <: V }])) => pair match {
      case (k, seqSeqTagged) =>
        val tag = (for (seqTagged <- seqSeqTagged; tagged <- seqTagged) yield tagged.tag).reduce(_ | _)
        val untaggedValues = seqSeqTagged.map(seqTagged => seqTagged.map(tagged => tagged.elem))
        Tagged((k, untaggedValues), tag)
    })
  }
  
  @transient
  private var splits_ : Array[Split] = {
    val firstRdd = rdds.head
    val array = new Array[Split](part.numPartitions)
    for (i <- 0 until array.size) {
      array(i) = new CoGroupSplit(i, rdds.zipWithIndex.map { case (r, j) =>
        (dependencies(j): Dependency[_]) match {
          case s: ShuffleDependency[_,_,_] =>
            new ShuffleCoGroupSplitDep(s.shuffleId): CoGroupSplitDep[K, V]
          case _ =>
            new NarrowCoGroupSplitDep[K, V](r, r.splits(i)): CoGroupSplitDep[K, V]
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
    val map = new HashMap[K, Seq[ArrayBuffer[V]]]
    def getSeq(k: K): Seq[ArrayBuffer[V]] = {
      map.getOrElseUpdate(k, Array.fill(rdds.size)(new ArrayBuffer[V]))
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

  private def writeObject(stream: java.io.ObjectOutputStream) {
    stream.defaultWriteObject()
    stream match {
      case _: EventLogOutputStream =>
        stream.writeObject(splits_)
      case _ => {}
    }
  }

  private def readObject(stream: java.io.ObjectInputStream) {
    stream.defaultReadObject()
    stream match {
      case s: EventLogInputStream =>
        splits_ = s.readObject().asInstanceOf[Array[Split]]
      case _ => {}
    }
  }

  reportCreation()
}
