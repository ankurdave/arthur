package spark

import java.util.{HashMap => JHashMap}

import spark.debugger.RDDTagger
import spark.debugger.SamePartitionMappedRDD
import spark.debugger.Tagged

class ShuffledRDDSplit(val idx: Int) extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}

class ShuffledRDD[K, V, C](
    @transient @debugger.EventLogSerializable parent: RDD[(K, V)],
    aggregator: Aggregator[K, V, C],
    part : Partitioner) 
  extends RDD[(K, C)](parent.context) {
  //override val partitioner = Some(part)
  override val partitioner = Some(part)
  
  @transient @debugger.EventLogSerializable
  val splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_
  
  override def preferredLocations(split: Split) = Nil
  
  val dep = new ShuffleDependency(context.newShuffleId, parent, aggregator, part)
  override val dependencies = List(dep)

  override def compute(split: Split): Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    val fetcher = SparkEnv.get.shuffleFetcher

    if (aggregator.mapSideCombine) {
      // Apply combiners on map partitions. In this case, post-shuffle we get a
      // list of outputs from the combiners and merge them using mergeCombiners.
      def mergePairWithMapSideCombiners(k: K, c: C) {
        val oldC = combiners.get(k)
        if (oldC == null) {
          combiners.put(k, c)
        } else {
          combiners.put(k, aggregator.mergeCombiners(oldC, c))
        }
      }
      fetcher.fetch[K, C](dep.shuffleId, split.index, mergePairWithMapSideCombiners)
    } else {
      // Do not apply combiners on map partitions (i.e. map side aggregation is
      // turned off). Post-shuffle we get a list of values and we use mergeValue
      // to merge them.
      def mergePairWithoutMapSideCombiners(k: K, v: V) {
        val oldC = combiners.get(k)
        if (oldC == null) {
          combiners.put(k, aggregator.createCombiner(v))
        } else {
          combiners.put(k, aggregator.mergeValue(oldC, v))
        }
      }
      fetcher.fetch[K, V](dep.shuffleId, split.index, mergePairWithoutMapSideCombiners)
    }

    return new Iterator[(K, C)] {
      var iter = combiners.entrySet().iterator()

      def hasNext: Boolean = iter.hasNext()

      def next(): (K, C) = {
        val entry = iter.next()
        (entry.getKey, entry.getValue)
      }
    }
  }

  override def tagged(tagger: RDDTagger): RDD[Tagged[(K, C)]] = {
    val taggedParent: RDD[(K, Tagged[V])] =
      new SamePartitionMappedRDD[(K, Tagged[V]), Tagged[(K, V)]](tagger(parent), {
        case Tagged((k, v), tag) => (k, Tagged(v, tag))
      })
    val tagAggregator: Aggregator[K, Tagged[V], Tagged[C]] = new Aggregator(
      { case Tagged(v, tag) => Tagged(aggregator.createCombiner(v), tag) },
      {
        case (Tagged(c, cTag), Tagged(v, vTag)) =>
          Tagged(aggregator.mergeValue(c, v), cTag union vTag)
      },
      {
        case (Tagged(c1, c1Tag), Tagged(c2, c2Tag)) =>
          Tagged(aggregator.mergeCombiners(c1, c2), c1Tag union c2Tag)
      }
    )
    val shuffled: RDD[(K, Tagged[C])] = new ShuffledRDD(taggedParent, tagAggregator, part)
    new SamePartitionMappedRDD[Tagged[(K, C)], (K, Tagged[C])](shuffled, {
      case (k, Tagged(c, tag)) => Tagged((k, c), tag)
    })
  }
}
