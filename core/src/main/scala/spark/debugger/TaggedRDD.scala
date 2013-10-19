package spark.debugger

import scala.collection.immutable

import spark.OneToOneDependency
import spark.Partitioner
import spark.RDD
import spark.SparkContext
import spark.Split

class TaggedRDD[T: ClassManifest, TagType](
  val elements: RDD[T],
  val tags: TagRDD[TagType]
) extends RDD[(T, TagType)](elements.context) {
  override def splits = elements.splits
  override val dependencies = List(
    new OneToOneDependency(elements),
    new OneToOneDependency(tags))
  override def compute(split: Split) = {
    val elementsIterator = elements.iterator(split)
    val tagsIterator = tags.iterator(split).next()._2.iterator
    elementsIterator.zip(tagsIterator)
  }
}

/**
  * An RDD composed of a sequence of tags intended to correspond one-to-one with
  * elements of another RDD containing the elements to tag.
  *
  * Internally represented as a pair RDD keyed on the split ID of the elements
  * RDD, where values are Arrays of the same size as the corresponding
  * partitions of the elements RDD.
  */
abstract class TagRDD[TagType](
  @transient sc: SparkContext,
  @transient splits_ : Array[Split]
) extends RDD[(Int, Array[TagType])](sc) {
  override def splits: Array[Split] = splits_
  override val partitioner: Option[Partitioner] =
    Some(new ExplicitSplitPartitioner(splits_.length))
}

class OneToOneTagRDD[TagType: ClassManifest](
  val elements: RDD[_],
  val tagger: RDDTagger[TagType]
) extends TagRDD[TagType](elements.context, elements.splits) {
  override val dependencies = List(new OneToOneDependency(elements))
  override def compute(split: Split): Iterator[(Int, Array[TagType])] = {
    List((split.index, elements.iterator(split).map(t => tagger(t)).toArray)).iterator
  }
}

class ExplicitSplitPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any): Int = key.asInstanceOf[Int]

  override def equals(other: Any): Boolean = other match {
    case esp: ExplicitSplitPartitioner =>
      esp.numPartitions == numPartitions
    case _ =>
      false
  }
}

class FlatMappedTagRDD[U: ClassManifest, T: ClassManifest, TagType: ClassManifest](
  prev: TaggedRDD[T, TagType],
  f: T => TraversableOnce[U]
) extends TagRDD[TagType](prev.context, prev.splits) {
  override val dependencies = List(
    new OneToOneDependency(prev))
  override def compute(split: Split): Iterator[(Int, Array[TagType])] = {
    val newTags =
      for {
        (oldElem, oldTag) <- prev.iterator(split)
        newElem <- f(oldElem)
      } yield oldTag
    List((split.index, newTags.toArray)).iterator
  }
}
