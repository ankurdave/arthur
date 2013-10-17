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
// TODO: write a custom partitioner for TagRDD that partitions by taking the key
// and using it directly as the split ID
// TODO: subclass this
abstract class TagRDD[TagType](
  @transient sc: SparkContext,
  numPartitions: Int
) extends RDD[(Int, Array[TagType])](sc) {
  override val partitioner: Option[Partitioner] =
    Some(new ExplicitSplitPartitioner(numPartitions))
}

class BooleanTagRDD[T: ClassManifest](
  val elements: RDD[T],
  p: T => Boolean
) extends TagRDD[Boolean](elements.context, elements.splits.length) {
  override def splits: Array[Split] = elements.splits
  override val dependencies = List(new OneToOneDependency(elements))
  override def compute(split: Split): Iterator[(Int, Array[Boolean])] = {
    List((split.index, elements.iterator(split).map(t => p(t)).toArray)).iterator
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
