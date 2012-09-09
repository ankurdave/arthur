package spark.debugger

import scala.collection.immutable

import spark.OneToOneDependency
import spark.RDD
import spark.Split

// TODO(ankurdave): Improve Tagged.toString
case class Tagged[+A](val elem: A, val tag: immutable.HashSet[Int]) extends Product2[Any, Any] {
  def map[B](f: A => B): Tagged[B] = Tagged(f(elem), tag)

  def flatMap[B](f: A => TraversableOnce[B]) =
    for (newElem <- f(elem)) yield Tagged(newElem, tag)

  override def _1: Any = elem match {
    case pair: Product2[_,_] => pair._1
    case _ => null
  }
  override def _2: Any = elem match {
    case pair: Product2[_,_] => Tagged(pair._2, tag)
    case _ => null
  }
  override def canEqual(that: Any) = that.isInstanceOf[Tagged[_]]
}

object Tagged {
  type Tag = Int
  type TagSet = immutable.HashSet[Tag]
}

case class OrderedTagged[A <% Ordered[A]](
  val elem: A,
  val tag: immutable.HashSet[Int]
) extends Ordered[OrderedTagged[A]] {
  def compare(that: OrderedTagged[A]) = this.elem.compare(that.elem)
}

trait RDDTagger {
  def apply[A](a: RDD[A]): RDD[Tagged[A]]
}

class UniquelyTaggedRDD[T](prev: RDD[T]) extends RDD[Tagged[T]](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = {
    val tagRangeLength = Int.MaxValue / numSplits
    val startTag = tagRangeLength * split.index
    prev.iterator(split).zipWithIndex.map {
      case (elem, i) => Tagged(elem, immutable.HashSet(startTag + i))
    }
  }

  private val numSplits = splits.length
}

/**
 * Maps the prev RDD assuming that f keeps the element in the same partition. This allows it to
 * reuse the partitioner.
 */
class SamePartitionMappedRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: T => U)
  extends RDD[U](prev.context) {

  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  // override val partitioner = prev.partitioner
  override def compute(split: Split) = prev.iterator(split).map(f)
  override def tagged(tagger: RDDTagger) =
    new SamePartitionMappedRDD(tagger(prev), (taggedT: Tagged[T]) => taggedT.map(f))
}
