package spark

import scala.collection.immutable

case class Tagged[+A](val elem: A, val tag: immutable.HashSet[Int]) {
  def map[B](f: A => B): Tagged[B] = Tagged(f(elem), tag)

  def flatMap[B](f: A => Traversable[B]) =
    for (newElem <- f(elem)) yield Tagged(newElem, tag)
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

class UniquelyTaggedRDD[T](prev: RDD[T]) extends RDD[Tagged[T]](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def mapDependencies(g: RDD ~> RDD) = new UniquelyTaggedRDD(g(prev))
  override def compute(split: Split) = {
    val tagRangeLength = Int.MaxValue / numSplits
    val startTag = tagRangeLength * split.index
    prev.iterator(split).zipWithIndex.map {
      case (elem, i) => Tagged(elem, immutable.HashSet(startTag + i))
    }
  }

  private val numSplits = splits.length

  reportCreation()
}
