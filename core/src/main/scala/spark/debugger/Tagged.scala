package spark.debugger

import scala.collection.immutable

import spark.OneToOneDependency
import spark.RDD
import spark.Split

// TODO(ankurdave): Improve Tagged.toString
// TODO(ankurdave): Serialize this efficiently
case class Tagged[+A](val elem: A, val tag: Tag) {
  def map[B](f: A => B): Tagged[B] = Tagged(f(elem), tag)

  def flatMap[B](f: A => TraversableOnce[B]) =
    for (newElem <- f(elem)) yield Tagged(newElem, tag)
}

class Tag protected () extends Serializable {
  def union(other: Tag): Tag = this
  def intersect(other: Tag): Tag = this
  def isTagged: Boolean = false
}

object Tag {
  val empty = new Tag()
}

class IntSetTag private () extends Tag {
  private var tags: immutable.HashSet[Int] = immutable.HashSet()

  def this(tags: immutable.HashSet[Int]) = {
    this()
    this.tags = tags
  }
  def this(tag: Int) = {
    this(immutable.HashSet(tag))
  }

  override def union(other: Tag): IntSetTag = {
    other match {
      case ist: IntSetTag =>
        new IntSetTag(tags union ist.tags)
      case t: Tag =>
        this
      case _ =>
        throw new UnsupportedOperationException(
          "Can't union IntSetTag %s with Tag %s".format(this, other))
    }
  }

  override def intersect(other: Tag): IntSetTag = {
    other match {
      case ist: IntSetTag =>
        new IntSetTag(tags intersect ist.tags)
      case t: Tag =>
        IntSetTag.empty
      case _ =>
        throw new UnsupportedOperationException(
          "Can't intersect IntSetTag %s with Tag %s".format(this, other))
    }
  }

  override def isTagged: Boolean = tags.nonEmpty

  override def toString: String = "IntSetTag(%s)".format(tags)
}

object IntSetTag {
  val empty = new IntSetTag()
}

class BooleanTag private () extends Tag {
  private var tag: Boolean = false

  private def this(tag: Boolean) = {
    this()
    this.tag = tag
  }

  override def union(other: Tag): BooleanTag = {
    other match {
      case bt: BooleanTag =>
        BooleanTag(tag || bt.tag)
      case t: Tag =>
        this
      case _ =>
        throw new UnsupportedOperationException(
          "Can't union %s with %s".format(this, other))
    }
  }

  override def intersect(other: Tag): BooleanTag = {
    other match {
      case bt: BooleanTag =>
        BooleanTag(tag && bt.tag)
      case t: Tag =>
        BooleanTag(false)
      case _ =>
        throw new UnsupportedOperationException(
          "Can't intersect BooleanTag %s with Tag %s".format(this, other))
    }
  }

  override def isTagged: Boolean = tag
}

object BooleanTag {
  val falseTag = new BooleanTag(false)
  val trueTag = new BooleanTag(true)
  def apply(tag: Boolean): BooleanTag = if (tag) trueTag else falseTag
}

case class OrderedTagged[A <% Ordered[A]](val elem: A, val tag: Tag)
  extends Ordered[OrderedTagged[A]] {

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
      case (elem, i) => Tagged(elem, new IntSetTag(startTag + i))
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
