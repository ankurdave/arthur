package spark

import scala.collection.immutable

case class Tagged[A](val elem: A, val tag: immutable.HashSet[Int]) {
  def map[B](f: A => B): Tagged[B] = Tagged(f(elem), tag)

  def flatMap[B](f: A => Traversable[B]) =
    for (newElem <- f(elem)) yield Tagged(newElem, tag)
}

case class OrderedTagged[A <% Ordered[A]](
  val elem: A,
  val tag: immutable.HashSet[Int]
) extends Ordered[OrderedTagged[A]] {
  def compare(that: OrderedTagged[A]) = this.elem.compare(that.elem)
}
