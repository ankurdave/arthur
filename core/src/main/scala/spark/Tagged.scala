package spark

case class Tagged[A](val elem: A, val tag: Boolean) {
  def map[B](f: A => B): Tagged[B] = Tagged(f(elem), tag)

  def flatMap[B](f: A => Traversable[B]) =
    for (newElem <- f(elem)) yield Tagged(newElem, tag)
}
