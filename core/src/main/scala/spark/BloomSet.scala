package spark

import java.io.{InputStream, OutputStream}

import java.util.Random

import scala.collection.immutable.Vector

import BloomSet._

class BloomSet[A] private (val size: Int, val k: Int, private val contents: Vector[Boolean]) extends ((A)=>Boolean) with Serializable {
  val width = contents.length
  
  /**
   * <p>A value between 0 and 1 which estimates the accuracy of the bloom filter.
   * This estimate is precisely the inverse of the probability function for
   * a bloom filter of a given size, width and number of hash functions.  The
   * probability function given by the following expression in LaTeX math syntax:</p>
   * 
   * <p><code>(1 - e^{-kn/m})^k</code> where <i>k</i> is the number of hash functions,
   * <i>n</i> is the number of elements in the bloom filter and <i>m</i> is the
   * width.</p>
   * 
   * <p>It is important to remember that this is only an estimate of the accuracy.
   * Likewise, it assumes perfectly ideal hash functions, thus it is somewhat
   * more optimistic than the reality of the implementation.</p>
   */
  lazy val accuracy = {
    val exp = ((k:Double) * size) / width
    val probability = Math.pow(1 - Math.exp(-exp), k)
    
    1d - probability
  }
  
  /**
   * Returns the optimal value of <i>k</i> for a bloom filter with the current
   * properties (width and size).  Useful in reducing false-positives on sets
   * with a limited range in size.
   */
  lazy val optimalK = {
    val num = (9:Double) * width
    val dem = (13:Double) * size
    Math.max((num / dem).intValue, 1)
  }
  
  def this(width: Int, k: Int) = this(0, k, alloc(width))
  
  def +(e: A) = new BloomSet[A](size + 1, k, add(contents)(e))
  
  def ++(col: Iterable[A]) = {
    var length = 0
    val newContents = col.foldLeft(contents) { (c, e) =>
      length += 1
      add(c)(e)
    }
    
    new BloomSet[A](size + length, k, newContents)
  }
  
  /**
   * Computes the union of two bloom filters and returns the result.  Note that
   * this operation is only defined for filters of the same width.  Filters which
   * have a different value of <i>k</i> (different number of hash functions) can
   * be unioned, but the result will have a higher probability of false positives
   * than either of the operands.  The <i>k</i> value of the resulting filter is
   * computed to be the minimum <i>k</i> of the two operands.  The <i>size</i> of
   * the resulting filter is precisely the combined size of the operands.  This
   * of course means that for sets with intersecting items the size will be
   * slightly large.
   */
  def |(set: BloomSet[A]) = {
    if (set.width != width) {
      throw new IllegalArgumentException("Bloom filter union is only defined for " +
          "sets of the same width (" + set.width + " != " + width + ")")
    }
    
    val newContents = contents.zip(set.contents).map { case (a, b) => a || b }
    
    new BloomSet[A](size + set.size, Math.min(set.k, k), newContents)    // min guarantees no false negatives
  }

  def intersection(set: BloomSet[A]) = {
    if (set.width != width) {
      throw new IllegalArgumentException("Bloom filter intersection is only defined for " +
          "sets of the same width (" + set.width + " != " + width + ")")
    }
    
    val newContents = contents.zip(set.contents).map { case (a, b) => a && b }
    
    new BloomSet[A](size + set.size, Math.min(set.k, k), newContents)
  }
  
  def contains(e: A) = {
    (0 until k).foldLeft(true) { (acc, i) => 
      acc && contents(hash(e, i, contents.length)) 
    }
  }

  def nonEmpty = {
    contents.foldLeft(false) { _ || _ }
  }

  def apply(e: A) = contains(e)
  
  override def equals(other: Any) = other match {
    case set: BloomSet[A] => {
      val back = (size == set.size) &&
                 (k == set.k) &&
                 (contents.length == set.contents.length)
      
      (0 until contents.length).foldLeft(back) { (acc, i) =>
        acc && (contents(i) == set.contents(i))
      }
    }
    
    case _ => false
  }
  
  override def hashCode = {
    (0 until width).foldLeft(size ^ k ^ width) { (acc, i) =>
      acc ^ (if (contents(i)) i else 0)
    }
  }
  
  protected def add(contents: Vector[Boolean])(e: Any) = {
    var back = contents
    
    for (i <- 0 until k) {
      back = back.updated(hash(e, i, back.length), true)
    }
    
    back
  }
}

object BloomSet {
  def apply[A](e: A*) = e.foldLeft(new BloomSet[A](200, 4)) { _ + _ }
  
  def load[A](is: InputStream) = {
    val buf = new Array[Byte](4)
    
    is.read(buf)
    val size = convertToInt(buf)
    
    is.read(buf)
    val k = convertToInt(buf)
    
    is.read(buf)
    val width = convertToInt(buf)
    
    var contents = Vector[Boolean]()
    for (_ <- 0 until (width / 8)) {
      var num = is.read()
      var buf: List[Boolean] = Nil
      
      for (_ <- 0 until 8) {
        buf = ((num & 1) == 1) :: buf
        num >>= 1
      }
      
      contents = contents ++ buf
    }
    
    if (width % 8 != 0) {
      var buf: List[Boolean] = Nil
      var num = is.read()
      
      for (_ <- 0 until (width % 8)) {
        buf = ((num & 1) == 1) :: buf
        num >>= 1
      }
      
      contents = contents ++ buf
    }
    
    new BloomSet[A](size, k, contents)
  }
  
  private[spark] def convertToBytes(i: Int) = {
    val buf = new Array[Byte](4)
    
    buf(0) = ((i & 0xff000000) >>> 24).byteValue
    buf(1) = ((i & 0x00ff0000) >>> 16).byteValue
    buf(2) = ((i & 0x0000ff00) >>> 8).byteValue
    buf(3) = ((i & 0x000000ff)).byteValue
    
    buf
  }
  
  private[spark] def convertToInt(buf: Array[Byte]) = {
    ((buf(0) & 0xFF) << 24) |
        ((buf(1) & 0xFF) << 16) |
        ((buf(2) & 0xFF) << 8) |
        (buf(3) & 0xFF)
  }
  
  private[spark] def alloc(size: Int) = {
    Vector.fill(size) { false }
  }
  
  private[spark] def hash(e: Any, iters: Int, bounds: Int): Int = {
    val rand = new Random(e.hashCode)
    (0 until iters).foldLeft(rand.nextInt(bounds)) { (x, y) => rand.nextInt(bounds) }
  }
}
