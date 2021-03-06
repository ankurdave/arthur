package spark

import java.io._

import scala.collection.mutable.Map

/**
 * A datatype that can be accumulated, i.e. has an commutative and associative +.
 *
 * You must define how to add data, and how to merge two of these together.  For some datatypes, these might be
 * the same operation (eg., a counter).  In that case, you might want to use [[spark.AccumulatorParam]].  They won't
 * always be the same, though -- eg., imagine you are accumulating a set.  You will add items to the set, and you
 * will union two sets together.
 *
 * @param initialValue initial value of accumulator
 * @param param helper object defining how to add elements of type `T`
 * @tparam R the full accumulated data
 * @tparam T partial data that can be added in
 */
class Accumulable[R, T] (
    @transient initialValue: R,
    param: AccumulableParam[R, T])
  extends Serializable {
  
  val id = Accumulators.newId
  @transient
  private var value_ = initialValue // Current value on master
  val zero = param.zero(initialValue)  // Zero value to be passed to workers
  var deserialized = false

  Accumulators.register(this, true)

  /**
   * Add more data to this accumulator / accumulable
   * @param term the data to add
   */
  def += (term: T) { value_ = param.addAccumulator(value_, term) }

  /**
   * Merge two accumulable objects together
   * 
   * Normally, a user will not want to use this version, but will instead call `+=`.
   * @param term the other Accumulable that will get merged with this
   */
  def ++= (term: R) { value_ = param.addInPlace(value_, term)}
  def value = {
    if (!deserialized) value_
    else throw new UnsupportedOperationException("Can't read accumulator value in task")
  }

  private[spark] def localValue = value_

  def value_= (r: R) {
    if (!deserialized) value_ = r
    else throw new UnsupportedOperationException("Can't assign accumulator value in task")
  }
 
  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    value_ = zero
    deserialized = true
    Accumulators.register(this, false)
  }

  override def toString = value_.toString
}

/**
 * Helper object defining how to accumulate values of a particular type.
 *
 * @tparam R the full accumulated data
 * @tparam T partial data that can be added in
 */
trait AccumulableParam[R, T] extends Serializable {
  /**
   * Add additional data to the accumulator value.
   * @param r the current value of the accumulator
   * @param t the data to be added to the accumulator
   * @return the new value of the accumulator
   */
  def addAccumulator(r: R, t: T) : R

  /**
   * Merge two accumulated values together
   * @param r1 one set of accumulated data
   * @param r2 another set of accumulated data
   * @return both data sets merged together
   */
  def addInPlace(r1: R, r2: R): R

  def zero(initialValue: R): R
}

/**
 * A simpler value of [[spark.Accumulable]] where the result type being accumulated is the same
 * as the types of elements being merged.
 *
 * @param initialValue initial value of accumulator
 * @param param helper object defining how to add elements of type `T`
 * @tparam T result type
 */
class Accumulator[T](
  @transient initialValue: T,
  param: AccumulatorParam[T]) extends Accumulable[T,T](initialValue, param)

/**
 * A simpler version of [[spark.AccumulableParam]] where the only datatype you can add in is the same type
 * as the accumulated value
 * @tparam T type of value to accumulate
 */
trait AccumulatorParam[T] extends AccumulableParam[T, T] {
  def addAccumulator(t1: T, t2: T) : T = {
    addInPlace(t1, t2)
  }
}

// TODO: The multi-thread support in accumulators is kind of lame; check
// if there's a more intuitive way of doing it right
private object Accumulators {
  // TODO: Use soft references? => need to make readObject work properly then
  val originals = Map[Long, Accumulable[_, _]]()
  val localAccums = Map[Thread, Map[Long, Accumulable[_, _]]]()
  var lastId: Long = 0
  
  def newId: Long = synchronized {
    lastId += 1
    return lastId
  }

  def register(a: Accumulable[_, _], original: Boolean): Unit = synchronized {
    if (original) {
      originals(a.id) = a
    } else {
      val accums = localAccums.getOrElseUpdate(Thread.currentThread, Map())
      accums(a.id) = a
    }
  }

  // Clear the local (non-original) accumulators for the current thread
  def clear() {
    synchronized {
      localAccums.remove(Thread.currentThread)
    }
  }

  // Get the values of the local accumulators for the current thread (by ID)
  def values: Map[Long, Any] = synchronized {
    val ret = Map[Long, Any]()
    for ((id, accum) <- localAccums.getOrElse(Thread.currentThread, Map())) {
      ret(id) = accum.localValue
    }
    return ret
  }

  // Add values to the original accumulators with some given IDs
  def add(values: Map[Long, Any]): Unit = synchronized {
    for ((id, value) <- values) {
      if (originals.contains(id)) {
        originals(id).asInstanceOf[Accumulable[Any, Any]] ++= value
      }
    }
  }
}
