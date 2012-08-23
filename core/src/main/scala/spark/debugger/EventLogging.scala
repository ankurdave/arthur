package spark.debugger

import java.io._

import spark.scheduler.Task
import spark.RDD
import spark.SparkContext

/** Spark program event that can be logged during execution and later replayed using Arthur. */
sealed trait EventLogEntry

case class ExceptionEvent(exception: Throwable, task: Task[_]) extends EventLogEntry
case class RDDRegistration(rdd: RDD[_]) extends EventLogEntry
case class TaskSubmission(tasks: Seq[Task[_]]) extends EventLogEntry

sealed trait ChecksumEvent extends EventLogEntry {
  /** A hashable key that identifies the entity associated with the checksum event. */
  def key: Any
  def checksum: Int
  /** Returns true if this event has the same key but a different checksum than the given event. */
  def mismatch(other: ChecksumEvent): Boolean
  /** The string to print if this event mismatches with a previous event. */
  def warningString: String
}

/** Checksum of the accumulator updates of a ShuffleMapTask. */
case class ShuffleMapTaskChecksum(
  rddId: Int,
  partition: Int,
  checksum: Int
) extends ChecksumEvent {
  def key = (rddId, partition)
  def mismatch(other: ChecksumEvent) = other match {
    case ShuffleMapTaskChecksum(a, b, otherChecksum) =>
      (rddId, partition) == (a, b) && checksum != otherChecksum
    case _ => false
  }
  def warningString =
    ("Nondeterminism detected in accumulator updates for ShuffleMapTask " +
     "on RDD %d, partition %d".format(rddId, partition))
}

/**
 * Checksum of the output of a ResultTask. funcHash is the hash of the
 * function used to compute the result. This is necessary because two
 * ResultTasks with same RDD ID and partition may compute different
 * functions.
 */
case class ResultTaskChecksum(
  rddId: Int,
  partition: Int,
  funcHash: Int,
  checksum: Int
) extends ChecksumEvent {
  def key = (rddId, partition, funcHash)
  def mismatch(other: ChecksumEvent) = other match {
    case ResultTaskChecksum(a, b, c, otherChecksum) =>
      (rddId, partition, funcHash) == (a, b, c) && checksum != otherChecksum
    case _ => false
  }
  def warningString =
    ("Nondeterminism detected in ResultTask " +
     "on RDD %d, partition %d".format(rddId, partition))
}

case class BlockChecksum(
  blockId: String,
  checksum: Int
) extends ChecksumEvent {
  def key = blockId
  def mismatch(other: ChecksumEvent) = other match {
    case BlockChecksum(otherBlockId, otherChecksum) =>
      blockId == otherBlockId && checksum != otherChecksum
    case _ => false
  }
  def warningString =
    ("Nondeterminism detected in block %d".format(blockId))
}

/**
 * Stream for writing the event log.
 *
 * Certain classes may need to write certain transient fields to the event log. Such classes should
 * implement a special writeObject method (see java.io.Serializable) that takes a
 * java.io.ObjectOutputStream as an argument. They should check the argument's dynamic type; if it
 * is an EventLogOutputStream, they should write the extra fields.
 */
class EventLogOutputStream(out: OutputStream) extends ObjectOutputStream(out)

/**
 * Stream for reading the event log. See EventLogOutputStream.
 */
class EventLogInputStream(in: InputStream, val sc: SparkContext) extends ObjectInputStream(in) {
  override def resolveClass(desc: ObjectStreamClass) =
    Class.forName(desc.getName, false, Thread.currentThread.getContextClassLoader)
}
