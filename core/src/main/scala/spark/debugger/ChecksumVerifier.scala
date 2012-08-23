package spark.debugger

import scala.collection.immutable
import scala.collection.mutable

import spark.Logging

/**
 * Verifies a stream of checksum events and detects mismatches, which occur when the same entity has
 * multiple checksums.
 */
class ChecksumVerifier extends Logging {
  /** Map from entities to the checksum events associated with that entity. */
  val checksums = new mutable.HashMap[Any, immutable.HashSet[ChecksumEvent]]
  /**
   * List of checksum events that had the same checksum as an existing event associated with the
   * same entity.
   */
  val mismatches = new mutable.ArrayBuffer[ChecksumEvent]

  def verify(c: ChecksumEvent) {
    if (checksums.contains(c.key)) {
      if (!checksums(c.key).contains(c)) {
        if (checksums(c.key).exists(_.mismatch(c))) reportMismatch(c)
        checksums(c.key) += c
      }
    } else {
      checksums.put(c.key, immutable.HashSet(c))
    }
  }

  private def reportMismatch(c: ChecksumEvent) {
    logWarning(c.warningString)
    mismatches += c
  }
}
