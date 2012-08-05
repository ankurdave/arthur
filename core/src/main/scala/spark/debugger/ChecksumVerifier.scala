package spark.debugger

import scala.collection.mutable
import scala.collection.immutable

import spark.Logging

/**
 * Verifies a stream of checksum events and detects mismatches, where
 * the same entity has multiple checksums.
 */
class ChecksumVerifier extends Logging {
  val checksums = new mutable.HashMap[Any, immutable.HashSet[ChecksumEvent]]
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
