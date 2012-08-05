package spark.debugger

import java.io._

import scala.collection.mutable
import scala.collection.immutable

import spark.Logging

/**
 * Writes events to an event log on disk.
 */
class EventLogWriter extends Logging {
  private var eventLog: Option[EventLogOutputStream] = None
  setEventLogPath(Option(System.getProperty("spark.arthur.logPath")))
  private val checksumVerifier = new ChecksumVerifier

  def setEventLogPath(eventLogPath: Option[String]) {
    eventLog =
      for {
        elp <- eventLogPath
        file = new File(elp)
        if !file.exists
      } yield new EventLogOutputStream(new FileOutputStream(file))
  }

  def log(entry: EventLogEntry) {
    for (l <- eventLog) {
      l.writeObject(entry)
    }

    entry match {
      case c: ChecksumEvent => checksumVerifier.verify(c)
      case _ => {}
    }
  }

  def flush() {
    for (l <- eventLog) {
      l.flush()
    }
  }

  def stop() {
    for (l <- eventLog) {
      l.close()
    }
  }
}
