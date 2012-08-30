package spark.debugger

import java.io._
import java.util.UUID

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import spark.Logging

/**
 * Writes events to an event log on disk. Logging is thread-safe.
 */
class EventLogWriter extends Logging {
  private var eventLog: Option[EventLogOutputStream] = None
  setEventLogPath(Option(System.getProperty("spark.debugger.logPath")))
  private val checksumVerifier = new ChecksumVerifier
  private val subscribers = new ArrayBuffer[EventLogEntry => Unit]

  /** Sets the path where events will be written. If eventLogPath is None, uses a default path. */
  def setEventLogPath(eventLogPath: Option[String]) {
    val path = eventLogPath.getOrElse {
      val dir = System.getProperty("spark.local.dir", System.getProperty("java.io.tmpdir"))
      new File(dir, "spark-debugger-event-log-" + UUID.randomUUID).getPath
    }
    val file = new File(path)
    if (!file.exists) {
      logInfo("Writing to event log %s".format(path))
      eventLog = Some(new EventLogOutputStream(new FileOutputStream(file)))
    } else {
      logWarning("Event log %s already exists, future log entries will be dropped".format(path))
      eventLog = None
    }
  }

  /** Registers a callback to be invoked whenever an event is logged. */
  def subscribe(callback: EventLogEntry => Unit) {
    subscribers.append(callback)
  }

  /** Logs the given event and invokes all registered subscribers. */
  def log(entry: EventLogEntry) {
    synchronized {
      for (l <- eventLog) {
        l.writeObject(entry)
      }
      entry match {
        case c: ChecksumEvent => checksumVerifier.verify(c)
        case _ => {}
      }
      for (s <- subscribers) {
        s(entry)
      }
    }
  }

  /** Flushes the event log to disk. */
  def flush() {
    synchronized {
      for (l <- eventLog) {
        l.flush()
      }
    }
  }

  /** Closes the event log file. Subsequent attempts to log events will fail. */
  def stop() {
    synchronized {
      for (l <- eventLog) {
        l.close()
      }
    }
  }
}
