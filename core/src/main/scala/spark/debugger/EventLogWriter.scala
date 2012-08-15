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
  val subscribers = new ArrayBuffer[EventLogEntry => Unit]

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
      logWarning("Event log %s already exists".format(path))
      eventLog = None
    }
  }

  def subscribe(callback: EventLogEntry => Unit) {
    subscribers.append(callback)
  }

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

  def flush() {
    synchronized {
      for (l <- eventLog) {
        l.flush()
      }
    }
  }

  def stop() {
    synchronized {
      for (l <- eventLog) {
        l.close()
      }
    }
  }
}
