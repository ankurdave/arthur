package spark.scheduler.cluster

/**
 * Information about a running task attempt inside a TaskSet.
 */
class TaskInfo(val taskId: Long, val index: Int, val launchTime: Long, val host: String) {
  var finishTime: Long = 0
  var failed = false

  def markSuccessful(time: Long = System.currentTimeMillis) {
    finishTime = time
  }

  def markFailed(time: Long = System.currentTimeMillis) {
    finishTime = time
    failed = true
  }

  def finished: Boolean = finishTime != 0

  def successful: Boolean = finished && !failed

  def running: Boolean = !finished

  def duration: Long = {
    if (!finished) {
      throw new UnsupportedOperationException("duration() called on unfinished tasks")
    } else {
      finishTime - launchTime
    }
  }

  def timeRunning(currentTime: Long): Long = currentTime - launchTime
}
