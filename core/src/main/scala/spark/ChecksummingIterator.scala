package spark

class ChecksummingIterator[T](rdd: RDD[T], split: Split, underlying: Iterator[T]) extends Iterator[T] {
  var alreadyReportedChecksum = false
  var runningChecksum = 0

  def hasNext = {
    val result = underlying.hasNext
    reportChecksum(result)
    result
  }

  def next(): T = {
    val result = underlying.next
    updateChecksum(result)
    result
  }

  def updateChecksum(nextVal: T) {
    // TODO: use a real hash function
    runningChecksum ^= nextVal.hashCode
    reportChecksum(underlying.hasNext)
  }

  def reportChecksum(hasNext: Boolean) {
    if (!hasNext && !alreadyReportedChecksum) {
      SparkEnv.get.eventReporter.reportRDDChecksum(rdd, split, runningChecksum)
      alreadyReportedChecksum = true
    }
  }
}
