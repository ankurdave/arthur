package spark.scheduler

import spark._

class ResultTask[T, U](
    stageId: Int,
    val rdd: RDD[T],
    val func: (TaskContext, Iterator[T]) => U,
    val partition: Int,
    @transient locs: Seq[String],
    val outputId: Int)
  extends Task[U](stageId) {
  
  val split = rdd.splits(partition)

  override def run(attemptId: Long): U = {
    val context = new TaskContext(stageId, partition, attemptId)
    func(context, rdd.iterator(split))
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ResultTask(" + stageId + ", " + partition + ")"
}
