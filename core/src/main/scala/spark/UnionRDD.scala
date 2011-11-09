package spark

import scala.collection.mutable.ArrayBuffer

class UnionSplit[T: ClassManifest](idx: Int, rdd: RDD[T], split: Split)
extends Split with Serializable {
  def iterator() = rdd.iterator(split)
  def preferredLocations() = rdd.preferredLocations(split)
  override val index = idx
}

class UnionRDD[T: ClassManifest](sc: SparkContext, rdds: Seq[RDD[T]])
extends RDD[T](sc) with Serializable {
  @transient val splits_ : Array[Split] = {
    val array = new Array[Split](rdds.map(_.splits.size).sum)
    var pos = 0
    for (rdd <- rdds; split <- rdd.splits) {
      array(pos) = new UnionSplit(pos, rdd, split)
      pos += 1
    }
    array
  }

  override def splits = splits_

  override val dependencies = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for ((rdd, index) <- rdds.zipWithIndex) {
      deps += new RangeDependency(rdd, 0, pos, rdd.splits.size) 
      pos += rdd.splits.size
    }
    deps.toList
  }
  
  override def compute(s: Split): Iterator[T] =
    s.asInstanceOf[UnionSplit[T]].iterator()

  override def preferredLocations(s: Split): Seq[String] =
    s.asInstanceOf[UnionSplit[T]].preferredLocations()

  override def setContext(newContext: SparkContext): UnionRDD[T] =
    new UnionRDD(newContext, rdds.map(_.setContext(newContext)))
}
