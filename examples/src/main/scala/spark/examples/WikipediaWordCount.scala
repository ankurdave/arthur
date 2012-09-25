package spark.examples

import spark._
import SparkContext._

object WikipediaWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: WikipediaWordCount <host> <filename>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "WikipediaWordCount")
    val start = System.currentTimeMillis
    val w = sc.textFile(args(1))
    val words = w.map(_.split("\t")(3)).flatMap(_.split("[^a-zA-Z]+").map(_.toLowerCase)).map(word => (word, 1))
    val counts = words.reduceByKey(_ + _).map(pair => (pair._2, pair._1))
    val sorted = counts.sortByKey(false)
    println(counts.take(10))
    val end = System.currentTimeMillis
    println("Took " + (end - start) + " ms")
    System.exit(0)
  }
}
