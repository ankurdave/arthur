package spark.bagel.examples

import scala.collection.mutable.ArrayBuffer
import scala.xml.{XML,NodeSeq}

import java.io._
import java.nio.ByteBuffer

import spark.SparkContext._
import spark._
import spark.bagel.Bagel._
import spark.bagel._
import spark.util.ByteBufferInputStream

/**
 * Runs PageRank on the articles in the  given Freebase WEX-formatted
 * dump of Wikipedia.
 *
 * For best performance, use the custom serializer by setting the
 * spark.serializer property to spark.bagel.examples.WPRSerializer.
 */
object WikipediaPageRankStandalone {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: WikipediaPageRankStandalone <inputFile> <threshold> <numIterations> <host> <usePartitioner>")
      System.exit(-1)
    }

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numIterations = args(2).toInt
    val host = args(3)
    val usePartitioner = args(4).toBoolean
    val sc = new SparkContext(host, "WikipediaPageRankStandalone")

    val input = sc.textFile(inputFile)
    logDebug("Created input")
    logDebug("Number of articles: " + input.count())
    val partitioner = new HashPartitioner(sc.defaultParallelism)
    val links =
      if (usePartitioner)
        input.map(parseArticle _).partitionBy(partitioner).cache
      else
        input.map(parseArticle _).cache
    logDebug("Created links")
    val n = links.count
    val defaultRank = 1.0 / n
    val a = 0.15

    // Do the computation
    val startTime = System.currentTimeMillis
    logDebug("Running PageRank")
    val ranks =
        pageRank(links, numIterations, defaultRank, a, n, partitioner, usePartitioner, sc.defaultParallelism)

    // Print the result
    System.err.println("Articles with PageRank >= "+threshold+":")
    logDebug("Collecting results")
    val top =
      (ranks
       .filter { case (id, rank) => rank >= threshold }
       .map { case (id, rank) => "%s\t%s\n".format(id, rank) }
       .collect.mkString)
    println(top)

    val time = (System.currentTimeMillis - startTime) / 1000.0
    println("Completed %d iterations in %f seconds: %f seconds per iteration"
            .format(numIterations, time, time / numIterations))
    System.exit(0)
  }

  def parseArticle(line: String): (String, Array[String]) = {
    val fields = line.split("\t")
    val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
    val id = new String(title)
    val links =
      if (body == "\\N")
        NodeSeq.Empty
      else
        try {
          XML.loadString(body) \\ "link" \ "target"
        } catch {
          case e: org.xml.sax.SAXParseException =>
            System.err.println("Article \""+title+"\" has malformed XML in body:\n"+body)
          NodeSeq.Empty
        }
    val outEdges = links.map(link => new String(link.text)).toArray
    (id, outEdges)
  }

  def pageRank(
    links: RDD[(String, Array[String])],
    numIterations: Int,
    defaultRank: Double,
    a: Double,
    n: Long,
    partitioner: Partitioner,
    usePartitioner: Boolean,
    numSplits: Int
  ): RDD[(String, Double)] = {
    var ranks = links.mapValues { edges => defaultRank }
    for (i <- 1 to numIterations) {
      logDebug("Starting iteration " + i)
      val contribs = links.groupWith(ranks).flatMap {
        case (id, (linksWrapper, rankWrapper)) =>
          if (linksWrapper.length > 0) {
            if (rankWrapper.length > 0) {
              linksWrapper(0).map(dest => (dest, rankWrapper(0) / linksWrapper(0).size))
            } else {
              linksWrapper(0).map(dest => (dest, defaultRank / linksWrapper(0).size))
            }
          } else {
            Array[(String, Double)]()
          }
      }
      ranks = (contribs.combineByKey((x: Double) => x,
                                     (x: Double, y: Double) => x + y,
                                     (x: Double, y: Double) => x + y,
                                     partitioner)
               .mapValues(sum => a/n + (1-a)*sum))
    }
    ranks
  }
}

class WPRSerializer extends spark.Serializer {
  def newInstance(): SerializerInstance = new WPRSerializerInstance()
}

class WPRSerializerInstance extends SerializerInstance {
  def serialize[T](t: T): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    ByteBuffer.wrap(bos.toByteArray)
  }

  def deserialize[T](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject().asInstanceOf[T]
  }

  def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis, loader)
    in.readObject().asInstanceOf[T]
  }

  def serializeStream(s: OutputStream): SerializationStream = {
    new WPRSerializationStream(s)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    new WPRDeserializationStream(s, Thread.currentThread.getContextClassLoader)
  }

  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new WPRDeserializationStream(s, loader)
  }
}

class WPRSerializationStream(os: OutputStream) extends SerializationStream {
  val dos = new DataOutputStream(os)
  val jss = new JavaSerializationStream(os)

  def writeObject[T](t: T): Unit = t match {
    case (id: String, ArrayBuffer(links: Array[String])) =>
      dos.writeInt(0)
      dos.writeUTF(id)
      dos.writeInt(links.length)
      for (link <- links) {
        dos.writeUTF(link)
      }
    case (id: String, ArrayBuffer(rank: Double)) =>
      dos.writeInt(1)
      dos.writeUTF(id)
      dos.writeDouble(rank)
    case (id: String, rank: Double) =>
      dos.writeInt(2)
      dos.writeUTF(id)
      dos.writeDouble(rank)
    case _ =>
      dos.writeInt(3)
      dos.flush()
      jss.writeObject(t)
      jss.flush()
  }

  def flush() { dos.flush() }
  def close() { dos.close() }
}

class WPRDeserializationStream(is: InputStream, loader: ClassLoader) extends DeserializationStream {
  val dis = new DataInputStream(is)
  val jds = new JavaDeserializationStream(is, loader)

  def readObject[T](): T = {
    val typeId = dis.readInt()
    typeId match {
      case 0 => {
        val id = dis.readUTF()
        val numLinks = dis.readInt()
        val links = new Array[String](numLinks)
        for (i <- 0 until numLinks) {
          val link = dis.readUTF()
          links(i) = link
        }
        (id, ArrayBuffer(links)).asInstanceOf[T]
      }
      case 1 => {
        val id = dis.readUTF()
        val rank = dis.readDouble()
        (id, ArrayBuffer(rank)).asInstanceOf[T]
      }
      case 2 => {
        val id = dis.readUTF()
        val rank = dis.readDouble()
        (id, rank).asInstanceOf[T]
      }
      case 3 =>
        jds.readObject()
    }
  }

  def close() { dis.close() }
}
