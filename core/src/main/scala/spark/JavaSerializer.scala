package spark

import java.io._
import java.nio.ByteBuffer

import spark.util.ByteBufferInputStream

class JavaSerializationStream(out: OutputStream) extends SerializationStream {
  val objOut = new ObjectOutputStream(out)
  def writeObject[T](t: T) { objOut.writeObject(t) }
  def flush() { objOut.flush() }
  def close() { objOut.close() }
}

class JavaDeserializationStream(in: InputStream, loader: ClassLoader)
extends DeserializationStream {
  val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) =
      Class.forName(desc.getName, false, loader)
  }

  def readObject[T](): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }
}

class JavaSerializerInstance extends SerializerInstance {
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
    new JavaSerializationStream(s)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, Thread.currentThread.getContextClassLoader)
  }

  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream(s, loader)
  }
}

class JavaSerializer extends Serializer {
  def newInstance(): SerializerInstance = new JavaSerializerInstance
}
