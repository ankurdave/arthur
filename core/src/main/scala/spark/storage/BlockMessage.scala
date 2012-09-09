package spark.storage

import java.nio.ByteBuffer

import scala.collection.mutable.StringBuilder
import scala.collection.mutable.ArrayBuffer

import spark._
import spark.network._

case class GetBlock(id: String)
case class GotBlock(id: String, data: ByteBuffer)
case class PutBlock(id: String, data: ByteBuffer, level: StorageLevel) 

class BlockMessage() extends Logging{
  // Un-initialized: typ = 0
  // GetBlock: typ = 1
  // GotBlock: typ = 2
  // PutBlock: typ = 3
  private var typ: Int = BlockMessage.TYPE_NON_INITIALIZED
  private var id: String = null
  private var data: ByteBuffer = null
  private var level: StorageLevel = null
 
  initLogging()

  def set(getBlock: GetBlock) {
    typ = BlockMessage.TYPE_GET_BLOCK
    id = getBlock.id
  }

  def set(gotBlock: GotBlock) {
    typ = BlockMessage.TYPE_GOT_BLOCK
    id = gotBlock.id
    data = gotBlock.data
  }

  def set(putBlock: PutBlock) {
    typ = BlockMessage.TYPE_PUT_BLOCK
    id = putBlock.id
    data = putBlock.data
    level = putBlock.level
  }

  def set(buffer: ByteBuffer) {
    val startTime = System.currentTimeMillis
    /*
    println()
    println("BlockMessage: ")
    while(buffer.remaining > 0) {
      print(buffer.get())
    }
    buffer.rewind()
    println()
    println()
    */
    typ = buffer.getInt()
    val idLength = buffer.getInt()
    val idBuilder = new StringBuilder(idLength)
    for (i <- 1 to idLength) {
      idBuilder += buffer.getChar()
    }
    id = idBuilder.toString()
    
    logDebug("Set from buffer Result: " + typ + " " + id)
    logDebug("Buffer position is " + buffer.position)
    if (typ == BlockMessage.TYPE_PUT_BLOCK) {

      val booleanInt = buffer.getInt()
      val replication = buffer.getInt()
      level = new StorageLevel(booleanInt, replication)
      
      val dataLength = buffer.getInt()
      data = ByteBuffer.allocate(dataLength)
      if (dataLength != buffer.remaining) {
        throw new Exception("Error parsing buffer")
      }
      data.put(buffer)
      data.flip()
      logDebug("Set from buffer Result 2: " + level + " " + data)
    } else if (typ == BlockMessage.TYPE_GOT_BLOCK) {

      val dataLength = buffer.getInt()
      logDebug("Data length is "+ dataLength)
      logDebug("Buffer position is " + buffer.position)
      data = ByteBuffer.allocate(dataLength)
      if (dataLength != buffer.remaining) {
        throw new Exception("Error parsing buffer")
      }
      data.put(buffer)
      data.flip()
      logDebug("Set from buffer Result 3: " + data)
    }

    val finishTime = System.currentTimeMillis
    logDebug("Converted " + id + " from bytebuffer in " + (finishTime - startTime) / 1000.0  + " s")
  }

  def set(bufferMsg: BufferMessage) {
    val buffer = bufferMsg.buffers.apply(0)
    buffer.clear()
    set(buffer)
  }
  
  def getType: Int = {
    return typ
  }
  
  def getId: String = {
    return id
  }
  
  def getData: ByteBuffer = {
    return data
  }
  
  def getLevel: StorageLevel = {
    return level
  }
  
  def toBufferMessage: BufferMessage = {
    val startTime = System.currentTimeMillis
    val buffers = new ArrayBuffer[ByteBuffer]()
    var buffer = ByteBuffer.allocate(4 + 4 + id.length() * 2)
    buffer.putInt(typ).putInt(id.length())
    id.foreach((x: Char) => buffer.putChar(x))
    buffer.flip()
    buffers += buffer

    if (typ == BlockMessage.TYPE_PUT_BLOCK) {
      buffer = ByteBuffer.allocate(8).putInt(level.toInt).putInt(level.replication)
      buffer.flip()
      buffers += buffer
      
      buffer = ByteBuffer.allocate(4).putInt(data.remaining)
      buffer.flip()
      buffers += buffer

      buffers += data
    } else if (typ == BlockMessage.TYPE_GOT_BLOCK) {
      buffer = ByteBuffer.allocate(4).putInt(data.remaining)
      buffer.flip()
      buffers += buffer

      buffers += data
    }
    
    logDebug("Start to log buffers.")
    buffers.foreach((x: ByteBuffer) => logDebug("" + x))
    /*
    println()
    println("BlockMessage: ")
    buffers.foreach(b => {
      while(b.remaining > 0) {
        print(b.get())
      }
      b.rewind()
    })
    println()
    println()
    */
    val finishTime = System.currentTimeMillis
    logDebug("Converted " + id + " to buffer message in " + (finishTime - startTime) / 1000.0  + " s")
    return Message.createBufferMessage(buffers)
  }

  override def toString: String = {
    "BlockMessage [type = " + typ + ", id = " + id + ", level = " + level + 
    ", data = " + (if (data != null) data.remaining.toString  else "null") + "]"
  }
}

object BlockMessage {
  val TYPE_NON_INITIALIZED: Int = 0
  val TYPE_GET_BLOCK: Int = 1
  val TYPE_GOT_BLOCK: Int = 2
  val TYPE_PUT_BLOCK: Int = 3
 
  def fromBufferMessage(bufferMessage: BufferMessage): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(bufferMessage)
    newBlockMessage
  }

  def fromByteBuffer(buffer: ByteBuffer): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(buffer)
    newBlockMessage
  }

  def fromGetBlock(getBlock: GetBlock): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(getBlock)
    newBlockMessage
  }

  def fromGotBlock(gotBlock: GotBlock): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(gotBlock)
    newBlockMessage
  }
  
  def fromPutBlock(putBlock: PutBlock): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(putBlock)
    newBlockMessage
  }

  def main(args: Array[String]) {
    val B = new BlockMessage()
    B.set(new PutBlock("ABC", ByteBuffer.allocate(10), StorageLevel.DISK_AND_MEMORY_2))
    val bMsg = B.toBufferMessage
    val C = new BlockMessage()
    C.set(bMsg)
    
    println(B.getId + " " + B.getLevel)
    println(C.getId + " " + C.getLevel)
  }
}
