package spark.storage

import java.nio.ByteBuffer

import scala.collection.mutable.StringBuilder
import scala.collection.mutable.ArrayBuffer

import spark._
import spark.network._

class BlockMessageArray(var blockMessages: Seq[BlockMessage]) extends Seq[BlockMessage] with Logging {
  
  def this(bm: BlockMessage) = this(Array(bm))

  def this() = this(null.asInstanceOf[Seq[BlockMessage]])

  def apply(i: Int) = blockMessages(i) 

  def iterator = blockMessages.iterator

  def length = blockMessages.length 

  initLogging()
  
  def set(bufferMessage: BufferMessage) {
    val startTime = System.currentTimeMillis
    val newBlockMessages = new ArrayBuffer[BlockMessage]()
    val buffer = bufferMessage.buffers(0)
    buffer.clear()
    /*
    println()
    println("BlockMessageArray: ")
    while(buffer.remaining > 0) {
      print(buffer.get())
    }
    buffer.rewind()
    println()
    println()
    */
    while (buffer.remaining() > 0) {
      val size = buffer.getInt()
      logDebug("Creating block message of size " + size + " bytes")
      val newBuffer = buffer.slice()
      newBuffer.clear()
      newBuffer.limit(size)
      logDebug("Trying to convert buffer " + newBuffer + " to block message")
      val newBlockMessage = BlockMessage.fromByteBuffer(newBuffer)
      logDebug("Created " + newBlockMessage)
      newBlockMessages += newBlockMessage 
      buffer.position(buffer.position() + size)
    }
    val finishTime = System.currentTimeMillis
    logDebug("Converted block message array from buffer message in " + (finishTime - startTime) / 1000.0  + " s")
    this.blockMessages = newBlockMessages 
  }
  
  def toBufferMessage: BufferMessage = {
    val buffers = new ArrayBuffer[ByteBuffer]()

    blockMessages.foreach(blockMessage => {
      val bufferMessage = blockMessage.toBufferMessage
      logDebug("Adding " + blockMessage)
      val sizeBuffer = ByteBuffer.allocate(4).putInt(bufferMessage.size)
      sizeBuffer.flip
      buffers += sizeBuffer
      buffers ++= bufferMessage.buffers
      logDebug("Added " + bufferMessage)
    })
   
    logDebug("Buffer list:")
    buffers.foreach((x: ByteBuffer) => logDebug("" + x))
    /*
    println()
    println("BlockMessageArray: ")
    buffers.foreach(b => {
      while(b.remaining > 0) {
        print(b.get())
      }
      b.rewind()
    })
    println()
    println()
    */
    return Message.createBufferMessage(buffers)
  }
}

object BlockMessageArray {
 
  def fromBufferMessage(bufferMessage: BufferMessage): BlockMessageArray = {
    val newBlockMessageArray = new BlockMessageArray()
    newBlockMessageArray.set(bufferMessage)
    newBlockMessageArray
  }
  
  def main(args: Array[String]) {
    val blockMessages = 
      (0 until 10).map(i => {
        if (i % 2 == 0) {
          val buffer =  ByteBuffer.allocate(100)
          buffer.clear
          BlockMessage.fromPutBlock(PutBlock(i.toString, buffer, StorageLevel.MEMORY_ONLY))
        } else {
          BlockMessage.fromGetBlock(GetBlock(i.toString))
        }
      })
    val blockMessageArray = new BlockMessageArray(blockMessages)
    println("Block message array created")
    
    val bufferMessage = blockMessageArray.toBufferMessage
    println("Converted to buffer message")
    
    val totalSize = bufferMessage.size
    val newBuffer = ByteBuffer.allocate(totalSize)
    newBuffer.clear()
    bufferMessage.buffers.foreach(buffer => {
      newBuffer.put(buffer)
      buffer.rewind()
    })
    newBuffer.flip
    val newBufferMessage = Message.createBufferMessage(newBuffer) 
    println("Copied to new buffer message, size = " + newBufferMessage.size)

    val newBlockMessageArray = BlockMessageArray.fromBufferMessage(newBufferMessage)
    println("Converted back to block message array")
    newBlockMessageArray.foreach(blockMessage => {
      blockMessage.getType match {
        case BlockMessage.TYPE_PUT_BLOCK => {
          val pB = PutBlock(blockMessage.getId, blockMessage.getData, blockMessage.getLevel)
          println(pB)
        } 
        case BlockMessage.TYPE_GET_BLOCK => {
          val gB = new GetBlock(blockMessage.getId)
          println(gB)
        }
      }
    })
  }
}


