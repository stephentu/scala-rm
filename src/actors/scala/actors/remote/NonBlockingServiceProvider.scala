/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

import java.io.{ ByteArrayOutputStream,IOException }
import java.net.{ InetAddress, InetSocketAddress, Socket }
import java.nio.ByteBuffer
import java.nio.channels.{ SelectionKey, Selector, 
                           ServerSocketChannel, SocketChannel }
import java.nio.channels.spi.{ AbstractSelectableChannel, SelectorProvider }

import java.util.{ Comparator, PriorityQueue }
import java.util.concurrent.{ ConcurrentLinkedQueue, Executors }
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ ArrayBuffer, HashMap, ListBuffer, Queue }



object InterestOpUtil {
    // for debug purposes
    sealed trait InterestOp {
      val op: Int
      def isInterested(_op: Int) = { (op & _op) != 0 }
    }

    case object OP_READ    extends InterestOp {
      val op = SelectionKey.OP_READ
    }

    case object OP_WRITE   extends InterestOp {
      val op = SelectionKey.OP_WRITE
    }

    case object OP_CONNECT extends InterestOp {
      val op = SelectionKey.OP_CONNECT
    }

    case object OP_ACCEPT  extends InterestOp {
      val op = SelectionKey.OP_ACCEPT
    }

    private val Ops = List(OP_READ, OP_WRITE, OP_CONNECT, OP_ACCEPT)

    def enumerateSet(ops: Int) = Ops.filter(_.isInterested(ops))
}

object NonBlockingServiceProvider {
  val NumSelectLoops = Runtime.getRuntime.availableProcessors * 4
}

class VaryingSizeByteBufferPool {

  private val comparator = new Comparator[ByteBuffer] {
    override def compare(b1: ByteBuffer, b2: ByteBuffer): Int = b2.capacity - b1.capacity // max heap
    override def equals(obj: Any): Boolean = this == obj
  }

  private val bufferQueue = new PriorityQueue[ByteBuffer](16, comparator)

  def take(minCap: Int): ByteBuffer = {
    val candidate = bufferQueue.peek
    if ((candidate eq null) || candidate.capacity < minCap) {
      ByteBuffer.allocateDirect(align(minCap))
    } else {
      val ret = bufferQueue.poll()
      assert(ret ne null) 
      ret.clear()
      ret
    }
  }

  def release(buf: ByteBuffer) { bufferQueue.offer(buf) }

  // align to power of two
  private def align(cap: Int): Int = {
    assert(cap > 0)
    var i = 1
    while (i < cap) i <<= 1
    i
  }

}

class FixedSizeByteBufferPool(fixedSize: Int) {
  assert(fixedSize > 0)
  private val bufferQueue = new ArrayBuffer[ByteBuffer]

  def take(): ByteBuffer = 
    if (bufferQueue.isEmpty) {
      ByteBuffer.allocateDirect(fixedSize)
    } else {
      val last = bufferQueue(bufferQueue.size - 1)
      bufferQueue.reduceToSize(bufferQueue.size - 1) // remove last
      last
    }

  def release(buf: ByteBuffer) { bufferQueue += buf }

}

class NonBlockingServiceProvider 
  extends ServiceProvider
  with    BytesReceiveCallbackAware
  with    ConnectionCallbackAware {

  import NonBlockingServiceProvider._

  override def mode = ServiceMode.NonBlocking

  private def nextSelectLoop() = {
    val num = selectLoopIdx.getAndIncrement()
    val idx = num % selectLoops.length
    selectLoops(idx)
  }

  private val executor = Executors.newCachedThreadPool()

  private val selectLoops = new Array[SelectorLoop](NumSelectLoops) 
  initializeSelectLoops()

  private def initializeSelectLoops() {
    var idx = 0
    while (idx < selectLoops.length) {
      val selectLoop = new SelectorLoop(idx)
      executor.execute(selectLoop) 
      selectLoops(idx) = selectLoop
      idx += 1
    }
  }

  private val selectLoopIdx = new AtomicInteger

  class SelectorLoop(id: Int) extends Runnable {

    object SendBufPool extends VaryingSizeByteBufferPool

    private def encodeToByteBuffer(unenc: Array[Byte]): ByteBuffer = {
      val len = unenc.length
      val buf = SendBufPool.take(len + 4)
      buf.putInt(len)
      buf.put(unenc)
      buf.flip()
      buf
    }

    private val selector = SelectorProvider.provider.openSelector

    // selector key registration management

    trait Operation {
      def invoke(): Unit
    }

    class ChangeInterestOp(socket: SocketChannel, op: Int) extends Operation {
      override def invoke() {
        import InterestOpUtil._
        //Debug.info("setting channel " + socket + " to be interested in: " + enumerateSet(op))
        socket.keyFor(selector).interestOps(op)
      }
    }

    class RegisterChannel(socket: AbstractSelectableChannel, 
                          op: Int, 
                          attachment: Option[AnyRef]) extends Operation {
      override def invoke() {
        socket.register(selector, op, attachment.getOrElse(null)) 
      }
    }

    private val operationQueue = new ConcurrentLinkedQueue[Operation]

    private def addOperation(op: Operation) {
      if (!operationQueue.offer(op))
        throw new RuntimeException("Could not offer operation to queue")
      selector.wakeup()
    }

    private val readBuffer = ByteBuffer.allocateDirect(8192)

    // receiving message state management 

    private class MessageState {

      var isReadingSize = true
      var messageSize   = 4
      var bytesSoFar    = 0
      var currentArray  = new Array[Byte](4)

      val messageQueue = new Queue[Array[Byte]]

      private def getInt(bytes: Array[Byte]): Int = {
        // big endian
        (bytes(0).toInt & 0xFF) << 24 | 
        (bytes(1).toInt & 0xFF) << 16 | 
        (bytes(2).toInt & 0xFF) << 8  |
        (bytes(3).toInt & 0xFF)
      }

      def doRead(chan: SocketChannel): Boolean = {
        import scala.math.min

        readBuffer.clear()

        var totalBytesRead = 0
        val shouldTerminate = 
          try {
            var continue = true
            var cnt = 0
            while (continue) {
              cnt = chan.read(readBuffer)
              if (cnt <= 0)
                continue = false
              else
                totalBytesRead += cnt
            }
            cnt == -1
          } catch {
            case e: IOException => 
              Debug.error(this + ": Exception " + e.getMessage + " when reading")
              Debug.doError { e.printStackTrace }
              true
          }

        if (totalBytesRead > 0) {
          readBuffer.flip()
          while (readBuffer.remaining > 0) {
            // some bytes are here to process, so process as many as
            // possible
            val toCopy = min(readBuffer.remaining, messageSize - bytesSoFar)
            readBuffer.get(currentArray, bytesSoFar, toCopy)
            bytesSoFar += toCopy

            // this message is done
            if (bytesSoFar == messageSize) 
              if (isReadingSize)
                startReadingMessage(getInt(currentArray))
              else {
                messageQueue += currentArray
                startReadingSize()
              }
          }
        } 

        shouldTerminate
      }

      def startReadingSize() {
        isReadingSize = true
        messageSize   = 4
        bytesSoFar    = 0
        currentArray  = new Array[Byte](4)
      }

      def startReadingMessage(size: Int) {
        isReadingSize = false
        messageSize   = size 
        bytesSoFar    = 0
        currentArray  = new Array[Byte](size)
      }

      def reset() {
        messageQueue.clear
        startReadingSize()
      }

      def hasMessage = !messageQueue.isEmpty
      def nextMessage() = messageQueue.dequeue()

    }

    class NonBlockingServiceConnection(
        socketChannel: SocketChannel,
        override val receiveCallback: BytesReceiveCallback)
      extends Connection {

      private val so = socketChannel.socket

      // TODO: for correctness, will need to wait until the socket channel is
      // connected before allowing the following two values to be computed
      override lazy val remoteNode = Node(so.getInetAddress.getHostName,  so.getPort) 
      override lazy val localNode  = Node(so.getLocalAddress.getHostName, so.getLocalPort)

      private val messageState = new MessageState

      private val writeQueue = new Queue[Array[Byte]]
      private val unfinishedWriteQueue = new Queue[ByteBuffer]

      private var isWriting = false

      override def mode = ServiceMode.NonBlocking

      override def doTerminate() {
        // cancel the key
        socketChannel.keyFor(selector).cancel()

        // close the socket
        socketChannel.close()
      }

      private val WriteChangeOp = new ChangeInterestOp(socketChannel, SelectionKey.OP_WRITE)

      override def send(bytes: Array[Byte]) {
        writeQueue.synchronized {
          writeQueue += bytes
          if (!isWriting && socketChannel.isConnected) {
            isWriting = true
            addOperation(WriteChangeOp) 
          }
        }
      }

      override def send(bytes0: Array[Byte], bytes1: Array[Byte]) {
        writeQueue.synchronized {
          writeQueue += bytes0
          writeQueue += bytes1
          if (!isWriting && socketChannel.isConnected) {
            isWriting = true
            addOperation(WriteChangeOp)
          } 
        }
      }

      def doRead(key: SelectionKey) {
        assert(!isWriting)

        val shouldTerminate = messageState.doRead(socketChannel)

        while (messageState.hasMessage) {
          val nextMsg = messageState.nextMessage
          receiveBytes(nextMsg)
        }

        if (shouldTerminate) 
          terminate()
      }

      def doWrite(key: SelectionKey) {
        assert(isWriting)
        try {
          writeQueue.synchronized {
            var socketFull = false

            // drain writeQueue into unfinishedWriteQueue (and encode to BB)
            while (!writeQueue.isEmpty) {
              val curBytes = writeQueue.head
              val curEntry = encodeToByteBuffer(curBytes)
              writeQueue.dequeue()
              unfinishedWriteQueue += curEntry
            }

            // now try to drain the unfinishedWriteQueue
            while (!unfinishedWriteQueue.isEmpty && !socketFull) {
              val curEntry = unfinishedWriteQueue.head
              val bytesWritten = socketChannel.write(curEntry)
              val finished = curEntry.remaining == 0
              if (finished) { 
                unfinishedWriteQueue.dequeue()
                SendBufPool.release(curEntry)
                //Debug.info(this + ": doWrite() - finished, so dequeuing - queue length is now " + unfinishedWriteQueue.size)
              }
              if (bytesWritten == 0 && !finished) {
                socketFull = true
                //Debug.info(this + ": doWrite() - socket is full")
              }
            }

            if (unfinishedWriteQueue.isEmpty) {
              // back to reading
              //Debug.info(this + ": doWrite() - setting " + key.channel + " back to OP_READ interest")
              isWriting = false
              key.interestOps(SelectionKey.OP_READ)
            }
          }
        } catch {
          case e: IOException => terminate()
        }
      }

      def doFinishConnect(key: SelectionKey) {
        try { 
          socketChannel.finishConnect()
        } catch {
          case e: IOException => terminate()
        }
        
        // check write queue. if it is not empty and we're not in write mode,
        // put us in write mode. otherwise, put us in read mode
        writeQueue.synchronized {
          if (!writeQueue.isEmpty && !isWriting) {
            isWriting = true
            key.interestOps(SelectionKey.OP_WRITE)
          } else key.interestOps(SelectionKey.OP_READ)
        }
      }

      override def toString = "<NonBlockingServiceConnection: " + socketChannel + ">"

    }

    class NonBlockingServiceListener(
        override val port: Int, 
        serverSocketChannel: ServerSocketChannel,
        override val connectionCallback: ConnectionCallback,
        receiveCallback: BytesReceiveCallback)
      extends Listener {

      override def mode = ServiceMode.NonBlocking

      def doAccept(key: SelectionKey) {
        /** For a channel to receive an accept even, it must be a server channel */
        val serverSocketChannel = key.channel.asInstanceOf[ServerSocketChannel]
        val clientSocketChannel = serverSocketChannel.accept
        nextSelectLoop().finishAccept(clientSocketChannel, receiveCallback, this, connectionCallback)
      }

      override def doTerminate() {
        serverSocketChannel.close()
      }

      override def toString = "<NonBlockingServiceListener: " + serverSocketChannel + ">"

    }

    def finishAccept(clientSocketChannel: SocketChannel, 
                     receiveCallback: BytesReceiveCallback,
                     listener: Listener,
                     connectionCallback: ConnectionCallback) {
      //Debug.info(this + ": processAccept from " + clientSocketChannel)
      clientSocketChannel.configureBlocking(false)
      val conn = new NonBlockingServiceConnection(clientSocketChannel, receiveCallback)
      addOperation(new RegisterChannel(clientSocketChannel, SelectionKey.OP_READ, Some(conn)))
      connectionCallback(listener, conn)
    }

    private def processOperationQueue() {
      var continue = true
      while (continue) {
        val op = operationQueue.poll()
        if (op eq null)
          continue = false
        else 
          op.invoke()
      }
    }

    override def run() {
      while (true) {
        try {
          processOperationQueue()
          //Debug.info(this + ": selectLoop calling select()")
          selector.select() /** This is a blocking operation */
          //Debug.info(this + ": selectLoop awaken from select()")
          val selectedKeys = selector.selectedKeys.iterator
          while (selectedKeys.hasNext) {
            val key = selectedKeys.next()
            selectedKeys.remove()
            if (key.isValid)
              if (key.isAcceptable)
                processAccept(key)
              else if (key.isConnectable)
                processConnect(key)
              else if (key.isWritable)
                processWrite(key)
              else if (key.isReadable)
                processRead(key)
            else
              Debug.error(this + ": Invalid key found: " + key)
          }
        } catch {
          case e: Exception =>
            Debug.error(this + " caught exception in select loop: " + e.getMessage)
            Debug.doError { e.printStackTrace }
        }
      }
    }

    // handle accepts

    private def processAccept(key: SelectionKey) {
      key.attachment.asInstanceOf[NonBlockingServiceListener].doAccept(key)
    }

    // handle reads

    private def processRead(key: SelectionKey) {
      key.attachment.asInstanceOf[NonBlockingServiceConnection].doRead(key)
    }

    // handle writes

    private def processWrite(key: SelectionKey) {
      key.attachment.asInstanceOf[NonBlockingServiceConnection].doWrite(key)
    }

    // handle connects

    private def processConnect(key: SelectionKey) {
      key.attachment.asInstanceOf[NonBlockingServiceConnection].doFinishConnect(key)
    }

    def connect(node: Node, receiveCallback: BytesReceiveCallback) = {
      val clientSocket = SocketChannel.open
      clientSocket.configureBlocking(false)
      val connected = clientSocket.connect(new InetSocketAddress(node.address, node.port))
      val interestOp = if (connected) SelectionKey.OP_READ else SelectionKey.OP_CONNECT
      val conn = new NonBlockingServiceConnection(clientSocket, receiveCallback)
      addOperation(new RegisterChannel(clientSocket, interestOp, Some(conn))) 
      conn
    }

    def listen(port: Int, connectionCallback: ConnectionCallback, receiveCallback: BytesReceiveCallback) = {
      val serverSocketChannel = ServerSocketChannel.open
      serverSocketChannel.configureBlocking(false)
      serverSocketChannel.socket.bind(new InetSocketAddress(port))
      val listener = new NonBlockingServiceListener(port, serverSocketChannel, connectionCallback, receiveCallback)
      addOperation(new RegisterChannel(serverSocketChannel, SelectionKey.OP_ACCEPT, Some(listener)))    
      listener
    }

    override def toString = "<SelectorLoop " + id + ">"

    def terminate() {

    }

  }

  override def connect(node: Node, receiveCallback: BytesReceiveCallback) = {
    nextSelectLoop().connect(node, receiveCallback)
  }

  override def listen(port: Int, connectionCallback: ConnectionCallback, receiveCallback: BytesReceiveCallback) = {
    nextSelectLoop().listen(port, connectionCallback, receiveCallback)
  }

  override def terminate() {
    selectLoops foreach { _.terminate() }
  }

}
