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
import java.nio.channels.{ ClosedSelectorException, SelectionKey, Selector, 
                           ServerSocketChannel, SocketChannel }
import java.nio.channels.spi.{ AbstractSelectableChannel, SelectorProvider }

import java.util.{ Comparator, PriorityQueue }
import java.util.concurrent.{ ConcurrentLinkedQueue, ConcurrentHashMap, Executors }
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ ArrayBuffer, HashMap, ListBuffer, Queue }
import scala.concurrent.SyncVar


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
  val NumConnectionLoops = Runtime.getRuntime.availableProcessors * 4
  val NumListenerLoops = Runtime.getRuntime.availableProcessors
  val ReadBufSize = 8192
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

  // next multiple of 8. assumes i > 0 and that the next multiple wont
  // overflow
  def align(i: Int): Int = {
    if ((i & 0x7) != 0x0) 
      (((i >>> 0x3) + 0x1) << 0x3)
    else
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

class NonBlockingServiceProvider extends ServiceProvider {

  import NonBlockingServiceProvider._

  override def mode = ServiceMode.NonBlocking

  private def nextConnectionLoop() = {
    val num = connLoopIdx.getAndIncrement()
    val idx = num % connLoops.length
    connLoops(idx)
  }

  private def nextListenerLoop() = {
    val num = listenerLoopIdx.getAndIncrement()
    val idx = num % listenerLoops.length
    listenerLoops(idx)
  }

  private val connExecutor = Executors.newCachedThreadPool()
  private val listenerExecutor = Executors.newCachedThreadPool()

  private val connLoops = new Array[SelectorLoop](NumConnectionLoops) 
  private val listenerLoops = new Array[SelectorLoop](NumListenerLoops) 

  initializeSelectLoops()

  private def initializeSelectLoops() {
    var idx = 0
    while (idx < listenerLoops.length) {
      val listenerLoop = new SelectorLoop(idx, false)
      listenerExecutor.execute(listenerLoop) 
      listenerLoops(idx) = listenerLoop
      idx += 1
    }
    idx = 0
    while (idx < connLoops.length) {
      val connLoop = new SelectorLoop(idx, true)
      connExecutor.execute(connLoop) 
      connLoops(idx) = connLoop
      idx += 1
    }
  }

  private val connLoopIdx = new AtomicInteger
  private val listenerLoopIdx = new AtomicInteger

  class SelectorLoop(id: Int, isConn: Boolean) 
    extends Runnable
    with    CanTerminate {

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

    import InterestOpUtil._

    class ChangeInterestOp(socket: SocketChannel, op: Int) extends Operation {
      override def invoke() {

        import java.nio.channels.CancelledKeyException

        try {
          socket.keyFor(selector).interestOps(op)
          Debug.info(this + ": done!")
        } catch {
          case e: CancelledKeyException =>
            Debug.error(this + ": tried to set interestOps for " + socket + " to " + enumerateSet(op) + ", but key was cancelled already")
        }
      }
      override def toString = "<ChangeInterestOp: " + socket + " to " + enumerateSet(op) + ">"
    }

    class RegisterChannel(socket: AbstractSelectableChannel, 
                          op: Int, 
                          attachment: Option[AnyRef]) extends Operation {
      override def invoke() {
        // TODO: catch exceptions
        socket.register(selector, op, attachment.getOrElse(null)) 
        Debug.info(this + ": done!")
      }
      override def toString = "<RegisterChannel: " + socket + " with " + enumerateSet(op) + ">"
    }

    abstract class QuerySelectorKeyAttachments extends Operation {
      private val future = new SyncVar[List[CanTerminate]]
      def attachments = future.get
      override def invoke() {
        val l = new ListBuffer[CanTerminate]
        val iter = selector.keys.iterator
        while (iter.hasNext) {
          val key = iter.next()
          val attachment = key.attachment.asInstanceOf[CanTerminate]
          if (include_?(attachment)) l += attachment
        }
        future.set(l.toList)
      }
      protected def include_?(attachment: CanTerminate): Boolean
    }

    class QueryAllKeys extends QuerySelectorKeyAttachments {
      override def include_?(attachment: CanTerminate) = true 
    }

    class QueryConnections extends QuerySelectorKeyAttachments {
      override def include_?(attachment: CanTerminate) = attachment.isInstanceOf[Connection]
    }

    class QueryListeners extends QuerySelectorKeyAttachments {
      override def include_?(attachment: CanTerminate) = attachment.isInstanceOf[Listener]
    }


    private val operationQueue = new ConcurrentLinkedQueue[Operation]

    private def addOperation(op: Operation) {
      if (!operationQueue.offer(op))
        throw new RuntimeException("Could not offer operation to queue")
      Debug.info(this + ": " + op + " added and calling wakeup()")
      selector.wakeup()
    }

    private val readBuffer = ByteBuffer.allocateDirect(ReadBufSize)

    // receiving message state management 

    private class MessageState {

      var isReadingSize = true
      var messageSize   = 4
      var bytesSoFar    = 0
      var currentArray  = new Array[Byte](4)

      val messageQueue = new Queue[Array[Byte]]


      // returns true iff either EOF was reached, or an exception
      // occured during read
      def doRead(chan: SocketChannel): Boolean = {
        import scala.math.min
        import BitUtils._

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
                startReadingMessage(bytesToInt(currentArray))
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
      extends ByteConnection {

      private val so = socketChannel.socket

      // for correctness, will need to wait until the socket channel is
      // connected before allowing the following two values to be computed.
      // that is why we wait here
      override lazy val remoteNode = {
        socketChannel.synchronized {
          while (!socketChannel.isConnected) socketChannel.wait()
          Node(so.getInetAddress.getHostName,so.getPort) 
        }
      }

      override lazy val localNode  = Node(so.getLocalAddress.getHostName, so.getLocalPort)

      private val messageState = new MessageState

      // writeQueue guarded by terminateLock
      private val writeQueue = new Queue[Array[Byte]]
      private val unfinishedWriteQueue = new Queue[ByteBuffer]

      // guarded by terminateLock
      private var isWriting = false
      private var shouldNotify = false

      private def ensureAlive() {
        if (terminateInitiated) throw new ConnectionAlreadyClosedException
      }

      override def mode = ServiceMode.NonBlocking

      override def doTerminateImpl(isBottom: Boolean) {
        if (!isBottom) { // if the terminate is from bottom, dont try to drain writeQueue b/c connection is already bad
          // wait 5 seconds for writeQueue to drain existing packets which
          // have not gone out on the wire, if the socket is still connected
          if (isWriting && socketChannel.isConnected) {
            shouldNotify = true
            Debug.info(this + ": doTerminateImpl(): waiting for write queue to drain for 5 seconds")
            terminateLock.wait(5000)
          }
          shouldNotify = false
          if (isWriting) 
            Debug.error(this + ": write queue did not successfully drain")
        }

        // cancel the key
        socketChannel.keyFor(selector).cancel()

        // close the socket
        socketChannel.close()
      }

      private val WriteChangeOp = new ChangeInterestOp(socketChannel, SelectionKey.OP_WRITE)

      override def send(bytes: Array[Byte]) {
        terminateLock.synchronized {
          ensureAlive()
          writeQueue += bytes
          if (!isWriting && socketChannel.isConnected) {
            isWriting = true
            addOperation(WriteChangeOp) 
          }
        }
      }

      override def send(bytes0: Array[Byte], bytes1: Array[Byte]) {
        terminateLock.synchronized {
          ensureAlive()
          writeQueue += bytes0
          writeQueue += bytes1
          if (!isWriting && socketChannel.isConnected) {
            isWriting = true
            addOperation(WriteChangeOp)
          } 
        }
      }

      def doRead(key: SelectionKey) {
        val shouldTerminate = messageState.doRead(socketChannel)

        while (messageState.hasMessage) {
          val nextMsg = messageState.nextMessage
          receiveBytes(nextMsg)
        }

        if (shouldTerminate) 
          terminateBottom()
      }

      def doWrite(key: SelectionKey) {
        assert(isWriting)
        try {
          terminateLock.synchronized {
            // drain writeQueue into unfinishedWriteQueue (and encode to BB)
            while (!writeQueue.isEmpty) {
              val curBytes = writeQueue.head
              val curEntry = encodeToByteBuffer(curBytes)
              writeQueue.dequeue()
              unfinishedWriteQueue += curEntry
            }
          }

          var socketFull = false
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

          terminateLock.synchronized {
            if (unfinishedWriteQueue.isEmpty && writeQueue.isEmpty) {
              // back to reading
              Debug.info(this + ": doWrite() - setting " + key.channel + " back to OP_READ interest")
              isWriting = false
              key.interestOps(SelectionKey.OP_READ)
              if (shouldNotify) 
                terminateLock.notifyAll()
            }
          }
        } catch {
          case e: IOException => 
            Debug.error(this + ": caught IOException in doWrite: " + e.getMessage)       
            Debug.doError { e.printStackTrace }
            terminateLock.synchronized {
              // since we had an error in writing, we don't want to have to
              // wait to drain the write queue on termination (since writes
              // probably won't succeed anyways)
              isWriting = false
            }
            terminateBottom()
        }
      }

      def doFinishConnect(key: SelectionKey) {
        try { 

          socketChannel.finishConnect()

          socketChannel.synchronized { socketChannel.notifyAll() }

          // check write queue. if it is not empty and we're not in write mode,
          // put us in write mode. otherwise, put us in read mode
          terminateLock.synchronized {
            if (!writeQueue.isEmpty && !isWriting) {
              isWriting = true
              key.interestOps(SelectionKey.OP_WRITE)
            } else 
              key.interestOps(SelectionKey.OP_READ)
          }

        } catch {
          case e: IOException => 
            Debug.error(this + ": caught IOException in doFinishConnect: " + e.getMessage)       
            Debug.doError { e.printStackTrace }
            terminateBottom()
        }
      }

      override def toString = "<NonBlockingServiceConnection: " + socketChannel + ">"

    }

    // used to put as value in concurrent hash map (since we dont care about
    // the value, and there is no concurrent hash set)
    private val DUMMY_VALUE = new Object

    class NonBlockingServiceListener(
        override val port: Int, 
        serverSocketChannel: ServerSocketChannel,
        override val connectionCallback: ConnectionCallback,
        receiveCallback: BytesReceiveCallback)
      extends Listener {

      private val childConnections = new ConcurrentHashMap[ByteConnection, Object]

      override def mode = ServiceMode.NonBlocking

      def doAccept(key: SelectionKey) {
        /** For a channel to receive an accept even, it must be a server channel */
        val serverSocketChannel = key.channel.asInstanceOf[ServerSocketChannel]
        val clientSocketChannel = serverSocketChannel.accept
        val conn = nextConnectionLoop().finishAccept(clientSocketChannel, receiveCallback)
        conn.afterTerminate {
          childConnections.remove(conn)
        }
        childConnections.put(conn, DUMMY_VALUE)
        receiveConnection(conn)
      }

      override def doTerminateImpl(isBottom: Boolean) {
        Debug.info(this + ": doTerminateImpl() called")
        val enum = childConnections.keys
        while (enum.hasMoreElements) {
          val conn = enum.nextElement()
          if (isBottom)
            conn.terminateBottom()
          else
            conn.terminateTop()
        }
        serverSocketChannel.close()
      }

      override def toString = "<NonBlockingServiceListener: " + serverSocketChannel + ">"

    }

    def finishAccept(clientSocketChannel: SocketChannel, receiveCallback: BytesReceiveCallback) = {
      clientSocketChannel.configureBlocking(false)
      val conn = new NonBlockingServiceConnection(clientSocketChannel, receiveCallback)
      addOperation(new RegisterChannel(clientSocketChannel, SelectionKey.OP_READ, Some(conn)))
      conn
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

    @volatile
    private var terminated = false

    override def run() {
      while (!terminated) {
        try {
          processOperationQueue()
          //Debug.info(this + ": calling select()")
          val selected = selector.select() /** TODO: consider using select(long) alternative */
          //Debug.info(this + ": woke up from select() with " + selected + " keys selected")
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
          case e: ClosedSelectorException =>
            Debug.info(this + ": selector closed")
            terminateBottom()
          case e: Exception =>
            Debug.error(this + " caught exception in select loop: " + e.getMessage)
            Debug.doError { e.printStackTrace }
            terminateBottom()
        }
      }
      Debug.info(this + ": run() is finished")
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

    private def ensureAlive() {
      if (terminateInitiated) throw new ProviderAlreadyClosedException 
    }

    def connect(node: Node, receiveCallback: BytesReceiveCallback) = terminateLock.synchronized {
      ensureAlive()
      Debug.info(this + ": connect called to: " + node)
      val clientSocket = SocketChannel.open
      clientSocket.configureBlocking(false)
      val connected = clientSocket.connect(new InetSocketAddress(node.address, node.port))
      val interestOp = if (connected) SelectionKey.OP_READ else SelectionKey.OP_CONNECT
      val conn = new NonBlockingServiceConnection(clientSocket, receiveCallback)
      addOperation(new RegisterChannel(clientSocket, interestOp, Some(conn)))
      conn
    }

    def listen(port: Int, connectionCallback: ConnectionCallback, receiveCallback: BytesReceiveCallback) = terminateLock.synchronized {
      ensureAlive()
      Debug.info(this + ": listening on port: " + port)
      val serverSocketChannel = ServerSocketChannel.open
      serverSocketChannel.configureBlocking(false)
      serverSocketChannel.socket.bind(new InetSocketAddress(port))
      val listener = new NonBlockingServiceListener(port, serverSocketChannel, connectionCallback, receiveCallback)
      addOperation(new RegisterChannel(serverSocketChannel, SelectionKey.OP_ACCEPT, Some(listener)))    
      listener
    }

    override def toString = 
      if (isConn)
        "<SelectorLoop (Connections) " + id + ">"
      else
        "<SelectorLoop (Listeners) " + id + ">"


    override def doTerminateImpl(isBottom: Boolean) {
      // try to terminate all the keys gracefully first
      Debug.info(this + ":terminate() - shutting down keys")
      val query = new QueryAllKeys
      addOperation(query) 
      query.attachments.foreach { q =>
        if (isBottom) q.terminateBottom() else q.terminateTop()
      }

      // now shut the loop down
      terminated = true
      Debug.info(this + ": closing selector")
      selector.close()
    }

  }

  override def connect(node: Node, receiveCallback: BytesReceiveCallback) = {
    nextConnectionLoop().connect(node, receiveCallback)
  }

  override def listen(port: Int, connectionCallback: ConnectionCallback, receiveCallback: BytesReceiveCallback) = {
    nextListenerLoop().listen(port, connectionCallback, receiveCallback)
  }

  override def doTerminateImpl(isBottom: Boolean) {
    Debug.info(this + ": doTerminateImpl() - closing connLoops")
    connLoops foreach { s =>
      if (isBottom) s.terminateBottom() else s.terminateTop()
    }
    Debug.info(this + ": doTerminateImpl() - closing listenerLoops")
    listenerLoops foreach { s =>
      if (isBottom) s.terminateBottom() else s.terminateTop()
    }

    Debug.info(this + ": doTerminateImpl() - closing executors")
    listenerExecutor.shutdownNow()
    connExecutor.shutdownNow()
  }

}
