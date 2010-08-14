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
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger }

import scala.collection.mutable.{ ArrayBuffer, HashMap, ListBuffer, Queue }
import scala.concurrent.SyncVar


private[remote] object InterestOpUtil {
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

private[remote] object NonBlockingServiceProvider {
  val NumConnectionLoops = Runtime.getRuntime.availableProcessors * 2
  val NumListenerLoops   = 1 /* Runtime.getRuntime.availableProcessors */

  val ReadBufSize        = 8192
}

private[remote] class VaryingSizeByteBufferPool {

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
      //assert(ret ne null) 
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

private[remote] class FixedSizeByteBufferPool(fixedSize: Int) {
  //assert(fixedSize > 0)
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

private[remote] class NonBlockingServiceProvider extends ServiceProvider {

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

  // called by ctor
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

    private def encodeToByteBuffer(seq: ByteSequence): ByteBuffer = {
      val len = seq.length
      val buf = SendBufPool.take(len + 4)

      if (seq.isDiscardable && seq.offset >= 4) {
        val b = seq.bytes
        val o = seq.offset
        val l = seq.length

        b(o - 4) = ((l >>> 24) & 0xff).toByte
        b(o - 3) = ((l >>> 16) & 0xff).toByte
        b(o - 2) = ((l >>> 8) & 0xff).toByte
        b(o - 1) = ((l & 0xff)).toByte

        buf.put(seq.bytes, seq.offset - 4, seq.length + 4)
      } else {
        buf.putInt(len)
        buf.put(seq.bytes, seq.offset, seq.length)
      }

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
          //Debug.error(this + ": done!")
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
        //Debug.error(this + ": done!")
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
      //Debug.info(this + ": " + op + " added and calling wakeup()")
      selector.wakeup()
    }

    private val readBuffer = ByteBuffer.allocateDirect(ReadBufSize)


    class InitiatedNonBlockingServiceConnection(
        override val remoteNode: Node,
        socketChannel: SocketChannel,
        receiveCallback: BytesReceiveCallback)
      extends NonBlockingServiceConnection(socketChannel, receiveCallback) {
        override val isEphemeral   = false
        override val connectFuture = new BlockingFuture
    }


    class ReceivedNonBlockingServiceConnection(
        socketChannel: SocketChannel,
        receiveCallback: BytesReceiveCallback)
      extends NonBlockingServiceConnection(socketChannel, receiveCallback) {

      override lazy val remoteNode = Node(so.getInetAddress.getHostName, so.getPort) 

      override val isEphemeral     = true
      override val connectFuture   = NoOpFuture
    }

    abstract class NonBlockingServiceConnection(
        socketChannel: SocketChannel,
        override val receiveCallback: BytesReceiveCallback)
      extends ByteConnection {

      // receiving message state management 
      private class MessageState {

        val intArray      = new Array[Byte](4)

        var isReadingSize = true
        var messageSize   = 4
        var bytesSoFar    = 0
        var currentArray  = intArray

        // returns true iff either EOF was reached, or an exception
        // occured during read
        def doRead(chan: SocketChannel): Boolean = {
          import BitUtils._

          var shouldTerminate = false /* Did we encounter an error within a readBuffer? */

          try {
            var outerContinue = true  /* Should we keep trying new readBuffers? */
            while (outerContinue) {
              readBuffer.clear()

              var totalBytesRead = 0
              var continue       = true /* Should we keep trying THIS readBuffer? */
              while (continue) {
                val cnt = chan.read(readBuffer)
                if (cnt == -1) {
                  // don't continue anymore, EOF
                  shouldTerminate = true
                  outerContinue   = false
                  continue        = false
                } else if (readBuffer.remaining == 0) {
                  // readBuffer is full, so don't continue anymore on this
                  // readBuffer, but use another one
                  totalBytesRead += cnt
                  continue = false
                } else if (cnt == 0) {
                  // socket has no more to offer to read, so don't continue
                  // anymore
                  outerContinue = false
                  continue      = false
                } else {
                  //assert(cnt > 0)
                  // socket offered some to read, so let's keep reading
                  totalBytesRead += cnt
                }
              }

              if (totalBytesRead > 0) {
                readBuffer.flip()
                while (readBuffer.remaining > 0) {
                  // some bytes are here to process, so process as many as
                  // possible
                  val toCopy = java.lang.Math.min(readBuffer.remaining, messageSize - bytesSoFar)
                  readBuffer.get(currentArray, bytesSoFar, toCopy)
                  bytesSoFar += toCopy

                  // this message is done
                  if (bytesSoFar == messageSize) 
                    if (isReadingSize)
                      startReadingMessage(bytesToInt(currentArray))
                    else {
                      receiveBytes(currentArray)
                      startReadingSize()
                    }
                }
              } 
            }
          } catch {
            case e: IOException => 
              Debug.error(this + ": Exception " + e.getMessage + " when reading")
              Debug.doError { e.printStackTrace }
              shouldTerminate = true
          }

          shouldTerminate
        }

        def startReadingSize() {
          isReadingSize = true
          messageSize   = 4
          bytesSoFar    = 0
          currentArray  = intArray
        }

        def startReadingMessage(size: Int) {
          isReadingSize = false
          messageSize   = size 
          bytesSoFar    = 0
          currentArray  = new Array[Byte](size)
        }

        def reset() {
          startReadingSize()
        }

      }

      /**
       * Not thread safe tasks
       */
      abstract class WriteLoopTask {
        def doWrite(socketChannel: SocketChannel): Long 
        def isFinished: Boolean
      }

      class WriteLoopTask1(b0: ByteSequence, ftch: Option[RFuture]) extends WriteLoopTask {

        private var b0_bb: ByteBuffer = _
        private var _isFinished       = false

        override def doWrite(socketChannel: SocketChannel) = {
          if (b0_bb eq null) 
            b0_bb = encodeToByteBuffer(b0)
          val bytesWritten =
            try {
              socketChannel.write(b0_bb)
            } catch {
              case e: IOException =>
                // cleanup
                releaseResources() // don't leak a byte buffer on error
                ftch.foreach(_.finishWithError(e))
                throw e
            }
          if (b0_bb.remaining == 0) { // done
            releaseResources()
            ftch.foreach(_.finishSuccessfully())
            _isFinished = true
          }
          bytesWritten
        }
        override def isFinished = _isFinished 
        @inline private def releaseResources() {
          if (b0_bb ne null) {
            SendBufPool.release(b0_bb)
            b0_bb = null
          }
        }
      }

      protected val so = socketChannel.socket

      override lazy val localNode = Node(so.getLocalAddress.getHostName, so.getLocalPort)

      private val messageState = new MessageState

      private val isWriting = new AtomicBoolean(false)

      private val writeQueue = new ConcurrentLinkedQueue[WriteLoopTask]

      override def newAlreadyTerminatedException() = new ConnectionAlreadyClosedException

      override def mode = ServiceMode.NonBlocking

      override def doTerminateImpl(isBottom: Boolean) {
        Debug.info(this + ": doTerminateImpl(" + isBottom + ")")
        if (!isBottom) { // if the terminate is from bottom, dont try to drain writeQueue b/c connection is already bad
          // poll the writeQueue until it is empty, only trying up to 1000 times
          Debug.info(this + ": attempting to drain writeQueue")
          var continue = true
          var numTries = 0
          while (continue) {
            val nextWrite = writeQueue.peek
            numTries += 1
            continue = (nextWrite ne null) && numTries < 1000
            if (continue) Thread.sleep(500)
          }
          if (writeQueue.peek ne null)
            Debug.info(this + ": writeQueue is not empty")
          else 
            Debug.info(this + ": successfully drained writeQueue")
        }

        // cancel the key, if it has not already been cancelled
        val key = socketChannel.keyFor(selector)
        if (key ne null) key.cancel()

        // close the socket
        socketChannel.close()
      }

      private val WriteChangeOp = new ChangeInterestOp(socketChannel, SelectionKey.OP_WRITE | SelectionKey.OP_READ) /** Accept both WRITEs and READs */

      private def setWriteMode() {
        val success = isWriting.compareAndSet(false, true)
        if (success) {
          //Debug.error(this + "setWriteMode(): adding WriteChangeOp to queue")
          addOperation(WriteChangeOp) 
        } 
        //else {
        //  Debug.error(this + "setWriteMode(): CAS failed - doing nothing")
        //}
      }

      override def send(seq: ByteSequence, ftch: Option[RFuture]) {
        //Debug.error(this + ": send(a0)")
        ensureAlive()
        writeQueue.offer(new WriteLoopTask1(seq, ftch))
        //Debug.error(this + ": send(a0) - offered")
        if (socketChannel.isConnected) setWriteMode()
      }

      def doRead(key: SelectionKey) {
        val shouldTerminate = messageState.doRead(socketChannel)
        if (shouldTerminate) 
          terminateBottom()
      }

      def doWrite(key: SelectionKey) {
        //Debug.error(this + ": doWrite on channel: " + socketChannel)
        //Debug.error(this + ": this channel is interested in: " + InterestOpUtil.enumerateSet(key.interestOps))
        //Debug.error(this + ": isWriting: " + isWriting.get)

        //assert(isWriting.get) /* Incorrect assumption */
        /**
         * Note: it is very possible that doWrite() is executed when isWriting
         * is false (in other words, the commented out assertion above is
         * wrong!). This is because setWriteMode() is not atomic. In the time
         * after the CAS and before offering to the op queue, isWriting can be
         * set to false again by the code below. In other words, the op queue
         * is always lagging behind the isWriting state, and since this loop
         * below greedily consumes writes, it is possible that it consumes
         * more writes in one cycle than it is supposed to. This is a race we will simply
         * ignore; the penalty is that we spin an extra few cycles in
         * doWrite(). This is (probably) more preferable than having to grab a
         * lock every time we want to do a send
         */

        try {
          var continue = true
          var numEmptyWrites = 0
          var hasWrittenBytes = false
          val spinsAllowed = 16
          while (continue) {
            val nextWrite = writeQueue.peek
            if (nextWrite eq null) {
              // queue is empty
              continue = false
              isWriting.set(false)
              //Debug.error(this + ": doWrite - isWriting set to false")
              /**
               * There is a race condition here!
               * When we enter the nextWrite eq null branch, isWriting still
               * is true, so any subsequent calls to send() won't enqueue a
               * ChangeOp in the op queue (since it will assume we are already
               * in writing mode). This could cause us to miss writes.
               * Therefore, after we set isWriting to false, we have to check
               * the writeQueue again to see if it's actually empty (meaning
               * we can actually turn writes off, because any subsequent calls
               * to send() will add an ChangeOp to the op queue.
               *
               * NOTE: The correctness of this depends on peek() being able to
               * read any entries given by offer() immediately after offer()
               * returns
               */
              if (writeQueue.peek eq null) {
                //Debug.error(this + ": doWrite - OP_READ interest set - FLAG")
                key.interestOps(SelectionKey.OP_READ) /** Go back to only handling READs */
                //assert(key.interestOps == SelectionKey.OP_READ)
              } else {
                // A thread snuck in sometime between peek() and set(), so
                // this means we need to handle writes again. So we set
                // isWriting back to true
                isWriting.set(true)
                //Debug.error(this + ": doWrite - isWriting set back to true, because writeQueue.peek ne null")
              }
            } else {
              val bytesWritten = nextWrite.doWrite(socketChannel)
              if (bytesWritten > 0)
                hasWrittenBytes = true

              if (nextWrite.isFinished) {
                writeQueue.poll() // remove the write from the queue
                //assert(bytesWritten > 0 && hasWrittenBytes)
                //assert((head ne null) && (head == nextWrite))
                //head.releaseResources()
              } else if (bytesWritten == 0) {
                numEmptyWrites += 1
                if (hasWrittenBytes)
                  // socket is full, and since we've already done a write, go
                  // ahead and stop now
                  continue = false
                else if (numEmptyWrites > spinsAllowed) 
                  // we've spun enough times w/o a write, so go ahead and stop
                  // now
                  continue = false
              }
            }
          }
        } catch {
          case e: IOException => 
            Debug.error(this + ": caught IOException in doWrite: " + e.getMessage)       
            Debug.doError { e.printStackTrace }
            // since we had an error in writing, we don't want to have to
            // wait to drain the write queue on termination (since writes
            // probably won't succeed anyways)
            isWriting.set(false)
            terminateBottom()
        }
      }

      def doFinishConnect(key: SelectionKey) {
        val ftch = key.attachment.asInstanceOf[NonBlockingServiceConnection].connectFuture
        try { 

          socketChannel.finishConnect()
          ftch.finishSuccessfully()

          // check write queue. if it is not empty try to put in write mode
          // otherwise, put us in read mode
          if (writeQueue.peek ne null) {
            val success = isWriting.compareAndSet(false, true)
            if (success) {
              //Debug.error(this + ": doConnectFinish(): setting to RW")
              key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ) /** Handle both WRITEs and READs */
            } 
            //else {
            //  Debug.error(this + ":on doConnectFinish(): not doing anything. interest set is: ")
            //  Debug.error("set: " + InterestOpUtil.enumerateSet(key.interestOps))
            //}
          } else {
            //Debug.error(this + ": doConnectFinish(): setting to R")
            key.interestOps(SelectionKey.OP_READ) /** Just READ for now */
          }

        } catch {
          case e: IOException => 
            Debug.error(this + ": caught IOException in doFinishConnect: " + e.getMessage)       
            Debug.doError { e.printStackTrace }
            ftch.finishWithError(e)
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
        override val connectionCallback: ConnectionCallback[ByteConnection],
        receiveCallback: BytesReceiveCallback)
      extends Listener {

      private val childConnections = new ConcurrentHashMap[ByteConnection, Object]

      override def mode = ServiceMode.NonBlocking

      def doAccept(key: SelectionKey) {
        /** For a channel to receive an accept even, it must be a server channel */
        val serverSocketChannel = key.channel.asInstanceOf[ServerSocketChannel]
        val clientSocketChannel = serverSocketChannel.accept()
        val conn = nextConnectionLoop().finishAccept(clientSocketChannel, receiveCallback)
        conn.afterTerminate { isBottom =>
          childConnections.remove(conn)
        }
        childConnections.put(conn, DUMMY_VALUE)
        receiveConnection(conn)
      }

      override def doTerminateImpl(isBottom: Boolean) {
        Debug.info(this + ": doTerminateImpl() called")
        import scala.collection.JavaConversions._
        childConnections.keys.foreach(_.doTerminate(isBottom))
        childConnections.clear()
        serverSocketChannel.close()
      }

      override def toString = "<NonBlockingServiceListener: " + serverSocketChannel + ">"

    }

    def finishAccept(clientSocketChannel: SocketChannel, receiveCallback: BytesReceiveCallback) = {
      clientSocketChannel.socket.setTcpNoDelay(true) // disable nagle's algorithm
      clientSocketChannel.configureBlocking(false)
      val conn = new ReceivedNonBlockingServiceConnection(clientSocketChannel, receiveCallback)
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

    @volatile private var terminated = false

    override def run() {
      while (!terminated) {
        try {
          processOperationQueue()

          selector.select(10000)

          /**
           * The idiomatic Java iterator construct is used here to save an
           * implicit conversion for each iteration. This is a tight loop, so
           * saving (several) object allocations can make a difference.
           */
          val selectedKeys = selector.selectedKeys.iterator
          while (selectedKeys.hasNext) {
            val key = selectedKeys.next()
            selectedKeys.remove()
            if (key.isValid) {
              val readyOps = key.readyOps
              if ((readyOps & SelectionKey.OP_ACCEPT) != 0) // mutually exclusive
                processAccept(key)
              else if ((readyOps & SelectionKey.OP_CONNECT) != 0) // mutually exclusive
                processConnect(key)
              else { // can do both reads and writes at the same time
                if ((readyOps & SelectionKey.OP_WRITE) != 0) processWrite(key) // do writes first
                if ((readyOps & SelectionKey.OP_READ) != 0)  processRead(key)  // then do reads
              }
            } else
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

    override def newAlreadyTerminatedException() = new ProviderAlreadyClosedException

    /**
     * Node already in canonical form
     */
    def connect(node: Node, receiveCallback: BytesReceiveCallback) = 
      withoutTermination {
        ensureAlive()
        Debug.info(this + ": connect called to: " + node)
        val clientSocket = SocketChannel.open
        clientSocket.socket.setTcpNoDelay(true)
        clientSocket.configureBlocking(false)
        val connected = clientSocket.connect(new InetSocketAddress(node.address, node.port))
        val interestOp = if (connected) SelectionKey.OP_READ else SelectionKey.OP_CONNECT
        val conn = new InitiatedNonBlockingServiceConnection(node, clientSocket, receiveCallback)
        addOperation(new RegisterChannel(clientSocket, interestOp, Some(conn)))
        conn
      }

    def listen(port: Int, 
               connectionCallback: ConnectionCallback[ByteConnection], 
               receiveCallback: BytesReceiveCallback) =
      withoutTermination {
        ensureAlive()
        Debug.info(this + ": listening on port: " + port)
        val serverSocketChannel = ServerSocketChannel.open
        serverSocketChannel.configureBlocking(false)
        serverSocketChannel.socket.bind(new InetSocketAddress(port))
        val listener = new NonBlockingServiceListener(serverSocketChannel.socket.getLocalPort, 
            serverSocketChannel, connectionCallback, receiveCallback)
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
      query.attachments.foreach(_.doTerminate(isBottom))

      // now shut the loop down
      terminated = true
      Debug.info(this + ": closing selector")
      selector.close()
    }

  }

  override def connect(node: Node, receiveCallback: BytesReceiveCallback) = {
    nextConnectionLoop().connect(node, receiveCallback)
  }

  override def listen(port: Int, 
                      connectionCallback: ConnectionCallback[ByteConnection], 
                      receiveCallback: BytesReceiveCallback) = {
    nextListenerLoop().listen(port, connectionCallback, receiveCallback)
  }

  override def doTerminateImpl(isBottom: Boolean) {
    Debug.info(this + ": doTerminateImpl() - closing connLoops")
    connLoops foreach { _.doTerminate(isBottom) }

    Debug.info(this + ": doTerminateImpl() - closing listenerLoops")
    listenerLoops foreach { _.doTerminate(isBottom) } 

    Debug.info(this + ": doTerminateImpl() - closing executors")
    listenerExecutor.shutdownNow()
    connExecutor.shutdownNow()
  }

}
