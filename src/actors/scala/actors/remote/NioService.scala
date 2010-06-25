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
import java.nio.channels.spi.SelectorProvider

import scala.collection.mutable.{ HashMap, ListBuffer, Queue }

object NioService extends ServiceCreator with NodeImplicits {

  type MyService = NioService
    
  def newService(port: Int, serializer: Serializer) = new NioService(port, serializer)

}

class NioService(port: Int, val serializer: Serializer) extends Service {
  import NioService._

  private val internalNode = Node(InetAddress.getLocalHost.getHostAddress, port)
  private val inetAddress  = new InetSocketAddress(port) 
  private val selector = SelectorProvider.provider.openSelector
  private val serverSocketChannel = ServerSocketChannel.open

  def node = internalNode

  private val registerQueue = new ListBuffer[RegisterInterestOp]
  private val connectionMap = new HashMap[InetSocketAddress, SocketChannel]
  private val channelMap    = new HashMap[SocketChannel, ChannelState]
  private val channelWriteQueue = new HashMap[SocketChannel, Queue[ByteBuffer]]
  private val connectionLock = new Object

  private def registerOpChange(op: RegisterInterestOp) {
    registerQueue.synchronized {
      registerQueue += op
    }
  }

  private def registerChannel(addr: InetSocketAddress, chan: SocketChannel) {
    connectionMap.synchronized {
      connectionMap += addr -> chan
    }
  }

  private def getChannel(addr: InetSocketAddress) = connectionMap.synchronized {
    connectionMap.get(addr) match {
      case Some(chan) => chan
      case None       => connect0(addr)
    }
  }

  /** Assumes lock for channelWriteQueue is already held */
  private def getQueueForChannel(chan: SocketChannel) = channelWriteQueue.get(chan) match {
    case Some(queue) => queue
    case None        =>
      val queue = new Queue[ByteBuffer]
      channelWriteQueue += chan -> queue
      queue
  }

  private def enqueueOnChannel(chan: SocketChannel, bytes: ByteBuffer) {
    Debug.info(this + ": enqueueOnChannel(chan = " + chan + ")")
    channelWriteQueue.synchronized {
      val queue = getQueueForChannel(chan)
      queue += bytes
      /** Signal to writeLoop that new data is available */
      Debug.info(this + ": signaling to writeLoop of new data")
      channelWriteQueue.notifyAll
    }
  }

  private class ChannelState(chan: SocketChannel) {
    val messageState   = new MessageState
    val handshakeState = serializer.initialState.map(new HandshakeState(chan, _)) 
    def reset {
      messageState.reset
      handshakeState.foreach(_.reset)
    }
  }

  sealed trait MessageStateEnum
  /** In the middle of reading the message size header (4 bytes) */
  case object ReadingSize extends MessageStateEnum
  /** In the middle of reading the variable size message */
  case object ReadingMessage extends MessageStateEnum

  private class MessageState {
    var buffer = ByteBuffer.allocate(4)
    var state: MessageStateEnum = ReadingSize
    var messageSize  = 4
    val messageQueue = new Queue[Array[Byte]]

    def consume(bytes: Array[Byte]) {
      import scala.math.min
      var idx = 0
      while (idx < bytes.length) {
        val bytesToPut = min(bytes.length - idx, bytesLeftInState)
        buffer.put(bytes, idx, bytesToPut)     
        idx += bytesToPut
        if (isFull) {
          buffer.flip
          state match {
            case ReadingSize    =>
              val size = buffer.getInt
              startReadingMessage(size)
            case ReadingMessage =>
              val msg = new Array[Byte](messageSize)
              buffer.get(msg)
              messageQueue += msg
              startReadingSize
          } 
        }
      }
    }

    /** True iff there is a message ( metadata + message ) ready to process
     *  by the serializer */
    def hasSerializerMessage = messageQueue.size >= 2

    def nextSerializerMessage = {
      assert(hasSerializerMessage)
      val first  = messageQueue.dequeue
      val second = messageQueue.dequeue
      (first, second)
    } 

    def startReadingSize {
      buffer      = ByteBuffer.allocate(4) 
      state       = ReadingSize
      messageSize = 4
    }

    def startReadingMessage(size: Int) {
      buffer      = ByteBuffer.allocate(size) 
      state       = ReadingMessage
      messageSize = size 
    }

    def reset {
      messageQueue.clear
      startReadingSize
    }

    /** The number of bytes left needed to read before a state change */
    def bytesLeftInState = messageSize - buffer.position

    /** IS the buffer full */
    def isFull = bytesLeftInState == 0

  }

  /** Use a trait instead of boolean because its more readible */
  sealed trait  SendOrReceive
  case   object Send          extends SendOrReceive
  case   object Receive       extends SendOrReceive

  private class HandshakeState(chan: SocketChannel, initialState: Any) {
    var done     = false
    var curState = initialState
    var sendOrReceive: SendOrReceive = Send
    var future   = new Future

    def isSending   = sendOrReceive == Send
    def isReceiving = sendOrReceive == Receive

    def nextHandshakeMessage = {
      assert(!done && isSending) 
      if (serializer.nextHandshakeMessage.isDefinedAt(curState)) {
        val (nextState, nextMsg) = serializer.nextHandshakeMessage.apply(curState)
        curState = nextState
        flip
        nextMsg match {
          case Some(_) => nextMsg
          case None    =>
            done = true
            None
        }
      } else 
        future.finishWithError(new IllegalHandshakeStateException)
    }

    def handleNextMessage(m: Any) {
      assert(!done && isReceiving)
      if (serializer.handleHandshakeMessage.isDefinedAt((curState, m))) {
        val nextState = serializer.handleHandshakeMessage((curState, m))
        curState = nextState
        flip
      } else 
        future.finishWithError(new IllegalHandshakeStateException)
    }

    def sendOnce {
      nextHandshakeMessage match {
        case Some(message) =>
          /** Send message away */
          val rawMsg = Array(Array[Byte](), serializer.javaSerialize(message.asInstanceOf[AnyRef]))
          enqueueOnChannel(chan, toByteBuffer(encodeAndConcat(rawMsg))) 
        case None          => 
          /** Finished with handshake */
          assert(done)
          future.finish
      }
    }

    def processMessage(m: Any) {
      handleNextMessage(m)
      sendOnce
    }

    def flip {
      assert (!done)
      sendOrReceive match {
        case Send    => sendOrReceive = Receive
        case Receive => sendOrReceive = Send
      }
    }

    def reset {
      done          = false
      curState      = initialState
      sendOrReceive = Send
      future        = new Future
    }

  }

  private def getChannelState(chan: SocketChannel) = channelMap.synchronized {
    channelMap.get(chan) match {
      case Some(state) => state
      case None        =>
        val state   = new ChannelState(chan)
        channelMap += chan -> state
        state
    }
  }

  private def allChannelStates = channelMap.synchronized {
    channelMap.valuesIterator.toList /** We want the copy */
  }

  private sealed abstract class RegisterInterestOp(val socket: SocketChannel) 
  extends Finishable {
    val op: Int /** SelectionKey.OP_XXX */
  }

  private case class AcceptOp(override val socket: SocketChannel) 
  extends RegisterInterestOp(socket) {
    override val op = SelectionKey.OP_ACCEPT
    override def finish() { } 
    override def finishWithError(ex: Throwable) { }
  }

  private case class ConnectOp(override val socket: SocketChannel, future: Future)
  extends RegisterInterestOp(socket) {
    override val op = SelectionKey.OP_CONNECT
    override def finish() { 
      assert(socket.isConnected)
      /** Bootstrap the handshake, before awaking connect0() */
      getChannelState(socket).handshakeState.foreach(_.sendOnce)
      future.finish 
    }
    override def finishWithError(ex: Throwable) { future.finishWithError(ex) }
  }

  /** Spawn the server */
  serve()

  def start() {
    /** Do nothing, since ctor starts service */
  }

  private def serve() {
    serverSocketChannel.configureBlocking(false)
    serverSocketChannel.socket.bind(inetAddress)
    /** Selector isn't running yet so we don't have to register in queue */
    serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT)
    Debug.info(this + ": Now serving on channel " + node)
    startSelectLoop
    startWriteLoop
  }

  private def startAction(action: => Unit) {
    val t = new Thread(new Runnable {
      override def run = action
    })
    t.setDaemon(true) /** Set daemon so we don't have to worry about explicit termination */
    t.start
  }

  private def startSelectLoop {
    startAction(selectLoop)
  }

  /** Warning: direct byte buffers are not guaranteed to be backed by array */
  private val readBuffer = ByteBuffer.allocateDirect(8192) 

  private def selectLoop {

    def finishKey(key: SelectionKey) = finishKey0(key, None)
    def finishKeyWithError(key: SelectionKey, ex: Throwable) = finishKey0(key, Some(ex))
    def finishKey0(key: SelectionKey, ex: Option[Throwable]) {
      key.attachment match {
        case null          => /* do nothing */
        case r: Finishable => ex match {
          case None    => r.finish()
          case Some(e) => r.finishWithError(e)
        }
        case u             => Debug.error(this + ": found unfinishable key as attachment: " + u)
      }
      key.attach(null) /** Discard attachment after usage */
    }

    def processAccept(key: SelectionKey) {
      /** For a channel to receive an accept even, it must be a server channel */
      val serverSocketChannel = key.channel.asInstanceOf[ServerSocketChannel]
      val clientSocketChannel = serverSocketChannel.accept
      Debug.info(this + ": processAccept on channel " + serverSocketChannel + " from " + clientSocketChannel)
      clientSocketChannel.configureBlocking(false)

      /** This is OK to do because we're running in the same thread as the select loop */
      clientSocketChannel.register(selector, SelectionKey.OP_READ)

      registerChannel(clientSocketChannel.socket, clientSocketChannel)

      key.attach(new Finishable {
        def finish() {
          val state = getChannelState(clientSocketChannel) 
          /** Bootstrap the handshake */
          state.handshakeState.foreach(_.sendOnce)
        }
        /** shouldn't be called */
        def finishWithError(ex: Throwable) { throw ex }
      })

      /** finish key w/ handshake bootstrap */
      finishKey(key)
    }

    def processConnect(key: SelectionKey) {
      val socketChannel = key.channel.asInstanceOf[SocketChannel]
      Debug.info(this + ": processConnect on channel " + socketChannel)
      try {
        socketChannel.finishConnect
      } catch {
        case e: IOException =>
          // Cancel the channel's registration with our selector
          Debug.error(this + ": caught IO exception on finishing connect: " + e.getMessage)
          finishKeyWithError(key, e)
          key.cancel
          return
      }

      /** Key is already registered, so only need to change interest ops to read */
      key.interestOps(SelectionKey.OP_READ)

      /** Don't need to register channel, since connect() already does that */

      /** Don't need to boostrap handshake, since connect0() does that */

      Debug.info(this + ": finishing connection to channel: " + socketChannel)
      finishKey(key)
    }

    def processRead(key: SelectionKey) {

      val socketChannel = key.channel.asInstanceOf[SocketChannel]
      val state = getChannelState(socketChannel)

      def tearDownChannel(ex: Throwable) {
        key.cancel
        socketChannel.close
        state.handshakeState.foreach(_.future.finishWithError(ex))
      }

      Debug.info(this + ": processRead on channel " + socketChannel)

      readBuffer.clear
      var bytesRead = 0

      try {
        bytesRead = socketChannel.read(readBuffer)
      } catch {
        case e: IOException =>
        // The remote forcibly closed the connection! Cancel
        // the selection key and close the channel.
        Debug.error(this + ": processRead caught IOException when reading from " + 
            socketChannel + ": " + e.getMessage)
        tearDownChannel(e)
        return
      }

      if (bytesRead == -1) {
        // Remote entity shut the socket down cleanly. Do the
        // same from our end and cancel the channel.
        Debug.error(this + ": processRead - remote shutdown (read -1 bytes)")
        tearDownChannel(new IllegalHandshakeStateException)
        return
      }

      Debug.info(this + ": read " + bytesRead + " bytes from channel: " + socketChannel)
      val chunk = new Array[Byte](bytesRead)
      readBuffer.rewind
      readBuffer.get(chunk) /** copy what we just read into chunk */

      /** Comsume the chunk into the message state handler */
      state.messageState.consume(chunk)

      def forwardKernel(meta: Array[Byte], data: Array[Byte]) {
        val msg = serializer.deserialize(Some(meta), data)
        kernel.processMsg(socketChannel.socket, msg)
      }

      def forwardHandshake(meta: Array[Byte], data: Array[Byte]) {
        val handshakeState = state.handshakeState.get
        assert(!handshakeState.done && handshakeState.isReceiving)
        val msg = serializer.javaDeserialize(Some(meta), data)
        handshakeState.processMessage(msg)
      }

      while (state.messageState.hasSerializerMessage) {
        val (meta, data) = state.messageState.nextSerializerMessage
        state.handshakeState match {
          case Some(handshakeState) =>
            if (handshakeState.done)
              forwardKernel(meta, data)
            else if (handshakeState.isReceiving)
              forwardHandshake(meta, data)
            else {
              Debug.error(this + ": processRead - handshake should never be sending here")
              tearDownChannel(new IllegalHandshakeStateException)
            }
          case None                 => 
            forwardKernel(meta, data)
        }
      }

    }

    Debug.info(this + ": selectLoop started...")
    while (true) {
      try {
        registerQueue.synchronized {
          for (op <- registerQueue) {
            op.socket.register(selector, op.op, op)
          }
          registerQueue.clear /** Flush queue because we've finished registering these ops */
        }
        Debug.info(this + ": selectLoop calling select()")
        selector.select() /** This is a blocking operation */
        Debug.info(this + ": selectLoop awaken from select()")
        val selectedKeys = selector.selectedKeys.iterator
        while (selectedKeys.hasNext) {
          val key = selectedKeys.next
          selectedKeys.remove()
          if (key.isValid)
            if (key.isAcceptable)
              processAccept(key)
            else if (key.isConnectable)
              processConnect(key)
            else if (key.isReadable)
              processRead(key)
          else
            Debug.error(this + ": Invalid key found: " + key)
        }
      } catch {
        case e => 
          Debug.error(this + " caught exception in select loop: " + e.getMessage)
          e.printStackTrace
      }
    }
  }

  private def startWriteLoop = startAction(writeLoop) 

  private def writeLoop = {
    Debug.info(this + ": writeLoop started...")
    while (true) {
      try {
        channelWriteQueue.synchronized {
          while (channelWriteQueue.isEmpty) {
            Debug.info(this + ": writeLoop waiting for data...")
            channelWriteQueue.notifyAll // announce that the queue is now empty
            channelWriteQueue.wait      // wait for an announcment that the queue is no longer empty
          }
          Debug.info(this + ": writeLoop awaken, writing data...")
          channelWriteQueue.retain { (chan, queue) => 
            if (!chan.isConnected) {
              Debug.info(this + ": writeLoop found unconnected channel in queue: " + chan)
              false
            } else {
              Debug.info(this + ": writeLoop writing data for queue in channel: " + chan)
              var keepTrying = true
              while (!queue.isEmpty && keepTrying) {
                val buf = queue.head
                Debug.info(this + ": writeLoop attempting to write buf " + buf)
                try {
                  val bytesWritten = chan.write(buf)
                  Debug.info(this + ": writeLoop wrote " + bytesWritten + " bytes to " + chan)
                  if (buf.remaining > 0) {
                    /** This channel is not ready to write anymore, so move on */
                    Debug.info(this + ": writeLoop found saturated channel: " + chan)
                    keepTrying = false 
                  } else {
                    Debug.info(this + ": buf is empty " + buf)
                    /** Dequeue this buffer and move on */
                    queue.dequeue
                  } 
                } catch {
                  case ioe: IOException =>
                    Debug.error(this + " caught IOException when writing to " + chan + ": " + ioe.getMessage)
                    keepTrying = false
                } 
              }
              !queue.isEmpty
            }
          } 
        } 
      } catch { 
        case e =>
          Debug.error(this + " caught exception in write loop: " + e.getMessage)
          e.printStackTrace
      }
    }
  }

  private def toByteBuffer(data: Array[Byte]): ByteBuffer = ByteBuffer.wrap(data)

  def rawSend(node: Node, data: Array[Byte]) {
    assert(data.length > 0)
    Debug.info(this + ": send " + data.length + " bytes to node: " + node)
    val chan = getChannel(node)
    val buf  = toByteBuffer(data)
    enqueueOnChannel(chan, buf)
  }

  def connect(addr: InetSocketAddress) = connectionMap.synchronized {
    Debug.info(this + ": connect to " + addr)
    connectionMap.get(addr) match {
      case Some(chan) => chan
      case None       => connect0(addr)
    }
  }

  def localNodeFor(n: Node): Node = {
    val socket = connect(n).socket
    // be explicit because implicit conversions assume remote node
    new InetSocketAddress(socket.getLocalAddress, socket.getLocalPort) 
  }

  /** Assumes that connectionMap lock is currently held, and
   *  assumes no valid connection to addr exists in connectionMap */
  @throws(classOf[IOException])
  private def connect0(addr: InetSocketAddress) = {
    Debug.info(this + ": connect0 to " + addr)
    val clientSocket = SocketChannel.open
    clientSocket.configureBlocking(false)
    val connected = clientSocket.connect(addr)
    val sock = 
      if (connected) {
        // TODO: do we need to change interest ops here?
        Debug.info(this + ": connect0: connected immediately to " + addr)
        clientSocket
      } else {
        Debug.info(this + ": connect0: need to wait on future for " + addr)
        val future = new Future
        registerOpChange(ConnectOp(clientSocket, future))
        /** Wakeup the blocking selector */
        selector.wakeup
        Debug.info(this + ": connect0: waiting on future")
        future.await /** await() will throw exception if not 
                       * properly connected. Bootstrap of the handshake
                       * is done in the future */
        getChannelState(clientSocket).handshakeState match {
          case Some(state) => 
            Debug.info(this + ": connect0: waiting for handshake to complete...")
            state.future.await
          case None        =>
            Debug.info(this + ": connect0: no handshake state; done")
        }
        clientSocket
      }
    // only add to connection map if the socket is connected
    connectionMap += addr -> sock
    Debug.info(this + ": connect0: finished connection to " + addr)
    sock
  }

  def terminate() {
    //Debug.info(this + ": terminate: beginning termination sequence...")
    //connectionMap.synchronized { // no connections can be made now
    //  
    //  // Finish all in progress handshakes first by waiting on their futures 
    //  // This is necessary so that all remaining messages can be sent
    //  allChannelStates foreach (_.handshakeState.foreach(_.future.await))
    //  Debug.info(this + ": terminate: all handshakes completed...")

    //  channelWriteQueue.synchronized { // no new data can be placed on the queue now
    //    while (!channelWriteQueue.isEmpty) { // wait until all writes have gone out
    //      channelWriteQueue.wait             // writeLoop will announce when the queue is empty
    //    }
    //    Debug.info(this + ": terminate: closing all remaining connections...")
    //    connectionMap.valuesIterator.foreach(_.socket.close) // now we can safely terminate all connections
    //    Debug.info(this + ": terminate: stopping server socket...")
    //    serverSocketChannel.close // Stop listening
    //  }

    //}
  }

  override def toString = "<NioService: " + node + ">"

}
