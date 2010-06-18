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
                           SelectableChannel, ServerSocketChannel, SocketChannel }
import java.nio.channels.spi.SelectorProvider

import scala.collection.mutable.{ HashMap, ListBuffer, Queue }

object NioService {

  private val ports = new HashMap[Int, NioService]

  implicit def nodeToInetAddress(node: Node): InetSocketAddress = 
    node.toInetSocketAddress

  implicit def inetAddressToNode(addr: InetSocketAddress): Node =
    Node(addr.getHostName, addr.getPort)

  implicit def socketToInetSocketAddress(socket: Socket): InetSocketAddress = 
    new InetSocketAddress(socket.getInetAddress, socket.getPort)

  implicit def socketToNode(socket: Socket): Node =
    inetAddressToNode(socketToInetSocketAddress(socket))

  def apply(port: Int, serializer: Serializer) = ports.synchronized {
    ports.get(port) match {
      case Some(service) =>
        if (service.serializer != serializer)
          throw new IllegalArgumentException("Cannot apply with different serializer")
        service
      case None =>
        val service = new NioService(port, serializer)
        serializer.service = service
        ports += Pair(port, service)
        service.start()
        Debug.info("created NIO service at "+service.node)
        service
    }
  }

}

class NioService(port: Int, val serializer: Serializer) extends Service {
  import NioService._

  private val internalNode = Node(InetAddress.getLocalHost.getHostAddress, port)
  private val inetAddress  = internalNode.toInetSocketAddress
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
    channelWriteQueue.synchronized {
      val queue = getQueueForChannel(chan)
      queue += bytes
      /** Signal to writeLoop that new data is available */
      channelWriteQueue.notifyAll
    }
  }

  sealed trait ChannelStateEnum
  /** In the middle of reading the message size header (4 bytes) */
  case object ReadingSize extends ChannelStateEnum
  /** In the middle of reading the variable size message */
  case object ReadingMessage extends ChannelStateEnum

  private class ChannelState {
    var buffer = ByteBuffer.allocate(4)
    var state: ChannelStateEnum = ReadingSize
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

  private def getChannelState(chan: SocketChannel) = channelMap.get(chan) match {
    case Some(state) => state
    case None        =>
      val state = new ChannelState
      channelMap += chan -> state
      state
  }
  
  private sealed abstract class RegisterInterestOp(val socket: SelectableChannel) {
    val op: Int /** SelectionKey.OP_XXX */
    def finish(): Unit
  }

  private case class AcceptOp(override val socket: SelectableChannel) 
  extends RegisterInterestOp(socket) {
    override val op = SelectionKey.OP_ACCEPT
    override def finish() { } 
  }

  private case class ConnectOp(override val socket: SelectableChannel, future: ConnectFuture)
  extends RegisterInterestOp(socket) {
    override val op = SelectionKey.OP_CONNECT
    override def finish() { future.finished }
  }

  private class ConnectFuture(clientSocket: SocketChannel) {
    var alreadyDone = false
    def await = {
      synchronized {
        if (!alreadyDone) wait
      }
    }
    def finished = {
      synchronized {
        alreadyDone = true
        notifyAll
      }
    }
  }

  /** Spawn the server */
  serve()

  def start() {
    /** Do nothing, since ctor starts service */
  }

  private def serve() {
    serverSocketChannel.configureBlocking(false)
    serverSocketChannel.socket.bind(inetAddress)
    serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT)
    Debug.info(this + ": Now serving on channel " + node)
    startSelectLoop
    startWriteLoop
  }

  private def startAction(action: => Unit) {
    val t = new Thread(new Runnable {
      override def run = action
    })
    t.setDaemon(true)
    t.start
  }

  private def startSelectLoop {
    startAction(selectLoop)
  }

  /** Warning: direct byte buffers are not guaranteed to be backed by array */
  private val readBuffer = ByteBuffer.allocateDirect(8192) 

  private def selectLoop {
    def finishKey(key: SelectionKey) {
      key.attachment match {
        case null                  => /* do nothing */
        case r: RegisterInterestOp => r.finish()
      }
    }

    def processAccept(key: SelectionKey) {
      /** For a channel to receive an accept even, it must be a server channel */
      val serverSocketChannel = key.channel.asInstanceOf[ServerSocketChannel]
      val clientSocketChannel = serverSocketChannel.accept
      clientSocketChannel.configureBlocking(false)

      /** This is OK to do because we're running in the same thread as the select loop */
      clientSocketChannel.register(selector, SelectionKey.OP_READ)

      registerChannel(
        new InetSocketAddress(clientSocketChannel.socket.getInetAddress, 
                              clientSocketChannel.socket.getPort),
        clientSocketChannel)

      finishKey(key)
    }

    def processConnect(key: SelectionKey) {
      val socketChannel = key.channel.asInstanceOf[SocketChannel]
      try {
        socketChannel.finishConnect
      } catch {
        case e: IOException =>
          // Cancel the channel's registration with our selector
          Debug.error(this + ": caught IO exception on finishing connect: " + e.getMessage)
          key.cancel
        return
      }

      /** Key is already registered, so only need to change interest ops to read */
      key.interestOps(SelectionKey.OP_READ)

      /** Don't need to register channel, since connect() already does that */

      finishKey(key)
    }

    def processRead(key: SelectionKey) {
      val socketChannel = key.channel.asInstanceOf[SocketChannel]
      readBuffer.clear
      var bytesRead = 0
      try {
        bytesRead = socketChannel.read(readBuffer)
      } catch {
        case e: IOException =>
        // The remote forcibly closed the connection! Cancel
        // the selection key and close the channel.
        key.cancel
        socketChannel.close
        return
      }
      if (bytesRead == -1) {
        // Remote entity shut the socket down cleanly. Do the
        // same from our end and cancel the channel.
        key.cancel
        socketChannel.close
        return
      }
      val chunk = new Array[Byte](bytesRead)
      readBuffer.rewind
      readBuffer.get(chunk) /** copy what we just read into chunk */

      val state = getChannelState(socketChannel)
      state.consume(chunk)

      while (state.hasSerializerMessage) {
        val (meta, data) = state.nextSerializerMessage
        val msg = serializer.deserialize(Some(meta), data)
        kernel.processMsg(socketChannel.socket, msg)
      }

    }

    while (true) {
      try {
        registerQueue.synchronized {
          for (op <- registerQueue) {
            op.socket.register(selector, op.op, op)
          }
          registerQueue.clear /** Flush queue because we've finished registering these ops */
        }
        selector.select() /** This is a blocking operation */
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
    while (true) {
      try {
        channelWriteQueue.synchronized {
          if (channelWriteQueue.isEmpty)
            channelWriteQueue.wait
          channelWriteQueue.retain { (chan, queue) => 
            if (!chan.isConnected)
              false
            else {
              var keepTrying = !queue.isEmpty
              while (keepTrying) {
                val buf = queue.head
                try {
                  chan.write(buf)
                  if (buf.remaining > 0) {
                    /** This channel is not ready to write anymore, so move on */
                    keepTrying = false 
                  } else {
                    /** Dequeue this buffer and move on */
                    queue.dequeue
                  } 
                } catch {
                  case e =>
                    Debug.error(this + " caught exception when writing: " + e.getMessage)
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

  private def encodeMessage(data: Array[Byte]): ByteBuffer = {
    val buf = ByteBuffer.allocate(4 + data.length)
    buf.putInt(data.length)
    buf.put(data)
    buf
  }

  def send(node: Node, data: Array[Byte]) {
    enqueueOnChannel(getChannel(node), encodeMessage(data))
  }

  def connect(addr: InetSocketAddress) = connectionMap.synchronized {
    connectionMap.get(addr) match {
      case Some(chan) => chan
      case None       => connect0(addr)
    }
  }

  /** Assumes that connectionMap lock is currently held, and
   *  assumes no valid connection to addr exists in connectionMap */
  private def connect0(addr: InetSocketAddress) = {
    val clientSocket = SocketChannel.open
    clientSocket.configureBlocking(false)
    val connected = clientSocket.connect(addr)
    connectionMap += addr -> clientSocket
    if (connected) {
      // TODO: do we need to change interest ops here?
      clientSocket      
    } else {
      val future = new ConnectFuture(clientSocket)
      registerOpChange(ConnectOp(clientSocket, future))
      future.await
      clientSocket
    }
  }

  def terminate() {
    // Terminate all remaining connections
    connectionMap.synchronized {
      connectionMap.valuesIterator.foreach(_.socket.close)
    }
    // Stop listening
    serverSocketChannel.close
  }

  override def toString = "<NioService: " + node + ">"

}
