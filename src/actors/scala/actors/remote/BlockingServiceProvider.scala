/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote


import java.io.{ DataInputStream, DataOutputStream, IOException }
import java.net.{ InetSocketAddress, ServerSocket, Socket, 
                  SocketException, UnknownHostException }
import java.util.concurrent.{ ConcurrentHashMap, Executors }

import scala.collection.mutable.HashMap


class BlockingServiceProvider extends ServiceProvider {

  private val executor = Executors.newCachedThreadPool()

  class BlockingServiceWorker(
      so: Socket, 
      override val isEphemeral: Boolean,
      override val receiveCallback: BytesReceiveCallback)
    extends Runnable
    with    ByteConnection 
    with    EncodingHelpers {

    override def mode = ServiceMode.Blocking

    private val datain  = new DataInputStream(so.getInputStream)
    private val dataout = new DataOutputStream(so.getOutputStream)

    override val remoteNode = Node(so.getInetAddress.getHostName,  so.getPort) 
    override val localNode  = Node(so.getLocalAddress.getHostName, so.getLocalPort)

    @volatile
    private var terminated = false

    override def doTerminateImpl(isBottom: Boolean) {
      Debug.info(this + ": doTerminateImpl(" + isBottom + ")")
      terminated = true
      dataout.flush()
      datain.close()
      dataout.close()
      so.close()
    }

    override def newAlreadyTerminatedException() = new ConnectionAlreadyClosedException

    override def send(seq: ByteSequence) {
      terminateLock.synchronized {
        ensureAlive()
        try {
					dataout.writeInt(seq.length)
					dataout.write(seq.bytes, seq.offset, seq.length)
          dataout.flush() // TODO: do we need to flush every message?
        } catch {
          case e: IOException => 
            Debug.error(this + ": caught " + e.getMessage)
            Debug.doError { e.printStackTrace }
            terminateBottom()
        }
      }
    }

    override def run() {
      try {
        while (!terminated) {
          val length = datain.readInt()
          if (length == 0) {
            receiveBytes(new Array[Byte](0))
          } else if (length < 0) {
            throw new IllegalStateException("received negative length message size header")
          } else {
            // TODO: need to split up into some buf size chunks, so
            // somebody can't send 0x0fffffff as a message size and eat up
            // lots of memory
            val bytes = new Array[Byte](length)
            datain.readFully(bytes, 0, length)
            receiveBytes(bytes)
          }
        }
      } catch {
        case e: IOException if (terminated) =>
          Debug.info(this + ": listening thread is shutting down")
        case e: Exception =>
          Debug.error(this + ": caught " + e.getMessage)
          Debug.doError { e.printStackTrace }
          terminateBottom()
      }

    }

    override def toString = "<BlockingServiceWorker: " + so + ">"
  }

  private val DUMMY_VALUE = new Object

  class BlockingServiceListener(
			serverPort: Int,
      override val connectionCallback: ConnectionCallback[ByteConnection],
      receiveCallback: BytesReceiveCallback)
    extends Runnable
    with    Listener {

    override def mode = ServiceMode.Blocking

    @volatile
    private var terminated = false

    private val serverSocket = new ServerSocket(serverPort)

		override def port = serverSocket.getLocalPort

    private val childConnections = new ConcurrentHashMap[ByteConnection, Object]

    override def run() {
      try {
        while (!terminated) {
          val client = serverSocket.accept()
          client.setTcpNoDelay(true)
          val conn = new BlockingServiceWorker(client, true, receiveCallback)
          conn afterTerminate { isBottom =>
            childConnections.remove(conn)
          }
          childConnections.put(conn, DUMMY_VALUE)
          executor.execute(conn)
          receiveConnection(conn)
        }
      } catch {
        case e: SocketException if (terminated) =>
          Debug.error(this + ": Listening thread is shutting down")
        case e: Exception =>
          Debug.error(this + ": caught " + e.getMessage)
          Debug.doError { e.printStackTrace }
          terminateBottom()
      }
    }

    override def doTerminateImpl(isBottom: Boolean) {
      terminated = true
      import scala.collection.JavaConversions._
      childConnections.keys.foreach(_.doTerminate(isBottom))
      childConnections.clear()
      serverSocket.close()
    }

    override def toString = "<BlockingServiceListener: " + serverSocket + ">"

  }

  override def mode = ServiceMode.Blocking

  override def newAlreadyTerminatedException() = new ProviderAlreadyClosedException

  override def connect(node: Node, receiveCallback: BytesReceiveCallback) = terminateLock.synchronized {
    ensureAlive()
    val socket = new Socket
    socket.setTcpNoDelay(true)
    socket.connect(new InetSocketAddress(node.address, node.port))
    val worker = new BlockingServiceWorker(socket, false, receiveCallback)
    executor.execute(worker)
    worker
  }

  override def listen(port: Int, 
                      connectionCallback: ConnectionCallback[ByteConnection], 
                      receiveCallback: BytesReceiveCallback) = {
    ensureAlive()
    val listener = new BlockingServiceListener(port, connectionCallback, receiveCallback)
    executor.execute(listener)
    listener
  }

  override def doTerminateImpl(isBottom: Boolean) {
    executor.shutdownNow() 
  }

  override def toString = "<BlockingServiceProvider>"

}
