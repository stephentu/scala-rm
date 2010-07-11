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
import java.net.{ InetAddress, ServerSocket, Socket, UnknownHostException }
import java.util.concurrent.Executors

import scala.collection.mutable.HashMap


class BlockingServiceProvider extends ServiceProvider {

  private val executor = Executors.newCachedThreadPool()

  class BlockingServiceWorker(
      so: Socket, 
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
      terminated = true
      so.close()
    }

    private def send0(data: Array[Byte]) {
      dataout.writeInt(data.length)
      dataout.write(data)
    }

    private def ensureAlive() {
      if (terminateInitiated) throw new ConnectionAlreadyClosedException
    }

    override def send(data: Array[Byte]) {
      terminateLock.synchronized {
        ensureAlive()
        assert(data.length > 0)
        try {
          send0(data)
          dataout.flush()
        } catch {
          case e: IOException => 
            Debug.error(this + ": caught " + e.getMessage)
            Debug.doError { e.printStackTrace }
            terminateBottom()
        }
      }
    }

    override def send(data0: Array[Byte], data1: Array[Byte]) {
      terminateLock.synchronized {
        ensureAlive()
        assert(data0.length > 0 && data1.length > 0)
        try {
          send0(data0)
          send0(data1)
          dataout.flush()
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
        case e: Exception =>
          Debug.error(this + ": caught " + e.getMessage)
          Debug.doError { e.printStackTrace }
          terminateBottom()
      }

    }

    override def toString = "<BlockingServiceWorker: " + so + ">"
  }

  class BlockingServiceListener(
      override val port: Int, 
      override val connectionCallback: ConnectionCallback,
      receiveCallback: BytesReceiveCallback)
    extends Runnable
    with    Listener {

    override def mode = ServiceMode.Blocking

    @volatile
    private var terminated = false

    private val serverSocket = new ServerSocket(port)

    override def run() {
      try {
        while (!terminated) {
          val client = serverSocket.accept()
          val conn = new BlockingServiceWorker(client, receiveCallback)
          executor.execute(conn)
          receiveConnection(conn)
        }
      } catch {
        case e: Exception =>
          Debug.error(this + ": caught " + e.getMessage)
          Debug.doError { e.printStackTrace }
          terminateBottom()
      }
    }

    override def doTerminateImpl(isBottom: Boolean) {
      terminated = true
      serverSocket.close()
    }

    override def toString = "<BlockingServiceListener: " + serverSocket + ">"

  }

  override def mode = ServiceMode.Blocking

  private def ensureAlive() {
    if (terminateInitiated) throw new ProviderAlreadyClosedException
  }

  override def connect(node: Node, receiveCallback: BytesReceiveCallback) = terminateLock.synchronized {
    ensureAlive()
    val socket = new Socket(node.address, node.port)
    val worker = new BlockingServiceWorker(socket, receiveCallback)
    executor.execute(worker)
    worker
  }

  override def listen(port: Int, connectionCallback: ConnectionCallback, receiveCallback: BytesReceiveCallback) = {
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
