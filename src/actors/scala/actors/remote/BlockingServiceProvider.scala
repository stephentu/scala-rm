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
      override val receiveCallback: (Connection, Array[Byte]) => Unit)
    extends Runnable
    with    Connection 
    with    EncodingHelpers 
    with    BytesReceiveCallbackAware {

    override def mode = ServiceMode.Blocking

    private val datain  = new DataInputStream(so.getInputStream)
    private val dataout = new DataOutputStream(so.getOutputStream)

    override val remoteNode = Node(so.getInetAddress.getHostName,  so.getPort) 
    override val localNode  = Node(so.getLocalAddress.getHostName, so.getLocalPort)

    @volatile
    private var terminated = false

    override def doTerminate() {
      synchronized {
        terminated = true
        so.close()
      }
    }

    private def send0(data: Array[Byte]) {
      dataout.writeInt(data.length)
      dataout.write(data)
    }

    private def ensureAlive() {
      if (terminated) throw new ConnectionAlreadyClosedException
    }

    override def send(data: Array[Byte]) {
      synchronized {
        ensureAlive()
        assert(data.length > 0)
        try {
          send0(data)
          dataout.flush()
        } catch {
          case e: IOException => 
            Debug.error(this + ": caught " + e.getMessage)
            Debug.doError { e.printStackTrace }
            terminate()
        }
      }
    }

    override def send(data0: Array[Byte], data1: Array[Byte]) {
      synchronized {
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
            terminate()
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
          terminate()
      }

    }

    override def toString = "<BlockingServiceWorker: " + so + ">"
  }

  class BlockingServiceListener(
      override val port: Int, 
      override val connectionCallback: (Listener, Connection) => Unit,
      receiveCallback: (Connection, Array[Byte]) => Unit) 
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
          terminate()
      }
    }

    override def doTerminate() {
      terminated = true
      serverSocket.close()
    }

    override def toString = "<BlockingServiceListener: " + serverSocket + ">"

  }

  override def mode = ServiceMode.Blocking

  override def connect(node: Node, receiveCallback: BytesReceiveCallback): Connection = {
    val socket = new Socket(node.address, node.port)
    val worker = new BlockingServiceWorker(socket, receiveCallback)
    executor.execute(worker)
    worker
  }

  override def listen(port: Int, connectionCallback: ConnectionCallback, receiveCallback: BytesReceiveCallback) = {
    val listener = new BlockingServiceListener(port, connectionCallback, receiveCallback)
    executor.execute(listener)
    listener
  }

  override def terminate() {
    executor.shutdownNow() 
  }

  override def toString = "<BlockingServiceProvider>"

}
