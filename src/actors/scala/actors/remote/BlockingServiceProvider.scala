/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote


import java.io.{DataInputStream, DataOutputStream, IOException}
import java.lang.{Thread, SecurityException}
import java.net.{InetAddress, ServerSocket, Socket, UnknownHostException}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.HashMap

object BlockingServiceProvider {
  private val counter = new AtomicInteger
  def nextWorkerId = counter.getAndIncrement
}

class BlockingServiceWorker(
    so: Socket, 
    override val receiveCallback: (Connection, Array[Byte]) => Unit)
  extends Thread("BlockingServiceWorker-" + BlockingServiceProvider.nextWorkerId)
  with    Connection 
  with    EncodingHelpers 
  with    BytesReceiveCallbackAware {

  override def mode = ServiceMode.Blocking

  private val datain  = new DataInputStream(so.getInputStream)
  private val dataout = new DataOutputStream(so.getOutputStream)

  override val remoteNode = Node(so.getInetAddress.getHostName,  so.getPort) 
  override val localNode  = Node(so.getLocalAddress.getHostName, so.getLocalPort)

  private var terminated = false

  override def doTerminate() {
    synchronized { 
      if (terminated) return 
      else terminated = true
    }
    Debug.info(this + ": doTerminate()")
    so.close() 
  }

  private def send0(data: Array[Byte]) {
    dataout.writeInt(data.length)
    dataout.write(data)
  }

  override def send(data: Array[Byte]) {
    synchronized {
      if (terminated)
        throw new IllegalStateException("Cannot send when not running")
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
      if (terminated)
        throw new IllegalStateException("Cannot send when not running")
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

  // Start running immediately
  start()

  override def run() {
    try {
      while (true) {
        val length = datain.readInt()
        if (length == 0) {
          receiveBytes(Array[Byte]())       
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
  extends Thread("BlockingServiceListener-" + port)
  with    Listener {

  override def mode = ServiceMode.Blocking

  private var terminated = false

  private val serverSocket = new ServerSocket(port)

  override def run() {
    try {
      while (true) {
        val client = serverSocket.accept()
        val conn = new BlockingServiceWorker(client, receiveCallback)
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
    synchronized {
      if (terminated) return
      terminated = true
    }
    serverSocket.close()
  }

  override def toString = "<BlockingServiceListener: " + port + ">"

}

class BlockingServiceProvider extends ServiceProvider {

  override def mode = ServiceMode.Blocking

  override def connect(node: Node, receiveCallback: BytesReceiveCallback): Connection = {
    val socket = new Socket(node.address, node.port)
    // ctor starts thread
    new BlockingServiceWorker(socket, receiveCallback)
  }

  override def listen(port: Int, connectionCallback: ConnectionCallback, receiveCallback: BytesReceiveCallback) = {
    val listener = new BlockingServiceListener(port, connectionCallback, receiveCallback)
    listener.start()
    listener
  }

  override def terminate() {

  }

  override def toString = "<BlockingServiceProvider>"

}
