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
    override val receiveCallback: ReceiveCallback)
  extends Thread("BlockingServiceWorker-" + BlockingServiceProvide.nextWorkerId)
  with    Connection 
  with    EncodingHelpers 
  with    TerminateOnError {

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

  override def send(data: Array[Byte]*) {
    rawSend(encodeAndConcat(data.toArray))
  }

  private def rawSend(data: Array[Byte]) {
    synchronized {
      if (terminated)
        throw new IllegalStateException("Cannot rawSend when not running")
      assert(data.length > 0)
      terminateOnError {
        dataout.write(data)
        data.flush()
      }
    }
  }

  // Start running immediately
  start()

  override def run() {
    terminateOnError {
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
    }
  }

  override def toString = "<BlockingServiceWorker: " + so + ">"
}

class BlockingServiceListener(
    override val port: Int, 
    override val connectionCallback: ConnectionCallback,
    receiveCallback: ReceiveCallback) 
  extends Thread("BlockingServiceListener-" + port)
  with    Listener {

  override def mode = ServiceMode.Blocking

  // start listening right away
  start()

  private val terminated = false

  private val serverSocket = new ServerSocket(port)

  override def run() {
    terminateOnError {
      while (true) {
        val client = serverSocket.accept()
        val conn = new BlockingServiceWorker(client, receiveCallback)
        receiveConnection(conn)
      }
    }
  }

  override def terminate() {
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

  override def connect(node: Node, receiveCallback: ReceiveCallback): Connection = {
    val socket = new Socket(n.address, n.port)
    // ctor starts thread
    new BlockingServiceWorker(socket, receiveCallback)
  }

  override def listen(port: Int, connectionCallback: ConnectionCallback, receiveCallback: ReceiveCallback) = {
    // ctor starts thread
    new BlockingServiceListener(port, connectionCallback, receiveCallback)
  }

  override def terminate() {

  }

  override def toString = "<BlockingServiceProvider>"

}

//private[actors] class TcpServiceWorker(id: Int, parent: TcpService, so: Socket) 
//extends Thread("TcpServiceWorker-" + id) {
//
//  import scala.concurrent.SyncVar
//
//  val datain  = new DataInputStream(so.getInputStream)
//  val dataout = new DataOutputStream(so.getOutputStream)
//
//  val remoteNode = Node(so.getInetAddress.getHostName,  so.getPort) 
//  val localNode  = Node(so.getLocalAddress.getHostName, so.getLocalPort)
//
//  def doHandshake {
//    val s = parent.serializer
//    s.initialState match {
//      case Some(initialState) =>
//
//        var curState = initialState
//        Debug.info(this + ": initialState = " + curState)
//
//        def getNextMessage =
//          if (s.nextHandshakeMessage.isDefinedAt(curState)) {
//            val (nextState, nextMsg) = s.nextHandshakeMessage.apply(curState)
//            Debug.info(this + ": " + curState + " -> " + Pair(nextState, nextMsg))
//            curState = nextState
//            nextMsg
//          } else
//            throw new IllegalHandshakeStateException("getNextMessage undefined at " + curState)
//
//        def handleNextMessage(msg: Any) {
//          if (s.handleHandshakeMessage.isDefinedAt((curState, msg))) {
//            val nextState = s.handleHandshakeMessage.apply((curState, msg))
//            Debug.info(this + ": " + Pair(curState, msg) + " -> " + nextState)
//            curState = nextState
//          } else
//            throw new IllegalHandshakeStateException("handleNextMessage undefined at " + curState)
//        }
//
//        def sendMessage(msg: Any) {
//          Debug.info(this + ": sendMessage(msg = " + msg + ")")
//          s.writeJavaObject(dataout, msg.asInstanceOf[AnyRef])
//        }
//
//        def readMessage: AnyRef = {
//          Debug.info(this + ": readMessage waiting for message")
//          val msg = s.readJavaObject(datain).asInstanceOf[AnyRef]
//          Debug.info(this + ": readMessage read " + msg)
//          msg
//        }
//            
//        var continue = true
//
//        while (continue) {
//          getNextMessage match {
//            case Some(msg) => sendMessage(msg)
//            case None      => continue = false
//          }
//          if (continue) handleNextMessage(readMessage)
//        }
//
//        Debug.info(this + ": handshake finished")
//
//      case None =>
//        /** Do nothing; skip handshake */
//        Debug.info(this + ": no handshake to perform")
//    }
//  }
//
//  def transmit(data: Array[Byte]): Unit = synchronized {
//    if (running) {
//      Debug.info(this+": checking to see if handshake is finished")
//      if (handshakeStatus.get) {
//        Debug.info(this+": transmitting " + data.length + " encoded bytes...")
//        assert(data.length > 0) // should never send unencoded data at this point
//        dataout.write(data)
//        dataout.flush()
//      } else 
//        throw new IllegalStateException("Cannot transmit when handshake failed")
//    } else
//      throw new IllegalStateException("Cannot transmit when not running")
//  }
//
//  var running         = true
//  val handshakeStatus = new SyncVar[Boolean]
//
//  def halt = synchronized {
//    so.close()
//    running = false
//  }
//
//  override def run() {
//    try {
//      if (running) {
//        doHandshake
//        handshakeStatus.set(true)
//      }
//      while (running) {
//        val msg = parent.serializer.readObject(datain);
//        parent.kernel.processMsg(remoteNode, msg)
//      }
//    }
//    catch {
//      case ioe: IOException =>
//        Debug.info(this+": caught "+ioe)
//        parent nodeDown remoteNode
//      case e: Exception =>
//        Debug.info(this+": caught "+e)
//        parent nodeDown remoteNode
//    }
//    Debug.info(this+": service terminated at "+parent.node)
//  }
//
//  override def toString = "<TcpServiceWorker (id " + id + "): local = " + 
//    localNode + " -> remote = " + remoteNode + ">"
//}
