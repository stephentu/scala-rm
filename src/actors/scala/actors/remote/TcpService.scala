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

import scala.collection.mutable.HashMap

/* Object TcpService.
 *
 * @version 0.9.9
 * @author Philipp Haller
 */
object TcpService extends ServiceCreator {
  type MyService = TcpService
  def newService(port: Int, serializer: Serializer) = new TcpService(port, serializer)
  var BufSize: Int = 65536
}

/* Class TcpService.
 *
 * @version 0.9.10
 * @author Philipp Haller
 */
class TcpService(port: Int, val serializer: Serializer) extends Thread with Service {

  private val internalNode = new Node(InetAddress.getLocalHost.getHostAddress, port)
  def node: Node = internalNode

  private val pendingSends = new HashMap[Node, List[Array[Byte]]]

  /**
   * Sends a byte array to another node on the network.
   * If the node is not yet up, up to <code>TcpService.BufSize</code>
   * messages are buffered.
   */
  def rawSend(node: Node, data: Array[Byte]): Unit = synchronized {

    def bufferMsg(t: Throwable) {
      // buffer message, so that it can be re-sent
      // when remote net kernel comes up
      (pendingSends.get(node): @unchecked) match {
        case None =>
          pendingSends += Pair(node, List(data))
        case Some(msgs) if msgs.length < TcpService.BufSize =>
          pendingSends += Pair(node, data :: msgs)
      }
    }

    // retrieve worker thread (if any) that already has connection
    getConnection(node) match {
      case None =>
        // we are not connected, yet
        try {
          val newWorker = connect(node)
          newWorker transmit data

          // TODO: i don't think this is right- you should try to send your
          // pending sends first, before sending the current message to 
          // preserve order

          // any pending sends?
          pendingSends.get(node) match {
            case None =>
              // do nothing
            case Some(msgs) =>
              msgs foreach {newWorker transmit _}
              pendingSends -= node
          }
        } catch {
          case uhe: UnknownHostException =>
            bufferMsg(uhe)
          case ioe: IOException =>
            bufferMsg(ioe)
          case se: SecurityException =>
            // do nothing
        }
      case Some(worker) =>
        worker transmit data
    }
  }

  def terminate() {
    shouldTerminate = true
    try {
      new Socket(internalNode.address, internalNode.port)
    } catch {
      case ce: java.net.ConnectException =>
        Debug.info(this+": caught "+ce)
    }
  }

  private var shouldTerminate = false

  override def run() {
    try {
      val socket = new ServerSocket(port)
      while (!shouldTerminate) {
        Debug.info(this+": waiting for new connection on port "+port+"...")
        val nextClient = socket.accept()
        if (!shouldTerminate) {
          val worker = new TcpServiceWorker(this, nextClient)
          Debug.info("Started new "+worker)
          addConnection(worker.remoteNode, worker) // add mapping for reply channel
          worker.doHandshake
          worker.start()
        } else
          nextClient.close()
      }
    } catch {
      case e: Exception =>
        Debug.info(this+": caught "+e)
        e.printStackTrace
    } finally {
      Debug.info(this+": shutting down...")
      connections foreach { case (_, worker) => worker.halt }
    }
  }

  // connection management

  private val connections = new HashMap[Node, TcpServiceWorker]

  private[actors] def addConnection(node: Node, worker: TcpServiceWorker) = synchronized {
    connections += Pair(node, worker)
  }

  def getConnection(n: Node) = synchronized {
    connections.get(n)
  }

  def isConnected(n: Node): Boolean = synchronized {
    !connections.get(n).isEmpty
  }

  def connect(n: Node): TcpServiceWorker = synchronized {
    Debug.info(this + ": connect(n = " + n + ")")
    val socket = new Socket(n.address, n.port)
    val worker = new TcpServiceWorker(this, socket)
    worker.doHandshake
    worker.start()
    addConnection(n, worker) // re-entrant lock so this is OK
    worker
  }

  def localNodeFor(n: Node): Node = synchronized {
    connections.get(n) match {
      case Some(worker) => worker.localNode
      case None         => connect(n).localNode // re-entrant lock so this is OK
    }
  }

  def disconnectNode(n: Node) = synchronized {
    connections.get(n) match {
      case None =>
        // do nothing
      case Some(worker) =>
        connections -= n
        worker.halt
    }
  }

  def isReachable(node: Node): Boolean =
    if (isConnected(node)) true
    else try {
      connect(node)
      return true
    } catch {
      case uhe: UnknownHostException => false
      case ioe: IOException => false
      case se: SecurityException => false
    }

  def nodeDown(mnode: Node): Unit = synchronized {
    connections -= mnode
  }
}


private[actors] class TcpServiceWorker(parent: TcpService, so: Socket) extends Thread {
  val datain  = new DataInputStream(so.getInputStream)
  val dataout = new DataOutputStream(so.getOutputStream)

  val remoteNode = Node(so.getInetAddress.getHostName,  so.getPort) 
  val localNode  = Node(so.getLocalAddress.getHostName, so.getLocalPort)

  def doHandshake {
    val s = parent.serializer
    s.initialState match {
      case Some(initialState) =>

        var curState = initialState
        Debug.info(this + ": initialState = " + curState)

        def getNextMessage =
          if (s.nextHandshakeMessage.isDefinedAt(curState)) {
            val (nextState, nextMsg) = s.nextHandshakeMessage.apply(curState)
            Debug.info(this + ": " + curState + " -> " + Pair(nextState, nextMsg))
            curState = nextState
            nextMsg
          } else
            throw new IllegalHandshakeStateException

        def handleNextMessage(msg: Any) {
          if (s.handleHandshakeMessage.isDefinedAt((curState, msg))) {
            val nextState = s.handleHandshakeMessage.apply((curState, msg))
            Debug.info(this + ": " + Pair(curState, msg) + " -> " + nextState)
            curState = nextState
          } else
            throw new IllegalHandshakeStateException
        }

        def sendMessage(msg: Any) {
          Debug.info(this + ": sendMessage(msg = " + msg + ")")
          s.writeJavaObject(dataout, msg.asInstanceOf[AnyRef])
        }

        def readMessage: AnyRef = {
          Debug.info(this + ": readMessage waiting for message")
          val msg = s.readJavaObject(datain).asInstanceOf[AnyRef]
          Debug.info(this + ": readMessage read " + msg)
          msg
        }
            
        var continue = true

        while (continue) {
          getNextMessage match {
            case Some(msg) => sendMessage(msg)
            case None      => continue = false
          }
          if (continue) handleNextMessage(readMessage)
        }

        Debug.info(this + ": handshake finished")

      case None =>
        /** Do nothing; skip handshake */
        Debug.info(this + ": no handshake to perform")
    }
  }

  def transmit(data: Array[Byte]): Unit = synchronized {
    Debug.info(this+": transmitting " + data.length + " encoded bytes...")
    assert(data.length > 0) // should never send unencoded data at this point
    //data is already encoded
    //dataout.writeInt(data.length)
    dataout.write(data)
    dataout.flush()
  }

  var running = true

  def halt = synchronized {
    so.close()
    running = false
  }

  override def run() {
    try {
      while (running) {
        val msg = parent.serializer.readObject(datain);
        parent.kernel.processMsg(remoteNode, msg)
      }
    }
    catch {
      case ioe: IOException =>
        Debug.info(this+": caught "+ioe)
        parent nodeDown remoteNode
      case e: Exception =>
        Debug.info(this+": caught "+e)
        parent nodeDown remoteNode
    }
    Debug.info(this+": service terminated at "+parent.node)
  }

  override def toString = "<TcpServiceWorker: local = " + localNode + " -> remote = " + remoteNode + ">"
}
