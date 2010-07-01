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

/* Object TcpService.
 *
 * @version 0.9.9
 * @author Philipp Haller
 */
object TcpService extends ServiceCreator {
  type MyService = TcpService
  def newService(port: Int, serializer: Serializer) = new TcpService(port, serializer)
  private var _bufSize = 65536
  def BufSize: Int = _bufSize
  def BufSize_=(bufSize: Int) {
    if (bufSize < 0)
      throw new IllegalArgumentException("BufSize must be non-negative")
    _bufSize = bufSize
  }
  private val workerId = new AtomicInteger
}

/* Class TcpService.
 *
 * @version 0.9.10
 * @author Philipp Haller
 */
class TcpService(port: Int, val serializer: Serializer) 
extends Thread("TcpService-" + port) with Service {
  import TcpService.workerId

  private val internalNode = Node(InetAddress.getLocalHost.getHostAddress, port)
  def node: Node           = internalNode

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
    connections.get(node) match {
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
    Debug.info(this + ": terminate()")
    shouldTerminate = true
    synchronized {
      if (serverSocket ne null) {
        Debug.info(this + ": terminate() - closing ServerSocket " + serverSocket)
        serverSocket.close
      }
    }
  }

  private var shouldTerminate = false
  private var serverSocket: ServerSocket = _

  override def run() {
    try {
      synchronized {
        serverSocket = new ServerSocket(port)
      }
      while (!shouldTerminate) {
        Debug.info(this+": waiting for new connection on port "+port+"...")
        val nextClient = serverSocket.accept()
        if (!shouldTerminate) {
          val worker = new TcpServiceWorker(workerId.getAndIncrement, this, nextClient)
          Debug.info("Started new "+worker)
          addConnection(worker.remoteNode, worker) // add mapping for reply channel
          worker.start()
        } else
          nextClient.close()
      }
    } catch {
      case e: Exception =>
        Debug.info(this+": caught "+e)
        Debug.doInfo { e.printStackTrace }
    } finally {
      Debug.info(this+": shutting down...")
      connections foreach { case (_, worker) => worker.halt }
    }
  }

  // connection management

  // only stores canonical nodes
  private val connections = new HashMap[Node, TcpServiceWorker]

  // assumes node is canonical
  private[remote] def addConnection(node: Node, worker: TcpServiceWorker) = synchronized {
    connections += Pair(node, worker)
  }

  def connect(node: Node) = synchronized {
    val n = node.canonicalForm 
    connections.get(n).getOrElse(connect0(n))
  }

  // assumes n is in canonical form
  private def connect0(n: Node): TcpServiceWorker = synchronized {
    if (!shouldTerminate) {
      Debug.info(this + ": connect0(n = " + n + ")")
      val socket = new Socket(n.address, n.port)
      val worker = new TcpServiceWorker(workerId.getAndIncrement, this, socket)
      addConnection(n, worker) // re-entrant lock so this is OK
      worker.start()
      worker
    } else 
      throw new IllegalStateException("Cannot connect to " + n + " when should be terminated")
  }

  def localNodeFor(node: Node): Node = connect(node).localNode 

  // assumes mnode is canonical
  private[remote] def nodeDown(mnode: Node): Unit = synchronized {
    connections -= mnode
  }
}


private[actors] class TcpServiceWorker(id: Int, parent: TcpService, so: Socket) 
extends Thread("TcpServiceWorker-" + id) {

  import scala.concurrent.SyncVar

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
            throw new IllegalHandshakeStateException("getNextMessage undefined at " + curState)

        def handleNextMessage(msg: Any) {
          if (s.handleHandshakeMessage.isDefinedAt((curState, msg))) {
            val nextState = s.handleHandshakeMessage.apply((curState, msg))
            Debug.info(this + ": " + Pair(curState, msg) + " -> " + nextState)
            curState = nextState
          } else
            throw new IllegalHandshakeStateException("handleNextMessage undefined at " + curState)
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
    if (running) {
      Debug.info(this+": checking to see if handshake is finished")
      if (handshakeStatus.get) {
        Debug.info(this+": transmitting " + data.length + " encoded bytes...")
        assert(data.length > 0) // should never send unencoded data at this point
        dataout.write(data)
        dataout.flush()
      } else 
        throw new IllegalStateException("Cannot transmit when handshake failed")
    } else
      throw new IllegalStateException("Cannot transmit when not running")
  }

  var running         = true
  val handshakeStatus = new SyncVar[Boolean]

  def halt = synchronized {
    so.close()
    running = false
  }

  override def run() {
    try {
      if (running) {
        doHandshake
        handshakeStatus.set(true)
      }
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

  override def toString = "<TcpServiceWorker (id " + id + "): local = " + 
    localNode + " -> remote = " + remoteNode + ">"
}
