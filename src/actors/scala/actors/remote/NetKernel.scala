/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

import scala.collection.mutable.{ HashMap, HashSet }
import java.io.IOException
import java.util.concurrent.{ ConcurrentHashMap, CountDownLatch, TimeUnit }


case object Terminate

/**
 * @version 1.0.0
 * @author Philipp Haller
 * @author Stephen Tu
 */
private[remote] object NetKernel {

  @volatile private var _service = new StandardService

  private val connections = new ConcurrentHashMap[(Node, Long, ServiceMode.Value), MessageConnection]

  def getConnectionFor(node: Node, cfg: Configuration[Proxy]) = {
    val key = (node, cfg.cachedSerializer.uniqueId, cfg.selectMode)
    val testConn = connections.get(key)
    if (testConn ne null)
      testConn
    else {
      connections.synchronized {
        val testConn0 = connections.get(key)
        if (testConn0 ne null)
          testConn0
        else {
          val conn = _service.connect(node, cfg.newSerializer(), cfg.selectMode, processMsgFunc)
          conn beforeTerminate { isBottom =>
            Debug.info(this + ": removing key: " + key)
            connections.remove(key)
          }
          connections.put(key, conn)
          conn
        }
      }
    }
  }

  private val listeners = new ConcurrentHashMap[Int, Listener]

  def getListenerFor(port: Int, cfg: Configuration[Proxy]) = {
    def ensureListener(listener: Listener) = {
      if (listener.mode != cfg.aliveMode)
        throw InconsistentServiceException(cfg.aliveMode, listener.mode)
      listener
    }
    val testListener = listeners.get(port)
    if (testListener ne null)
      ensureListener(testListener)
    else {
      listeners.synchronized {
        val testListener0 = listeners.get(port)
        if (testListener0 ne null)
          ensureListener(testListener0)
        else {
          val listener = _service.listen(port, cfg.aliveMode, msgConnCallback, processMsgFunc)
          val realPort = listener.port
          listener beforeTerminate { isBottom =>
            listeners.remove(realPort)
          }
          listeners.put(realPort, listener)
          listener
        }
      }
    }
  }

  def unlisten(port: Int) {
    Debug.info(this + ": unlisten() - port " + port)
    val listener = listeners.remove(port)
    if (listener eq null)
      Debug.info(this + ": unlisten() - no listener on port " + port)
    else
      listener.terminateTop()
  }

  def releaseResources() {
    listeners.synchronized {
      connections.synchronized {
        import scala.collection.JavaConversions._
        connections.values.foreach(_.terminateTop())
        connections.clear()

        listeners.values.foreach(_.terminateTop())
        listeners.clear()

        _service.terminateTop()
        _service = new StandardService
      }
    }
  }

  def asyncSend(conn: MessageConnection, toName: Symbol, fromName: Option[Symbol], msg: AnyRef) {
    conn.send { serializer: Serializer[Proxy] =>
      val wireMsg  = serializer.intercept(msg)
      val metadata = serializer.serializeMetaData(wireMsg).orNull
      val bytes    = serializer.serialize(wireMsg)
      serializer.newAsyncSend(fromName, toName, metadata, bytes)
    }
  }

  def syncSend(conn: MessageConnection, toName: Symbol, fromName: Symbol, msg: AnyRef, session: Symbol) {
    conn.send { serializer: Serializer[Proxy] =>
      val wireMsg  = serializer.intercept(msg)
      val metadata = serializer.serializeMetaData(wireMsg).orNull
      val bytes    = serializer.serialize(wireMsg)
      serializer.newSyncSend(fromName, toName, metadata, bytes, session)
    }
  }

  def syncReply(conn: MessageConnection, toName: Symbol, msg: AnyRef, session: Symbol) {
    conn.send { serializer: Serializer[Proxy] =>
      val wireMsg  = serializer.intercept(msg)
      val metadata = serializer.serializeMetaData(wireMsg).orNull
      val bytes    = serializer.serialize(wireMsg)
      serializer.newSyncReply(toName, metadata, bytes, session)
    }
  }

  def remoteApply(conn: MessageConnection, toName: Symbol, fromName: Symbol, rfun: RemoteFunction) {
    conn.send { serializer: Serializer[Proxy] =>
      serializer.newRemoteApply(fromName, toName, rfun) 
    }
  }

  private val processMsgFunc = processMsg _

  private val msgConnCallback = (listener: Listener, msgConn: MessageConnection) => {
  }

  private final val NoSender = new Proxy {
    override def remoteNode = throw new RuntimeException("NoSender")
    override def name = throw new RuntimeException("NoSender")
    override def handleMessage(m: ProxyCommand) {
      m match {
        case SendTo(out, m) =>
          out.send(m, null)
        case _ =>
          throw new RuntimeException("NoSender cannot handle: " + m)
      }
    }
    override def send(msg: Any, sender: OutputChannel[Any]) {
      throw new RuntimeException("NoSender")
    }
    // TODO: linkTo, unlinkFrom, exit
    override def toString = "<NoSender>"
  }

  private def processMsg(conn: MessageConnection, serializer: Serializer[Proxy], msg: AnyRef) {
    def mkProxy(senderName: Symbol) = {
      val remoteNode  = serializer.newNode(conn.remoteNode.address, conn.remoteNode.port) 
      val senderProxy = serializer.newProxy(remoteNode, senderName)
      senderProxy.setConn(conn)
      senderProxy
    }
    def deserialize(meta: Array[Byte], data: Array[Byte]) = 
      serializer.deserialize(Option(meta), data)
    def removeOrphan(a: OutputChannel[Any]) {
      // orphaned actor sitting in hash maps
      Debug.info(this + ": found orphaned (terminated) actor: " + a)
      RemoteActor.unregister(a)
    }
    val (senderName, receiverName) = msg match {
      case AsyncSend(sender, receiver, _, _) => 
        (sender, receiver) 
      case SyncSend(sender, receiver, _, _, _) => 
        (Some(sender), receiver)
      case SyncReply(receiver, _, _, _) => 
        (None, receiver)
      case RemoteApply(sender, receiver, _) => 
        (Some(sender), receiver)
    }
    val a = RemoteActor.getActor(receiverName)
    if (a eq null)
      // message is lost
      Debug.info(this+": lost message: " + msg)
    else if (a.isInstanceOf[Actor] && 
             a.asInstanceOf[Actor].getState == Actor.State.Terminated)
      removeOrphan(a)
    else {
      val cmd = msg match {
        case AsyncSend(_, _, meta, data) => 
          SendTo(a, deserialize(meta, data)) 
        case SyncSend(_, _, meta, data, session) => 
          StartSession(a, deserialize(meta, data), session) 
        case SyncReply(_, meta, data, session) => 
          FinishSession(a, deserialize(meta, data), session)
        case RemoteApply(_, _, rfun) => 
          LocalApply0(rfun, a.asInstanceOf[AbstractActor])
      }
      senderName.map(n => mkProxy(n)).getOrElse(NoSender).handleMessage(cmd)
    }
  }

}
