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

case class LocalApply0(rfun: Function2[AbstractActor, Proxy, Unit], a: AbstractActor)

case class  SendTo(a: OutputChannel[Any], msg: Any, session: Option[Symbol])
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

  def namedSend(conn: MessageConnection, toName: Symbol, fromName: Symbol, session: Option[Symbol], msg: AnyRef) {
    conn.send { serializer: Serializer[Proxy] =>
      val wireMsg  = serializer.intercept(msg)
      val metadata = serializer.serializeMetaData(wireMsg).orNull
      val bytes    = serializer.serialize(wireMsg)

      val messageCreator   = serializer 
      val _senderLocNode   = messageCreator.newNode(conn.localNode.address, conn.localNode.port)
      val _receiverLocNode = messageCreator.newNode(conn.remoteNode.address, conn.remoteNode.port)
      val _senderLoc       = messageCreator.newLocator(_senderLocNode, fromName)
      val _receiverLoc     = messageCreator.newLocator(_receiverLocNode, toName)
      messageCreator.newNamedSend(_senderLoc, _receiverLoc, metadata, bytes, session)
    }
  }

  def forward(conn: MessageConnection, toName: Symbol, from: OutputChannel[Any], session: Option[Symbol], msg: AnyRef) {
    val fromName = 
      if (from eq null) Symbol("$$NoSender$$") // TODO: this is a hack for now
      else              RemoteActor.getOrCreateName(from)
    namedSend(conn, toName, fromName, session, msg)
  }

  def remoteApply(conn: MessageConnection, toName: Symbol, from: OutputChannel[Any], rfun: RemoteFunction) {
    conn.send { serializer: Serializer[Proxy] =>
      val messageCreator   = serializer
      val _senderLocNode   = messageCreator.newNode(conn.localNode.address, conn.localNode.port)
      val _receiverLocNode = messageCreator.newNode(conn.remoteNode.address, conn.remoteNode.port)
      val _senderLoc       = messageCreator.newLocator(_senderLocNode, RemoteActor.getOrCreateName(from))
      val _receiverLoc     = messageCreator.newLocator(_receiverLocNode, toName)
      messageCreator.newRemoteApply(_senderLoc, _receiverLoc, rfun) 
    }
  }

  private val processMsgFunc = processMsg _

  private val msgConnCallback = (listener: Listener, msgConn: MessageConnection) => {
  }

  private def processMsg(conn: MessageConnection, serializer: Serializer[Proxy], msg: AnyRef) {
    msg match {
      case cmd @ RemoteApply(senderLoc, receiverLoc, rfun) =>
        Debug.info(this+": processing "+cmd)
        val a = RemoteActor.getActor(receiverLoc.name)
        if (a eq null) {
          // message is lost
          Debug.info(this+": lost message")
        } else {
          val msgCreator  = serializer 
          val remoteNode  = msgCreator.newNode(conn.remoteNode.address, conn.remoteNode.port) 
          val senderProxy = msgCreator.newProxy(remoteNode, senderLoc.name)
          senderProxy.setConn(conn)
          senderProxy.handleMessage(LocalApply0(rfun, a.asInstanceOf[AbstractActor]))
        }
      case cmd @ NamedSend(senderLoc, receiverLoc, metadata, data, session) =>
        Debug.info(this+": processing "+cmd)

        def sendToProxy(a: OutputChannel[Any]) {
          try {
            val msg = serializer.deserialize(if (metadata eq null) None else Some(metadata), data)
            val msgCreator  = serializer 
            val remoteNode  = msgCreator.newNode(conn.remoteNode.address, conn.remoteNode.port) 
            val senderProxy = msgCreator.newProxy(remoteNode, senderLoc.name)
            senderProxy.setConn(conn)
            senderProxy.handleMessage(SendTo(a, msg, session))
          } catch {
            case e: Exception =>
              Debug.error(this+": caught "+e)
              e.printStackTrace
          }
        }

        def removeOrphan(a: OutputChannel[Any]) {
          // orphaned actor sitting in hash maps
          Debug.info(this + ": found orphaned (terminated) actor: " + a)
          RemoteActor.unregister(a)
        }

        val a = RemoteActor.getActor(receiverLoc.name)
        if (a eq null) {
          // message is lost
          Debug.info(this+": lost message")
        } else {
          if (a.isInstanceOf[Actor] && 
              a.asInstanceOf[Actor].getState == Actor.State.Terminated) 
            removeOrphan(a)
          else 
            sendToProxy(a)
        }

    }
  }

}
