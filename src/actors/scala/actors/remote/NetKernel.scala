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

import java.util.concurrent.{ ConcurrentHashMap, CountDownLatch, TimeUnit }

object NamedSend {
  def apply(senderLoc: Locator, receiverLoc: Locator, metaData: Array[Byte], data: Array[Byte], session: Option[Symbol]): NamedSend =
    DefaultNamedSendImpl(senderLoc, receiverLoc, metaData, data, session)
  def unapply(n: NamedSend): Option[(Locator, Locator, Array[Byte], Array[Byte], Option[Symbol])] =
    Some((n.senderLoc, n.receiverLoc, n.metaData, n.data, n.session))
}

trait NamedSend {
  def senderLoc: Locator
  def receiverLoc: Locator
  def metaData: Array[Byte]
  def data: Array[Byte]
  def session: Option[Symbol]
}

case class DefaultNamedSendImpl(override val senderLoc: Locator, 
                                override val receiverLoc: Locator, 
                                override val metaData: Array[Byte], 
                                override val data: Array[Byte], 
                                override val session: Option[Symbol]) extends NamedSend

case class RemoteApply0(senderLoc: Locator, receiverLoc: Locator, rfun: Function2[AbstractActor, Proxy, Unit])
case class LocalApply0(rfun: Function2[AbstractActor, Proxy, Unit], a: AbstractActor)

case class  SendTo(a: OutputChannel[Any], msg: Any, session: Option[Symbol])
case object Terminate

object Locator {
  def apply(node: Node, name: Symbol): Locator = DefaultLocatorImpl(node, name)
  def unapply(l: Locator): Option[(Node, Symbol)] = Some((l.node, l.name))
}

trait Locator {
  def node: Node
  def name: Symbol
  override def equals(o: Any) = o match {
    case l: Locator =>
      l.node == this.node && l.name == this.name
    case _ => false
  }
  override def hashCode = node.hashCode + name.hashCode
}

case class DefaultLocatorImpl(override val node: Node, override val name: Symbol) extends Locator

case class MsgConnAttachment(val del: DelegateActor, val proxies: ConcurrentHashMap[Symbol, Proxy])

/**
 * @version 0.9.17
 * @author Philipp Haller
 */
private[remote] class NetKernel extends CanTerminate {

  private val service = new StandardService

  import java.io.IOException

  def namedSend(conn: MessageConnection, senderLoc: Locator, receiverLoc: Locator, msg: AnyRef, session: Option[Symbol]) {
    conn.send { serializer: Serializer[Proxy] =>
      val wireMsg  = serializer.intercept(msg)
      val metadata = serializer.serializeMetaData(wireMsg).getOrElse(null)
      val bytes    = serializer.serialize(wireMsg)

      // deep copy everything into serializer form
      val _senderLocNode   = serializer.newNode(senderLoc.node.address, senderLoc.node.port)
      val _receiverLocNode = serializer.newNode(receiverLoc.node.address, receiverLoc.node.port)
      val _senderLoc       = serializer.newLocator(_senderLocNode, senderLoc.name)
      val _receiverLoc     = serializer.newLocator(_receiverLocNode, receiverLoc.name)
      serializer.newNamedSend(_senderLoc, _receiverLoc, metadata, bytes, session)
    }
  }

  private val actors = new ConcurrentHashMap[Symbol, OutputChannel[Any]]

  @throws(classOf[NameAlreadyRegisteredException])
  def register(name: Symbol, a: OutputChannel[Any]) {
    val existing = actors.putIfAbsent(name, a)
    if (existing ne null) {
      if (existing != a)
        throw NameAlreadyRegisteredException(name, existing)
      Debug.warning("re-registering " + name + " to channel " + a)
    } else Debug.info(this + ": successfully mapped " + name + " to " + a)
    a.channelName match {
      case Some(prevName) => 
        if (prevName != name)
          throw new IllegalStateException("Channel already registered as: " + prevName)
      case None =>
        a.channelName = Some(name)
    }
  }

  def unregister(a: OutputChannel[Any]) {
    a.channelName.foreach(name => {
      actors.remove(name)
      Debug.info(this + ": successfully removed name " + name + " for " + a + " from map")
    })
  }

  def getOrCreateName(from: OutputChannel[Any]) = from.channelName match {
    case Some(name) => name
    case None => 
      val newName = FreshNameCreator.newName("remotesender")
      Debug.info(this + ": creating new name for output channel " + from + " as " + newName)
      register(newName, from)
      newName
  }

  def forward(from: OutputChannel[Any], conn: MessageConnection, name: Symbol, msg: AnyRef, session: Option[Symbol]) {
    val fromName = 
      if (from eq null) Symbol("$$NoSender$$") // TODO: this is a hack for now
      else              getOrCreateName(from)
    val senderLoc   = Locator(conn.localNode,  fromName)
    val receiverLoc = Locator(conn.remoteNode, name)
    namedSend(conn, senderLoc, receiverLoc, msg, session)
  }

  def remoteApply(conn: MessageConnection, name: Symbol, from: OutputChannel[Any], rfun: Function2[AbstractActor, Proxy, Unit]) {
    val senderLoc   = Locator(conn.localNode, getOrCreateName(from))
    val receiverLoc = Locator(conn.remoteNode, name)
    conn.send(RemoteApply0(senderLoc, receiverLoc, rfun))
  }

  /**
   * Does NOT assign delegate
   */
  private def createProxy(conn: MessageConnection, sym: Symbol): Proxy = {
    // invariant: createProxy() is always called with a connection that has
    // already created an activeSerializer (not necessarily that the handshake
    // is complete though)
    val serializer  = conn.activeSerializer.asInstanceOf[Serializer[Proxy]]
    val _remoteNode = serializer.newNode(conn.remoteNode.address, conn.remoteNode.port)
    serializer.newProxy(_remoteNode, conn.mode, serializer.getClass.getName, sym)
  }

  private val processMsgFunc = processMsg _

  private val connectionCache = new HashMap[(Node, Serializer[Proxy], ServiceMode.Value), MessageConnection]

  // TODO: guard connect with terminateLock
  def connect(node: Node, serializer: Serializer[Proxy], serviceMode: ServiceMode.Value): MessageConnection =
    connect0(node.canonicalForm, serializer, serviceMode)

  private def newDelegate(conn: MessageConnection) = {
    val del = new DelegateActor(conn, this)
    del.start()
    del
  }

  private def getDelegate(conn: MessageConnection) =
    conn.attachment_!.asInstanceOf[MsgConnAttachment].del

  private def getProxies(conn: MessageConnection) =
    conn.attachment_!.asInstanceOf[MsgConnAttachment].proxies

  private def connect0(node: Node, serializer: Serializer[Proxy], serviceMode: ServiceMode.Value): MessageConnection = connectionCache.synchronized {
    connectionCache.get((node, serializer, serviceMode)) match {
      case Some(conn) => conn
      case None =>
        val conn = service.connect(node, serializer, serviceMode, processMsgFunc)

        conn.attach(MsgConnAttachment(newDelegate(conn), new ConcurrentHashMap[Symbol, Proxy]))

        conn beforeTerminate { isBottom =>
          // don't let threads on this Node use this connection anymore
          connectionCache.synchronized {
            connectionCache -= ((node, serializer, serviceMode))
          }

          // wait until the connection's delegate is terminated
          terminateDelegateFor(conn)
        }

        connectionCache += ((node, serializer, serviceMode)) -> conn
        conn
    }
  }

  /**
   * Waits up til 1 minute for the connection's delegate to terminate
   */
  private def terminateDelegateFor(conn: MessageConnection) {
    // terminate the delegate actor for this connection
    val del = getDelegate(conn)
    if (del.getState != Actor.State.Terminated) {
      Debug.info(this + ": waiting for delegate to terminate")
      val latch = new CountDownLatch(1)
      del onTerminate { latch.countDown() }
      del.send(Terminate, null)
      if (!latch.await(60, TimeUnit.SECONDS))
        Debug.error(this + ": Delegate did not terminate within given time")
    } else 
      Debug.info(this + ": ERROR: delegate was already terminated!")
  }

  private val listeners = new HashMap[(Int, ServiceMode.Value), Listener]
  private val listenersByPort = new HashMap[Int, Listener]

  private val msgConnCallback = (listener: Listener, msgConn: MessageConnection) => {
    // NOTE: we don't bother to cache the connections which come from
    // listeners, since we cannot connect to these anyways
    msgConn.attach(MsgConnAttachment(newDelegate(msgConn), new ConcurrentHashMap[Symbol, Proxy]))
    msgConn beforeTerminate { isBottom => terminateDelegateFor(msgConn) }
  }

  // TODO: guard listen with terminateLock
  def listen(port: Int, serviceMode: ServiceMode.Value): Listener = listeners.synchronized {
    listeners.get((port, serviceMode)) match {
      case Some(listener) => listener
      case None =>
        // check the port
        if (listenersByPort.contains(port))
          throw InconsistentServiceException(serviceMode, listenersByPort(port).mode)

        val listener = service.listen(port, serviceMode, msgConnCallback, processMsgFunc)
				val realPort = listener.port
        listener beforeTerminate { isBottom =>
          listeners.synchronized {
            listeners -= ((realPort, serviceMode))
            listenersByPort -= realPort
          }
        }
        listeners += ((realPort, serviceMode)) -> listener
        listenersByPort += realPort -> listener
        listener
    }
  }

  def unlisten(port: Int) {
    Debug.info(this + ": unlisten() - port " + port)
    listeners.synchronized {
      listenersByPort.get(port) match {
        case Some(listener) => listener.terminateTop()
        case None =>
          Debug.info(this + ": unlisten() - port " + port + " is not being listened on")
      }
    }
  }

  def getOrCreateProxy(conn: MessageConnection, senderName: Symbol): Proxy = {
    val proxies   = getProxies(conn) 
    val testProxy = proxies.get(senderName)
    if (testProxy ne null) testProxy
    else {
      val newProxy  = createProxy(conn, senderName)
      newProxy.del  = getDelegate(conn)
      val newProxy0 = proxies.putIfAbsent(senderName, newProxy)
      if (newProxy0 ne null) newProxy0 // newProxy0 came first, so use that one
      else {
        Debug.info(this + ": created new proxy: " + newProxy)
        newProxy
      }
    }
  }

  // TODO: guard with terminateLock
  private[remote] def delegateFor(proxy: Proxy) = {

    // TODO: catch exceptions here
    val serializer  = Class.forName(proxy.serializerClassName).newInstance().asInstanceOf[Serializer[Proxy]]

    // grab connection (possibly making a new connection)
    val messageConn = connect(proxy.remoteNode, serializer, proxy.mode)

    // NOTE: yes this means that we allow duplicate proxies, if a proxy object
    // is instantiated explicitly
    getDelegate(messageConn)
  }

  private def processMsg(conn: MessageConnection, serializer: Serializer[Proxy], msg: AnyRef) {
    msg match {
      case cmd@RemoteApply0(senderLoc, receiverLoc, rfun) =>
        Debug.info(this+": processing "+cmd)
        val a = actors.get(receiverLoc.name)
        if (a eq null) {
          // message is lost
          Debug.info(this+": lost message")
        } else {
          val senderProxy = getOrCreateProxy(conn, senderLoc.name)
          senderProxy.send(LocalApply0(rfun, a.asInstanceOf[AbstractActor]), null)
        }
      case cmd@NamedSend(senderLoc, receiverLoc, metadata, data, session) =>
        Debug.info(this+": processing "+cmd)

        def sendToProxy(a: OutputChannel[Any]) {
          try {
            val msg = serializer.deserialize(if (metadata eq null) None else Some(metadata), data)
            val senderProxy = getOrCreateProxy(conn, senderLoc.name)
            senderProxy.send(SendTo(a, msg, session), null)
          } catch {
            case e: Exception =>
              Debug.error(this+": caught "+e)
              e.printStackTrace
          }
        }

        def removeOrphan(a: OutputChannel[Any]) {
          // orphaned actor sitting in hash maps
          Debug.info(this + ": found orphaned (terminated) actor: " + a)
          unregister(a)
        }

        val a = actors.get(receiverLoc.name)
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

  override def doTerminateImpl(isBottom: Boolean) {
    Debug.info(this + ": doTerminateImpl()")

    Debug.info(this + ": try to gracefully shut down all the remaining connections")
    // terminate all connections
    connectionCache.synchronized {
      connectionCache.valuesIterator.foreach { _.terminateTop() }
      connectionCache.clear()
    }

    Debug.info(this + ": try to gracefully shut down remaining listeners")
    // terminate all listeners
    listeners.synchronized {
      listeners.valuesIterator.foreach { _.terminateTop() }
      listeners.clear()
    }
    
    Debug.info(this + ": unregister all actors")
    // clear all name mappings
    actors.clear()

    Debug.info(this + ": now kill service threads")
    // terminate the service
    service.terminateTop()
  }
}
