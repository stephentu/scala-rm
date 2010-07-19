/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

import scala.collection.mutable.{HashMap, HashSet}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch

object NamedSend {
  def apply(senderLoc: Locator, receiverLoc: Locator, metaData: Array[Byte], data: Array[Byte], session: Symbol): NamedSend =
    DefaultNamedSendImpl(senderLoc, receiverLoc, metaData, data, session)
  def unapply(n: NamedSend): Option[(Locator, Locator, Array[Byte], Array[Byte], Symbol)] =
    Some((n.senderLoc, n.receiverLoc, n.metaData, n.data, n.session))
}

trait NamedSend {
  def senderLoc: Locator
  def receiverLoc: Locator
  def metaData: Array[Byte]
  def data: Array[Byte]
  def session: Symbol
}

case class DefaultNamedSendImpl(override val senderLoc: Locator, 
                                override val receiverLoc: Locator, 
                                override val metaData: Array[Byte], 
                                override val data: Array[Byte], 
                                override val session: Symbol) extends NamedSend

case class RemoteApply0(senderLoc: Locator, receiverLoc: Locator, rfun: Function2[AbstractActor, Proxy, Unit])
case class LocalApply0(rfun: Function2[AbstractActor, Proxy, Unit], a: AbstractActor)

case class  SendTo(a: OutputChannel[Any], msg: Any, session: Symbol)
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

/**
 * @version 0.9.17
 * @author Philipp Haller
 */
private[remote] class NetKernel extends CanTerminate {

  private val service = new StandardService

  import java.io.IOException

  def namedSend(conn: MessageConnection, senderLoc: Locator, receiverLoc: Locator, msg: AnyRef, session: Symbol) {
    conn.send { serializer: Serializer[Proxy] =>
      val metadata = serializer.serializeMetaData(msg).getOrElse(null)
      val bytes = serializer.serialize(msg)
      // deep copy everything into serializer form
      val _senderLocNode = serializer.newNode(senderLoc.node.address, senderLoc.node.port)
      val _receiverLocNode = serializer.newNode(receiverLoc.node.address, receiverLoc.node.port)
      val _senderLoc = serializer.newLocator(_senderLocNode, senderLoc.name)
      val _receiverLoc = serializer.newLocator(_receiverLocNode, receiverLoc.name)
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
    }
    a.channelName = Some(name)
  }

  def unregister(a: OutputChannel[Any]) {
    a.channelName.foreach(name => actors.remove(name)) 
    a.channelName = None
  }

  def getOrCreateName(from: OutputChannel[Any]) = from.channelName match {
    case Some(name) => name
    case None => 
      Debug.info(this + ": creating new name for output channel " + from)
      val newName = FreshNameCreator.newName("remotesender")
      register(newName, from)
      newName
  }

  def send(conn: MessageConnection, name: Symbol, msg: AnyRef): Unit =
    send(conn, name, msg, 'nosession)

  def send(conn: MessageConnection, name: Symbol, msg: AnyRef, session: Symbol) {
    val senderLoc   = Locator(conn.localNode, getOrCreateName(Actor.self))
    val receiverLoc = Locator(conn.remoteNode, name)
    namedSend(conn, senderLoc, receiverLoc, msg, session)
  }

  def forward(from: OutputChannel[Any], conn: MessageConnection, name: Symbol, msg: AnyRef, session: Symbol) {
    val senderLoc   = Locator(conn.localNode, getOrCreateName(from))
    val receiverLoc = Locator(conn.remoteNode, name)
    namedSend(conn, senderLoc, receiverLoc, msg, session)
  }

  def remoteApply(conn: MessageConnection, name: Symbol, from: OutputChannel[Any], rfun: Function2[AbstractActor, Proxy, Unit]) {
    val senderLoc   = Locator(conn.localNode, getOrCreateName(from))
    val receiverLoc = Locator(conn.remoteNode, name)
    conn.send(RemoteApply0(senderLoc, receiverLoc, rfun))
  }

  private def createProxy(conn: MessageConnection, sym: Symbol): Proxy = {
    // invariant: createProxy() is always called with a connection that has
    // already created an activeSerializer (not necessarily that the handshake
    // is complete though)
    val serializer = conn.activeSerializer.asInstanceOf[Serializer[Proxy]]
    val _remoteNode = serializer.newNode(conn.remoteNode.address, conn.remoteNode.port)
    val p = serializer.newProxy(_remoteNode, conn.mode, serializer.getClass.getName, sym)
    val delegate = p.newDelegate(conn)
    delegate.start()
    p.del = delegate
    proxies += Pair((conn, sym), p)
    p
  }

  private val proxies = new HashMap[(MessageConnection, Symbol), Proxy]

  private val processMsgFunc = processMsg _

  private val connectionCache = new HashMap[(Node, Serializer[Proxy], ServiceMode.Value), MessageConnection]

  // TODO: guard connect with terminateLock
  def connect(node: Node, serializer: Serializer[Proxy], serviceMode: ServiceMode.Value): MessageConnection =
    connect0(node.canonicalForm, serializer, serviceMode)

  private def connect0(node: Node, serializer: Serializer[Proxy], serviceMode: ServiceMode.Value): MessageConnection = connectionCache.synchronized {
    connectionCache.get((node, serializer, serviceMode)) match {
      case Some(conn) => conn
      case None =>
        val conn = service.connect(node, serializer, serviceMode, processMsgFunc)
        conn beforeTerminate {
          connectionCache.synchronized {
            connectionCache -= ((node, serializer, serviceMode))
          }
        }
        connectionCache += ((node, serializer, serviceMode)) -> conn
        conn
    }
  }

  private val listeners = new HashMap[(Int, ServiceMode.Value), Listener]
  private val listenersByPort = new HashMap[Int, Listener]

  // TODO: guard listen with terminateLock
  def listen(port: Int, serviceMode: ServiceMode.Value): Listener = listeners.synchronized {
    listeners.get((port, serviceMode)) match {
      case Some(listener) => listener
      case None =>
        // check the port
        if (listenersByPort.contains(port))
          throw InconsistentServiceException(serviceMode, listenersByPort(port).mode)

        val listener = service.listen(port, serviceMode, processMsgFunc)
        listener beforeTerminate {
          listeners.synchronized {
            listeners -= ((port, serviceMode))
            listenersByPort -= port
          }
        }
        listeners += ((port, serviceMode)) -> listener
        listenersByPort += port -> listener
        listener
    }
  }

  def unlisten(port: Int) {
    Debug.info(this + ": unlisten() - port " + port)
    listeners.synchronized {
      listenersByPort.get(port) match {
        case Some(listener) =>
          val waitFor = proxies.synchronized {
            proxies.values.filter { p => 
              p.del.conn.localNode.port == port && p.del.getState != Actor.State.Terminated 
            } toSeq
          }
          waitForProxies(waitFor)
          listener.terminateTop()
        case None =>
          Debug.info(this + ": unlisten() - port " + port + " is not being listened on")
      }
    }
  }

  def getOrCreateProxy(conn: MessageConnection, senderName: Symbol): Proxy =
    proxies.synchronized {
      proxies.get((conn, senderName)) match {
        case Some(senderProxy) => senderProxy
        case None              => createProxy(conn, senderName)
      }
    }

  // TODO: guard with terminateLock
  private[remote] def setupProxy(proxy: Proxy) {
    // TODO: catch exceptions here
    val serializer = Class.forName(proxy.serializerClassName).newInstance().asInstanceOf[Serializer[Proxy]]
    val messageConn = connect(proxy.remoteNode, serializer, proxy.mode)
    proxies.synchronized {
      proxies.get((messageConn, proxy.name)) match {
        case Some(curProxy) =>
          // this machine has seen this proxy before, so point the delegate
          // accordingly
          proxy.del = curProxy.del
        case None =>
          // this machine hasn't seen this proxy yet, so make this the actual
          // proxy
          val delegate = proxy.newDelegate(messageConn)
          delegate.start()
          proxy.del = delegate
          proxies += ((messageConn, proxy.name)) -> proxy
      }
    }
  }

  private def processMsg(conn: MessageConnection, serializer: Serializer[Proxy], msg: AnyRef): Unit = synchronized {
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
          if (a.isInstanceOf[Actor] && a.asInstanceOf[Actor].getState == Actor.State.Terminated) 
            removeOrphan(a)
          else 
            sendToProxy(a)
        }

    }
  }

  private val waitProxyLock = new Object

  private def waitForProxies(proxies: Seq[Proxy]) {
    waitProxyLock.synchronized {
      Debug.info(this + ": waitForProxies - waiting for " + proxies)
      val delegates = proxies.filter(_.del.getState != Actor.State.Terminated).map({ _.del }).distinct // > 1 proxy can map to the same delegate
      val len = delegates.length
      val latch = new CountDownLatch(len)
      // register termination handlers on all delegates 
      delegates.foreach(_.onTerminate { latch.countDown() })
      // tell all delegate to terminate
      delegates.foreach { _.send(Terminate, null) }
      Debug.info(this + ": waiting on latch")
      // wait on latch
      latch.await()
      Debug.info(this + ": woke up from latch")
    }
  }

  override def doTerminateImpl(isBottom: Boolean) {
    Debug.info(this + ": doTerminateImpl()")

    Debug.info(this + ": kill all the remaining alive delegates first")
    // wait for all proxies to finish
    val waitFor = proxies.synchronized { 
      proxies.values.filter({ _.del.getState != Actor.State.Terminated }).toSeq
    }
    waitForProxies(waitFor)

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
