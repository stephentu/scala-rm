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
import java.util.concurrent.CountDownLatch

case class NamedSend(senderLoc: Locator, 
                     receiverLoc: Locator, 
                     metaData: Array[Byte], 
                     data: Array[Byte], 
                     session: Symbol)

case class RemoteApply0(senderLoc: Locator, receiverLoc: Locator, rfun: Function2[AbstractActor, Proxy, Unit])
case class LocalApply0(rfun: Function2[AbstractActor, Proxy, Unit], a: AbstractActor)

case class  SendTo(a: OutputChannel[Any], msg: Any, session: Symbol)
case object Terminate

case class Locator(node: Node, name: Symbol)

/**
 * @version 0.9.17
 * @author Philipp Haller
 */
private[remote] class NetKernel extends CanTerminate {

  private val service = new StandardService

  import java.io.IOException

  def namedSend(conn: MessageConnection, senderLoc: Locator, receiverLoc: Locator, msg: AnyRef, session: Symbol) {
    conn.send { serializer: Serializer =>
      val metadata = serializer.serializeMetaData(msg) match {
        case Some(data) => data
        case None       => null
      }
      val bytes = serializer.serialize(msg)
      NamedSend(senderLoc, receiverLoc, metadata, bytes, session)
    }
  }

  private val actors = new HashMap[Symbol, OutputChannel[Any]]
  private val names = new HashMap[OutputChannel[Any], Symbol]

  @throws(classOf[NameAlreadyRegisteredException])
  def register(name: Symbol, a: OutputChannel[Any]): Unit = actors.synchronized {
    actors.get(name) match {
      case Some(actor) =>
        if (a != actor)
          // trying to clobber the name of another actor
          throw NameAlreadyRegisteredException(name, actor)
        Debug.warning("registering " + name + " to channel " + a)
      case None        =>
        actors += Pair(name, a)
        names  += Pair(a, name)
    }
  }

  /**
   * Errors silently if a mapping did not previously exist
   */
  def unregister(a: OutputChannel[Any]): Unit = actors.synchronized {
    names -= a
    actors.retain((_,v) => v != a)
  }

  def getOrCreateName(from: OutputChannel[Any]) = actors.synchronized { 
    names.get(from) match {
      case None =>
        val freshName = FreshNameCreator.newName("remotesender")
        Debug.info("Made freshName for output channel: " + from)
        register(freshName, from)
        freshName
      case Some(name) =>
        name
    }
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
    val p = conn.activeSerializer.newProxy(conn.remoteNode, conn.mode, conn.activeSerializer.getClass.getName, sym)
    val delegate = p.newDelegate(conn)
    delegate.start()
    p.del = delegate
    proxies += Pair((conn, sym), p)
    p
  }

  private val proxies = new HashMap[(MessageConnection, Symbol), Proxy]

  private val processMsgFunc = processMsg _

  private val connectionCache = new HashMap[(Node, Serializer, ServiceMode.Value), MessageConnection]

  // TODO: guard connect with terminateLock
  def connect(node: Node, serializer: Serializer, serviceMode: ServiceMode.Value): MessageConnection =
    connect0(node.canonicalForm, serializer, serviceMode)

  private def connect0(node: Node, serializer: Serializer, serviceMode: ServiceMode.Value): MessageConnection = connectionCache.synchronized {
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
    val serializer = Class.forName(proxy.serializerClassName).newInstance().asInstanceOf[Serializer]
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

  private def processMsg(conn: MessageConnection, serializer: Serializer, msg: AnyRef): Unit = synchronized {
    msg match {
      case cmd@RemoteApply0(senderLoc, receiverLoc, rfun) =>
        Debug.info(this+": processing "+cmd)
        actors.synchronized {
          actors.get(receiverLoc.name) match {
            case Some(a) =>
              val senderProxy = getOrCreateProxy(conn, senderLoc.name)
              senderProxy.send(LocalApply0(rfun, a.asInstanceOf[AbstractActor]), null)

            case None =>
              // message is lost
              Debug.info(this+": lost message")
          }
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

        actors.synchronized {
          actors.get(receiverLoc.name) match {
            case Some(a) =>
              if (a.isInstanceOf[Actor] && 
                  a.asInstanceOf[Actor].getState == Actor.State.Terminated) removeOrphan(a)
              else                                                          sendToProxy(a)
            case None =>
              // message is lost
              Debug.info(this+": lost message")
          }
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
    actors.synchronized {
      actors.clear()
      names.clear()
    }

    Debug.info(this + ": now kill service threads")
    // terminate the service
    service.terminateTop()
  }
}
