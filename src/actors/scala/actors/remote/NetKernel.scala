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
private[remote] class NetKernel(val service: Service) {

  import java.io.IOException

  def sendToNode(node: Node, msg: AnyRef) = {
    Debug.info("--NetKernel.sendToNode--")
    Debug.info("node: " + node)
    Debug.info("msg: " + msg)
    try {
      val meta = service.serializer.serializeMetaData(msg) match {
        case Some(data) => data 
        case None       => Array[Byte]()
      }
      val bytes = service.serializer.serialize(msg)
      service.send(node, meta, bytes)
    } catch {
      case ioe: IOException =>
        Debug.error("sendToNode: message " + msg + " dropped because: " + ioe.getMessage)
    }
  }

  def namedSend(senderLoc: Locator, receiverLoc: Locator,
                msg: AnyRef, session: Symbol) {
    //Debug.info("--NetKernel.namedSend--")
    //Debug.info("senderLoc: " + senderLoc + ", receiverLoc: " + receiverLoc)
    //Debug.info("msg: " + msg)
    //Debug.info("session: " + session)

    val metadata = service.serializer.serializeMetaData(msg) match {
      case Some(data) => data
      case None       => null
    }
    val bytes = service.serializer.serialize(msg)
    sendToNode(receiverLoc.node, NamedSend(senderLoc, receiverLoc, metadata, bytes, session))
  }

  private val actors = new HashMap[Symbol, OutputChannel[Any]]
  private val names = new HashMap[OutputChannel[Any], Symbol]

  def register(name: Symbol, a: OutputChannel[Any]): Unit = synchronized {
    actors += Pair(name, a)
    names  += Pair(a, name)
  }

  def unregister(a: OutputChannel[Any]): Unit = synchronized {
    names -= a
    actors.retain((_,v) => v != a)
  }

  def getOrCreateName(from: OutputChannel[Any]) = names.get(from) match {
    case None =>
      val freshName = FreshNameCreator.newName("remotesender")
      register(freshName, from)
      freshName
    case Some(name) =>
      name
  }

  def send(node: Node, name: Symbol, msg: AnyRef): Unit =
    send(node, name, msg, 'nosession)

  def send(node: Node, name: Symbol, msg: AnyRef, session: Symbol) {
    val senderLoc   = Locator(service localNodeFor node, getOrCreateName(Actor.self))
    val receiverLoc = Locator(node, name)
    namedSend(senderLoc, receiverLoc, msg, session)
  }

  def forward(from: OutputChannel[Any], node: Node, name: Symbol, msg: AnyRef, session: Symbol) {
    val senderLoc   = Locator(service localNodeFor node, getOrCreateName(from))
    val receiverLoc = Locator(node, name)
    namedSend(senderLoc, receiverLoc, msg, session)
  }

  def remoteApply(node: Node, name: Symbol, from: OutputChannel[Any], rfun: Function2[AbstractActor, Proxy, Unit]) {
    val senderLoc   = Locator(service localNodeFor node, getOrCreateName(from))
    val receiverLoc = Locator(node, name)
    sendToNode(receiverLoc.node, RemoteApply0(senderLoc, receiverLoc, rfun))
  }

  def createProxy(node: Node, sym: Symbol): Proxy = {
    val p = new Proxy(node, sym, this)
    proxies += Pair((node, sym), p)
    p
  }

  val proxies = new HashMap[(Node, Symbol), Proxy]

  def getOrCreateProxy(senderNode: Node, senderName: Symbol): Proxy =
    proxies.synchronized {
      proxies.get((senderNode, senderName)) match {
        case Some(senderProxy) => senderProxy
        case None              => createProxy(senderNode, senderName)
      }
    }

  /* Register proxy if no other proxy has been registered.
   */
  def registerProxy(senderNode: Node, senderName: Symbol, p: Proxy): Unit =
    proxies.synchronized {
      proxies.get((senderNode, senderName)) match {
        case Some(senderProxy) => // do nothing
        case None              => proxies += Pair((senderNode, senderName), p)
      }
    }

  def processMsg(senderNode: Node, msg: AnyRef): Unit = synchronized {
    // Note: senderNode as an argument is ignored here. instead,
    // senderLoc.node is used
    msg match {
      case cmd@RemoteApply0(senderLoc, receiverLoc, rfun) =>
        Debug.info(this+": processing "+cmd)
        actors.get(receiverLoc.name) match {
          case Some(a) =>
            val senderProxy = getOrCreateProxy(senderLoc.node, senderLoc.name)
            senderProxy.send(LocalApply0(rfun, a.asInstanceOf[AbstractActor]), null)

          case None =>
            // message is lost
            Debug.info(this+": lost message")
        }

      case cmd@NamedSend(senderLoc, receiverLoc, metadata, data, session) =>
        Debug.info(this+": processing "+cmd)

        def sendToProxy(a: OutputChannel[Any]) {
          try {
            val msg = service.serializer.deserialize(if (metadata eq null) None else Some(metadata), data)
            val senderProxy = getOrCreateProxy(senderLoc.node, senderLoc.name)
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

  def terminate() {
    // TODO: can a new proxy be created while terminate is running?

    // find all running proxies delegates
    val runningProxies = proxies.values.filter(_.del.getState != Actor.State.Terminated)

    // future to wait on
    Debug.info(this + ": terminate(): waiting for (" + runningProxies.size + "/" + proxies.values.size + 
        ") to terminate")
    val countFuture = new CountFuture(runningProxies.size)

    // register proxy on termination handlers so we know when
    // all the proxies are actually finished terminating
    proxies.values.map(_.del).foreach(_.onTerminate {
      countFuture addOne 
    })

    // tell all proxies to terminate
    proxies.values foreach { _.send(Terminate, null) }

    // wait on the future until all proxies are terminated
    countFuture.await

    Debug.info("woke up from countFuture")

    // tell service to terminate
    service.terminate()
  }
}
