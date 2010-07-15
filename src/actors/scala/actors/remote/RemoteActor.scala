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

/**
 *  This object provides methods for creating, registering, and
 *  selecting remotely accessible actors.
 *
 *  A remote actor is typically created like this:
 *  {{{
 *  actor {
 *    alive(9010)
 *    register('myName, self)
 *
 *    // behavior
 *  }
 *  }}}
 *  It can be accessed by an actor running on a (possibly)
 *  different node by selecting it in the following way:
 *  {{{
 *  actor {
 *    // ...
 *    val c = select(Node("127.0.0.1", 9010), 'myName)
 *    c ! msg
 *    // ...
 *  }
 *  }}}
 *
 * @author Philipp Haller
 */
object RemoteActor {

  private val portToActors = new HashMap[Int, HashSet[Actor]]
  private val actorToPorts = new HashMap[Actor, HashSet[Int]]

  /**
   * Backing net kernel
   */
  private[remote] val netKernel = new NetKernel

  /* If set to <code>null</code> (default), the default class loader
   * of <code>java.io.ObjectInputStream</code> is used for deserializing
   * java objects sent as messages. Custom serializers are free to ignore this
   * field (especially since it probably doesn't apply).
   */
  private var cl: ClassLoader = null

  def classLoader: ClassLoader = cl
  def classLoader_=(x: ClassLoader) { cl = x }

  /**
   * Default serializer instance, using Java serialization to implement
   * serialization of objects. Not a val, so we can capture changes made
   * to the classloader instance
   */
  def defaultSerializer = new JavaSerializer(cl)



  /**
   * Makes <code>self</code> remotely accessible on TCP port
   * <code>port</code>.
   */
  @throws(classOf[InconsistentServiceException])
  def alive(port: Int, serviceMode: ServiceMode.Value = ServiceMode.Blocking): Unit = portToActors.synchronized {
    // listen if necessary
    listenOnPort(port, serviceMode)

    // now register the actor
    val thisActor = Actor.self

    portToActors.get(port) match {
      case Some(set) => 
        set += thisActor
      case None =>
        val set = new HashSet[Actor]
        set += thisActor
        portToActors += port -> set 
    }

    actorToPorts.get(thisActor) match {
      case Some(set) =>
        set += port
      case None =>
        val set = new HashSet[Int]
        set += port
        actorToPorts += thisActor -> set
    }

    // termination handler
    thisActor onTerminate {
      Debug.info("alive actor " + thisActor + " terminated")
      // Unregister actor from kernel
      unregister(thisActor)

      /*  
       *  TODO: this causes deadlocks - move all connection/listener garbage
       *  collection to a separate collection task instead (which runs like
       *  every N minutes) 
      // we only worry about garbage collecting for un-used listeners. we
      // don't bother to garbage collect for un-used connections
      portToActors.synchronized { // prevents new connections
        // get all the ports this actor was listening on...
        val candidatePorts = actorToPorts.remove(thisActor) match {
          case Some(ports) => 
            ports
          case None => 
            Debug.error("Could not find " + thisActor + " in actorToPorts")
            new HashSet[Int]()
        }

        // ... now for each of these candidates, remove this actor
        // from the set and terminate if the set becomes empty
        candidatePorts.foreach(p => portToActors.get(p) match {
          case Some(actors) =>
            actors -= thisActor
            if (actors.isEmpty) {
              portToActors -= p
              tryShutdown(p) // try to shutdown in a different thread
                             // this is because blocking in a
                             // ForkJoinScheduler worker is bad (causes
                             // starvation)
            }
          case None => tryShutdown(p) 
        })

      }
      */

    }
  }

  private def tryShutdown(port: Int) {
    newThread {
      portToActors.synchronized {
        portToActors.get(port) match {
          case Some(actors) =>
            // ignore request - this is a potential race condition to check
            // for, in the case where tryShutdown(p) is called, then a call to
            // alive(p) is made, then this thread executes. 
          case None =>
            netKernel.unlisten(port)
        }
      }
    }
  }

  private def newThread(f: => Unit) {
    val t = new Thread {
      override def run() = f
    }
    t.start()
  }

  def register(name: Symbol, actor: Actor) {
    netKernel.register(name, actor)
  }

  def unregister(actor: Actor) {
    netKernel.unregister(actor)
  }

  private def listenOnPort(port: Int, mode: ServiceMode.Value) { 
    netKernel.listen(port, mode) 
  }

  //def remoteStart[A <: Actor, S <: Serializer](node: Node, 
  //                                             serializer: Serializer,
  //                                             actorClass: Class[A], 
  //                                             port: Int, 
  //                                             name: Symbol,
  //                                             serviceFactory: Option[ServiceFactory],
  //                                             serializerClass: Option[Class[S]]) {
  //  remoteStart(node, serializer, actorClass.getName, port, name, serviceFactory, serializerClass.map(_.getName))
  //} 

  //def remoteStart(node: Node,
  //                serializer: Serializer,
  //                actorClass: String, 
  //                port: Int, 
  //                name: Symbol,
  //                serviceFactory: Option[ServiceFactory],
  //                serializerClass: Option[String]) {
  //  val remoteActor = select(node, 'remoteStartActor, serializer)
  //  remoteActor ! RemoteStart(actorClass, port, name, serviceFactory, serializerClass)
  //}

  private var remoteListenerStarted = false

  def startListeners(): Unit = synchronized {
    if (!remoteListenerStarted) {
      RemoteStartActor.start
      remoteListenerStarted = true
    }
  }

  def stopListeners(): Unit = synchronized {
    if (remoteListenerStarted) {
      RemoteStartActor.send(Terminate, null)
      remoteListenerStarted = false
    }
  }

  private def connect(node: Node, serializer: Serializer[Proxy], mode: ServiceMode.Value): MessageConnection =
    netKernel.connect(node, serializer, mode)

  /**
   * Returns (a proxy for) the actor registered under
   * <code>name</code> on <code>node</code>.
   */
  @throws(classOf[InconsistentSerializerException])
  @throws(classOf[InconsistentServiceException])
  def select[T <: Proxy](node: Node, sym: Symbol, 
             serializer: Serializer[T] = defaultSerializer,
             serviceMode: ServiceMode.Value = ServiceMode.Blocking): T =
    netKernel.getOrCreateProxy(connect(node, serializer, serviceMode), sym).asInstanceOf[T]

  def releaseResources() {
    netKernel.terminateTop()
  }

  def releaseResourcesInActor() {
    newThread { netKernel.terminateTop() }
  }

}

object Node {
  def apply(address: String, port: Int): Node = DefaultNodeImpl(address, port)
  def unapply(n: Node): Option[(String, Int)] = Some((n.address, n.port))
}

trait Node {
  import java.net.{ InetAddress, InetSocketAddress }

  def address: String
  def port: Int

  /**
   * Returns an InetSocketAddress representation of this Node
   */
  def toInetSocketAddress = new InetSocketAddress(address, port)

  /**
   * Returns the canonical representation of this form, resolving the
   * address into canonical form (as determined by the Java API)
   */
  def canonicalForm =
    newNode(InetAddress.getByName(address).getCanonicalHostName, port)

  protected def newNode(a: String, p: Int): Node

  def isCanonical = this == canonicalForm

  override def equals(o: Any) = o match {
    case n: Node =>
      n.address == this.address && n.port == this.port
    case _ => 
      false
  }

  override def hashCode = address.hashCode + port.hashCode

}

case class DefaultNodeImpl(override val address: String, override val port: Int) extends Node {
  override def newNode(a: String, p: Int) = DefaultNodeImpl(a, p)
}

case class InconsistentSerializerException(expected: Serializer[Proxy], actual: Serializer[Proxy]) 
  extends Exception("Inconsistent serializers: Expected " + expected + " but got " + actual)

case class InconsistentServiceException(expected: ServiceMode.Value, actual: ServiceMode.Value) 
  extends Exception("Inconsistent service modes: Expected " + expected + " but got " + actual)

case class NameAlreadyRegisteredException(sym: Symbol, a: OutputChannel[Any])
  extends Exception("Name " + sym + " is already registered for channel " + a)
