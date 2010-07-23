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

import java.net.{ InetAddress, InetSocketAddress }

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
 * @author Stephen Tu
 */
object RemoteActor {

  /**
   * Remote name used for the singleton Controller actor
   */
  private final val ControllerSymbol = Symbol("$$ControllerActor$$")

  /**
   * Maps each port to a set of actors which are listening on it.
   * Guarded by `portToActors` lock
   */
  private val portToActors = new HashMap[Int, HashSet[Actor]]

  /**
   * Maps each actor a the set of ports which it is listening on
   * (inverted index of `portToActors`). Guarded by `portToActors` lock
   */
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

  @volatile private var explicitlyTerminate = true
  def setExplicitTermination(isExplicit: Boolean) {
    explicitlyTerminate = isExplicit
  }

  def alive(port: Int, actor: Actor)(implicit cfg: Configuration[_]) {
    portToActors.synchronized {
      // listen if necessary
      val listener = listenOnPort(port, cfg.aliveMode)
      val realPort = listener.port

      // now register the actor
      val thisActor = actor 

      portToActors.get(realPort) match {
        case Some(set) => 
          set += thisActor
        case None =>
          val set = new HashSet[Actor]
          set += thisActor
          portToActors += realPort -> set 
      }

      actorToPorts.get(thisActor) match {
        case Some(set) =>
          set += realPort
        case None =>
          val set = new HashSet[Int]
          set += realPort
          actorToPorts += thisActor -> set
      }

      thisActor onTerminate { actorTerminated(thisActor) }
    }
  }

  /**
   * Makes <code>self</code> remotely accessible on TCP port
   * <code>port</code>.
   */
  @throws(classOf[InconsistentServiceException])
  def alive(port: Int)(implicit cfg: Configuration[_]) {
    alive(port, Actor.self)
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

  private def actorTerminated(actor: Actor) {
    Debug.info("actorTerminated(): alive actor " + actor + " terminated")
    unregister(actor)
      //// termination handler
      //thisActor onTerminate {
      //  Debug.info("alive actor " + thisActor + " terminated")
      //  // Unregister actor from kernel
      //  unregister(thisActor)

      //  /*  
      //   *  TODO: this causes deadlocks - move all connection/listener garbage
      //   *  collection to a separate collection task instead (which runs like
      //   *  every N minutes) 
      //  // we only worry about garbage collecting for un-used listeners. we
      //  // don't bother to garbage collect for un-used connections
      //  portToActors.synchronized { // prevents new connections
      //    // get all the ports this actor was listening on...
      //    val candidatePorts = actorToPorts.remove(thisActor) match {
      //      case Some(ports) => 
      //        ports
      //      case None => 
      //        Debug.error("Could not find " + thisActor + " in actorToPorts")
      //        new HashSet[Int]()
      //    }

      //    // ... now for each of these candidates, remove this actor
      //    // from the set and terminate if the set becomes empty
      //    candidatePorts.foreach(p => portToActors.get(p) match {
      //      case Some(actors) =>
      //        actors -= thisActor
      //        if (actors.isEmpty) {
      //          portToActors -= p
      //          tryShutdown(p) // try to shutdown in a different thread
      //                         // this is because blocking in a
      //                         // ForkJoinScheduler worker is bad (causes
      //                         // starvation)
      //        }
      //      case None => tryShutdown(p) 
      //    })

      //  }
      //  */

      //}
  }

  def register(name: Symbol, actor: Actor) {
    netKernel.register(name, actor)
    actor onTerminate { actorTerminated(actor) }
  }

  def unregister(actor: Actor) {
    netKernel.unregister(actor)
  }

  private def listenOnPort(port: Int, mode: ServiceMode.Value) =
    netKernel.listen(port, mode) 

  object controller extends ControllerActor(ControllerSymbol)

  def remoteStart[A <: Actor](node: Node)(implicit m: Manifest[A], cfg: Configuration[Proxy]) {
    remoteStart(node, m.erasure.asInstanceOf[Class[A]])
  }

  def remoteStart[A <: Actor](node: Node, actorClass: Class[A])(implicit cfg: Configuration[Proxy]) {
    val remoteController = select(node, ControllerSymbol)
    remoteController !? RemoteStartInvoke(actorClass.getName) match {
      case RemoteStartResult(None) => // success, do nothing
      case RemoteStartResult(Some(e)) => throw new RuntimeException(e)
      case _ => throw new RuntimeException("Failed")
    }
  }

  def remoteStartAndListen[A <: Actor, P <: Proxy](node: Node, port: Int, name: Symbol)(implicit m: Manifest[A], cfg: Configuration[P]): P =
    remoteStartAndListen(node, m.erasure.asInstanceOf[Class[A]], port, name)

  def remoteStartAndListen[A <: Actor, P <: Proxy](node: Node, actorClass: Class[A], port: Int, name: Symbol)(implicit cfg: Configuration[P]): P = {
    remoteStart(node, actorClass)
    select(Node(node.address, node.port), name)
  }

  def remoteSelf[P <: Proxy](implicit cfg: Configuration[P]): P = {
    val thisActor = Actor.self
    val serializer = cfg.newSerializer()

    // need to establish a port for this actor to listen on, so that the proxy
    // returned by remoteSelf makes sense (as in, you can connect to it)
    val port = portToActors.synchronized {

      // check actorToPorts first to see if this actor is already alive
      actorToPorts.get(thisActor).flatMap(_.headOption).getOrElse({

        // oops, this actor isn't alive anywhere. in this case, pick a random
        // port (by scanning portToActors)
        portToActors.headOption.map(_._1).getOrElse({

          // no ports are actually listening. so pick a random one now (by
          // listening with 0 as the port)
          val listener = listenOnPort(0, cfg.aliveMode)
          val realPort = listener.port

          // now call alive, so that thisActor gets mapped to the new port
          alive(realPort, thisActor)

          // return realPort
          realPort
        })
      })
    }

    val remoteNode        = serializer.newNode(Node.localhost, port)
    val remoteConnectMode = cfg.selectMode
    val clzName           = serializer.getClass.getName
    val remoteName        = netKernel.getOrCreateName(Actor.self) // auto-registers name if a new one is created
    serializer.newProxy(remoteNode, remoteConnectMode, clzName, remoteName)
  }

  private def connect(node: Node, serializer: Serializer[Proxy], mode: ServiceMode.Value): MessageConnection =
    netKernel.connect(node, serializer, mode)

  /**
   * Returns (a proxy for) the actor registered under
   * <code>name</code> on <code>node</code>.
   */
  @throws(classOf[InconsistentSerializerException])
  @throws(classOf[InconsistentServiceException])
  def select[P <: Proxy](node: Node, sym: Symbol)(implicit cfg: Configuration[P]): P =
    netKernel.getOrCreateProxy(connect(node, cfg.newSerializer(), cfg.selectMode), sym).asInstanceOf[P]

  def releaseResources() {
    controller ! Terminate
    netKernel.terminateTop()
  }

  def releaseResourcesInActor() {
    newThread { releaseResources() }
  }

}

object Node {
  val localhost = InetAddress.getLocalHost.getCanonicalHostName
  def apply(address: String, port: Int): Node = DefaultNodeImpl(address, port)
  def apply(port: Int): Node = apply(localhost, port)
  def unapply(n: Node): Option[(String, Int)] = Some((n.address, n.port))
}

trait Node {

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
