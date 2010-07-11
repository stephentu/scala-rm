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

  private val actorsByPort = new HashMap[Int, HashSet[Actor]]

  /**
   * Backing net kernel
   */
  private val netKernel = new NetKernel

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
  def defaultSerializer: Serializer = new JavaSerializer(cl)



  /**
   * Makes <code>self</code> remotely accessible on TCP port
   * <code>port</code>.
   */
  @throws(classOf[InconsistentServiceException])
  def alive(port: Int, serviceMode: ServiceMode.Value = ServiceMode.Blocking): Unit = actorsByPort.synchronized {
    // listen if necessary
    listenOnPort(port, serviceMode)

    // now register the actor
    val thisActor = Actor.self
    val registrations = actorsByPort.get(port) match {
      case Some(set) => 
        set += thisActor
      case None =>
        val set = new HashSet[Actor]
        set += thisActor
        actorsByPort += port -> set 
    }

    // termination handler
    thisActor onTerminate {
      Debug.info("alive actor " + thisActor + " terminated")
      // Unregister actor from kernel
      netKernel.unregister(thisActor)

      actorsByPort.synchronized {
        val portsToTerminate = actorsByPort flatMap { case (port, set) => 
          set -= thisActor
          if (set.isEmpty) List()
          else List(port)
        }
        portsToTerminate.map(p => (p, netKernel.listenerOn(p))).foreach {
          case (_, Some(listener)) => listener.terminateTop()
          case (p, None) => actorsByPort -= p
        }
      }
    }
  }

  def register(name: Symbol, actor: Actor) {
    netKernel.register(name, actor)
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

  private def connect(node: Node, serializer: Serializer, mode: ServiceMode.Value): MessageConnection =
    netKernel.connect(node, serializer, mode)

  /**
   * Returns (a proxy for) the actor registered under
   * <code>name</code> on <code>node</code>.
   */
  @throws(classOf[InconsistentSerializerException])
  @throws(classOf[InconsistentServiceException])
  def select(node: Node, sym: Symbol, 
             serializer: Serializer = defaultSerializer,
             serviceMode: ServiceMode.Value = ServiceMode.Blocking): AbstractActor = synchronized {
    netKernel.getOrCreateProxy(connect(node, serializer, serviceMode), sym)
  }

}


/**
 * This class represents a machine node on a TCP network.
 *
 * @param address the host name, or <code>null</code> for the loopback address.
 * @param port    the port number.
 *
 * @author Philipp Haller
 */
case class Node(address: String, port: Int) {
  import java.net.{ InetAddress, InetSocketAddress }

  /**
   * Returns an InetSocketAddress representation of this Node
   */
  def toInetSocketAddress = new InetSocketAddress(address, port)

  /**
   * Returns the canonical representation of this form, resolving the
   * address into canonical form (as determined by the Java API)
   */
  def canonicalForm: Node = {
    val a = InetAddress.getByName(address)
    Node(a.getCanonicalHostName, port)
  }
  def isCanonical         = this == canonicalForm
}

case class InconsistentSerializerException(expected: Serializer, actual: Serializer) 
  extends Exception("Inconsistent serializers: Expected " + expected + " but got " + actual)

case class InconsistentServiceException(expected: ServiceMode.Value, actual: ServiceMode.Value) 
  extends Exception("Inconsistent service modes: Expected " + expected + " but got " + actual)

case class NameAlreadyRegisteredException(sym: Symbol, a: OutputChannel[Any])
  extends Exception("Name " + sym + " is already registered for channel " + a)
