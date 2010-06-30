/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

import scala.collection.mutable.HashMap

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

  /**
   * Maps actors to their backing NetKernels. An actor can be backed by at
   * most one netkernel (meaning that multiple calls to alive(port) within a
   * single actor with different ports will throw an exception)
   */
  private val kernels = new HashMap[Actor, NetKernel]

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

  sealed abstract class ServiceFactory extends Function2[Int, Serializer, Service] {
    def createsService(service: Service): Boolean = service.getClass == serviceClass
    def serviceClass: Class[_]
  }

  /**
   * Uses default blocking IO
   */
  final object TcpServiceFactory extends ServiceFactory {
    def apply(port: Int, serializer: Serializer): Service = TcpService(port, serializer)
    override def serviceClass = classOf[TcpService]
    override def toString = "<TcpServiceFactory>"
  }

  /**
   * Uses Java NIO
   */
  final object NioServiceFactory extends ServiceFactory {
    def apply(port: Int, serializer: Serializer): Service = NioService(port, serializer)
    override def serviceClass = classOf[NioService]
    override def toString = "<NioServiceFactory>"
  }

  case class ActorAlreadyAliveException(port: Int) 
    extends Exception("Actor is already alive on port " + port)

  case class InconsistentSerializerException(expected: Serializer, actual: Serializer) 
    extends Exception("Inconsistent serializers: Expected " + expected + " but got " + actual)

  case class InconsistentServiceException(expected: String, actual: String) 
    extends Exception("Inconsistent service classes: Expected " + expected + " but got " + actual)

  /**
   * Makes <code>self</code> remotely accessible on TCP port
   * <code>port</code>.
   */
  @throws(classOf[ActorAlreadyAliveException])
  @throws(classOf[InconsistentSerializerException])
  @throws(classOf[InconsistentServiceException])
  def alive(port: Int, 
            serializer: Serializer = defaultSerializer, 
            serviceFactory: ServiceFactory = TcpServiceFactory): Unit = synchronized {
    kernels.get(Actor.self) match {
      case Some(kern) =>
        val MyPort       = port
        val (portMismatch, kernPort) = kern.service.node match {
          case Node(_, MyPort)  => (false, MyPort)
          case Node(_, unknown) => (true, unknown)
        }
        if (portMismatch)
          // are we running on the same port?
          throw ActorAlreadyAliveException(kernPort)
        ensureKernelConsistent(kern, serializer, serviceFactory) 
        Debug.warning(this + ": alive already called on port " + port)
      case None       =>
        createNetKernelOnPort(Actor.self, port, serializer, serviceFactory)
    }
  }

  private def ensureKernelConsistent(kern: NetKernel, serializer: Serializer, serviceFactory: ServiceFactory) {
    if (kern.service.serializer != serializer)
      // are we using the same serializer?
      throw InconsistentSerializerException(serializer, kern.service.serializer)
    if (!(serviceFactory createsService kern.service))
      // are we using the same service layer?
      throw InconsistentServiceException(serviceFactory.serviceClass.getName, kern.service.getClass.getName)
  }

  // assumes lock for this is already held
  private[remote] def createNetKernelOnPort(actor: Actor, 
                                            port: Int, 
                                            serializer: Serializer, 
                                            serviceFactory: ServiceFactory): NetKernel = {
    Debug.info("createNetKernelOnPort: creating net kernel for actor " + actor + 
        " on port " + port + " using service factory " + serviceFactory)
    val serv = serviceFactory(port, serializer)
    val kern = serv.kernel
    addNetKernel(actor, kern)
    kern
  }

  // does NOT invoke kernel.register()
  private def addNetKernel(actor: Actor, kern: NetKernel) {
    kernels += Pair(actor, kern) // assumes no such mapping previously exists

    actor.onTerminate {
      Debug.info("alive actor "+actor+" terminated")
      // remove mapping for `actor`
      kernels -= actor
      // Unregister actor from kernel
      kern.unregister(actor)
      // terminate `kern` when it does
      // not appear as value any more
      if (!kernels.valuesIterator.contains(kern)) {
        Debug.info("terminating "+kern)
        // terminate NetKernel
        kern.terminate()
      }
    }

  }

  case class ActorNotAliveException(a: Actor) 
    extends Exception("Actor " + a + " is not currently alive (listening on port)")

  case class NameAlreadyRegisteredException(sym: Symbol, a: OutputChannel[Any])
    extends Exception("Name " + sym + " is already registered for channel " + a)

  /**
   * Registers <code>a</code> under <code>name</code> on this
   * node. Is an error if the actor is not currently alive, or if
   * the name being registered is already being used to identify
   * another actor on the same port.
   */
  @throws(classOf[ActorNotAliveException])
  @throws(classOf[NameAlreadyRegisteredException])
  def register(name: Symbol, a: Actor): Unit = synchronized {
    val kernel = kernelFor(a).getOrElse(throw ActorNotAliveException(a)) 
    kernel.register(name, a)
  }

  private def selfKernel = kernelFor(Actor.self)
  private def kernelFor(a: Actor) = kernels.get(a)

  def remoteStart[A <: Actor, S <: Serializer](node: Node, 
                                               serializer: Serializer,
                                               actorClass: Class[A], 
                                               port: Int, 
                                               name: Symbol,
                                               serviceFactory: Option[ServiceFactory],
                                               serializerClass: Option[Class[S]]) {
    remoteStart(node, serializer, actorClass.getName, port, name, serviceFactory, serializerClass.map(_.getName))
  } 

  def remoteStart(node: Node,
                  serializer: Serializer,
                  actorClass: String, 
                  port: Int, 
                  name: Symbol,
                  serviceFactory: Option[ServiceFactory],
                  serializerClass: Option[String]) {
    val remoteActor = select(node, 'remoteStartActor, serializer)
    remoteActor ! RemoteStart(actorClass, port, name, serviceFactory, serializerClass)
  }

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

  /**
   * Returns (a proxy for) the actor registered under
   * <code>name</code> on <code>node</code>.
   */
  @throws(classOf[InconsistentSerializerException])
  @throws(classOf[InconsistentServiceException])
  def select(node: Node, 
             sym: Symbol, 
             serializer: Serializer = defaultSerializer,
             serviceFactory: ServiceFactory = TcpServiceFactory): AbstractActor = synchronized {
    val thisKern = selfKernel match {
      case Some(kern) => 
        // a NetKernel was found, but we need to validate if its consistent
        // with serializer + serviceFactory
        ensureKernelConsistent(kern, serializer, serviceFactory)
        kern
      case None       =>
        // if there's no selfKernel yet, assume the user doesn't care about
        // which port this actor will listen on. Therefore, search all kernels
        // first to see if we can find a suitable kernel (matching serializer
        // + serviceFactory), before picking a random port
        val matches = kernels.valuesIterator.filter(kern => {
          (kern.service.serializer == serializer) && 
          (serviceFactory createsService kern.service)
        }).toList
        if (matches.isEmpty) {
          // no candidates found, so use random port
          createNetKernelOnPort(Actor.self, TcpService.generatePort, serializer, serviceFactory)
        } else {
          // valid candidate found
          val kern = matches.head
          // update mapping 
          addNetKernel(Actor.self, kern)
          kern
        }
    }
    thisKern.getOrCreateProxy(node.canonicalForm, sym)
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
