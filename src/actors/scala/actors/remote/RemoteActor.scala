/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote


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

  private val kernels = new scala.collection.mutable.HashMap[Actor, NetKernel]

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

  sealed abstract class ServiceFactory extends Function2[Int, Serializer, Service]

  final object TcpServiceFactory extends ServiceFactory {
    def apply(port: Int, serializer: Serializer): Service = TcpService(port, serializer)
  }

  final object NioServiceFactory extends ServiceFactory {
    def apply(port: Int, serializer: Serializer): Service = NioService(port, serializer)
  }

  /**
   * Makes <code>self</code> remotely accessible on TCP port
   * <code>port</code>.
   */
  def alive(port: Int, 
            serializer: Serializer = defaultSerializer, 
            serviceFactory: ServiceFactory = TcpServiceFactory): Unit = synchronized {
    createNetKernelOnPort(Actor.self, port, serializer, serviceFactory)
  }

  private[remote] def createNetKernelOnPort(actor: Actor, 
                                            port: Int, 
                                            serializer: Serializer, 
                                            serviceFactory: ServiceFactory): NetKernel = {
    Debug.info("createNetKernelOnPort: creating net kernel for actor " + actor + " on port " + port)
    val serv = serviceFactory(port, serializer)
    val kern = serv.kernel
    kernels += Pair(actor, kern)

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

    kern
  }

  @deprecated("this member is going to be removed in a future release")
  def createKernelOnPort(port: Int): NetKernel =
    createNetKernelOnPort(Actor.self, port, defaultSerializer, TcpServiceFactory)

  /**
   * Registers <code>a</code> under <code>name</code> on this
   * node.
   */
  def register(name: Symbol, 
               a: Actor, 
               serializer: Serializer = defaultSerializer,
               serviceFactory: ServiceFactory = TcpServiceFactory): Unit = synchronized {
    val kernel = kernelFor(a, serializer, serviceFactory)
    kernel.register(name, a)
  }

  private def selfKernel(serializer: Serializer, serviceFactory: ServiceFactory) = 
    kernelFor(Actor.self, serializer, serviceFactory)

  private def kernelFor(a: Actor, serializer: Serializer, serviceFactory: ServiceFactory) = kernels.get(a) match {
    case None =>
      // establish remotely accessible
      // return path (sender)
      createNetKernelOnPort(a, TcpService.generatePort, serializer, serviceFactory)
    case Some(k) =>
      // serializer + factory arguments are ignored here in the case where we already have
      // a NetKernel instance
      k
  }

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
      RemoteStartActor ! Terminate
      remoteListenerStarted = false
    }
  }

  /**
   * Returns (a proxy for) the actor registered under
   * <code>name</code> on <code>node</code>.
   */
  def select(node: Node, 
             sym: Symbol, 
             serializer: Serializer = defaultSerializer,
             serviceFactory: ServiceFactory = TcpServiceFactory): AbstractActor = synchronized {
    selfKernel(serializer, serviceFactory).getOrCreateProxy(node.canonicalForm, sym)
  }

  private[remote] def someNetKernel: NetKernel =
    kernels.valuesIterator.next

  @deprecated("this member is going to be removed in a future release")
  def someKernel: NetKernel =
    someNetKernel
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
  def toInetSocketAddress = new InetSocketAddress(address, port)
  def canonicalForm: Node = {
    val a = InetAddress.getByName(address)
    Node(a.getCanonicalHostName, port)
  }
  def isCanonical         = this == canonicalForm
}
