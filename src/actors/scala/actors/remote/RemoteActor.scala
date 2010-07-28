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
 * @author Stephen Tu
 */
object RemoteActor {

  /**
   * Remote name used for the singleton Controller actor
   */
  private final val ControllerSymbol = Symbol("$$ControllerActor$$")

  /**
   * Contains the set of all actors which have either called alive() or select(), 
   * or some combination of them. This is used so when
   * explicit shutdown is not desired, we have a way to keep track of the
   * actors which are still using the remote actor service. 
   */
  private val remoteActors = new HashSet[Actor]

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

  @volatile private var _ctrl: Actor = _

  /* If set to <code>null</code> (default), the default class loader
   * of <code>java.io.ObjectInputStream</code> is used for deserializing
   * java objects sent as messages. Custom serializers are free to ignore this
   * field (especially since it probably doesn't apply).
   */
  private var cl: ClassLoader = null

  def classLoader: ClassLoader = cl
  def classLoader_=(x: ClassLoader) { cl = x }

  @volatile private var explicitlyTerminate = false

  /**
   * Used to indicate whether explicit termination of the network kernel
   * resources is desired. The default value is <code>false</code>, meaning
   * that the network kernel will automatically shutdown when all actors which
   * have expressed intent to use remote functionality (via <code>alive</code>
   * and <code>select</code>) have terminated. In most simple usages, this is
   * the desirable behavior.
   *
   * However, some may want to reuse network kernel resources. For example,
   * consider the following usage pattern:
   * {{{
   * actor {
   *   alive(9000)
   *   register('anActor, self)
   *   // do some stuff
   * }
   * // do more actions, which take longer than the duration of 'anActor
   * actor {
   *   val p = select(Node("foo.com", 9000), 'fooActor)
   *   // do some stuff
   * }
   * }}}
   *
   * Without explicit termination set to <code>true</code>, after
   * <code>'anActor</code> terminates, the resources will be freed, and then
   * reallocated when the second actor executes. Since freeing resources is
   * not cheap, this is probably not desirable in some cases. Thus by setting
   * <code>setExplicitTermination</code> to <code>true</code>, the above
   * usage pattern would reuse the same resources.
   */
  def setExplicitTermination(isExplicit: Boolean) {
    explicitlyTerminate = isExplicit
  }

  @volatile private var explicitlyUnlisten = false

  def setExplicitUnlisten(isExplicit: Boolean) {
    explicitlyUnlisten = isExplicit
  }

  @volatile private var _configuration = DefaultConfiguration
  def setConfiguration(c: Configuration[Proxy]) {
    _configuration = c
  }
  private[remote] def configuration = _configuration

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

  private[remote] def getOrCreateName(from: OutputChannel[Any]) = from.channelName match {
    case Some(name) => name
    case None => 
      val newName = FreshNameCreator.newName("remotesender")
      Debug.info(this + ": creating new name for output channel " + from + " as " + newName)
      register(newName, from)
      newName
  }

  private val channels = new ConcurrentHashMap[Symbol, OutputChannel[Any]]
  private val sessions = new ConcurrentHashMap[OutputChannel[Any], Symbol]

  def newChannel(ch: OutputChannel[Any]) = {
    val fresh = FreshNameCreator.newSessionId()
    channels.put(fresh, ch)
    fresh
  }
  def findChannel(name: Symbol) = Option(channels.get(name))

  def finishChannel(name: Symbol) {
    channels.remove(name)
  }

  def finishSession(ch: OutputChannel[Any]) = Option(session.remove(ch))

  def startSession(ch: OutputChannel[Any], name: Symbol) {
    sessions.put(ch, name)
  }

  private def watchActor(actor: Actor) {
    // set termination handler on actor
    actor onTerminate { actorTerminated(actor) }
  }

  private[remote] def alive0(port: Int, actor: Actor, addToSet: Boolean)(implicit cfg: Configuration[_]) {
    // listen if necessary
    val listener = listenOnPort(port, cfg.aliveMode)

    // actual port chosen (is different from port iff port == 0)
    val realPort = listener.port

    // register actor to the actual port
    portToActors.synchronized {
      portToActors.get(realPort) match {
        case Some(set) => 
          set += actor
        case None =>
          val set = new HashSet[Actor]
          set += actor
          portToActors += realPort -> set 
      }

      actorToPorts.get(actor) match {
        case Some(set) =>
          set += realPort
        case None =>
          val set = new HashSet[Int]
          set += realPort
          actorToPorts += actor -> set
      }
    }

    if (addToSet)
      remoteActors.synchronized { remoteActors += actor }
    watchActor(actor)
  }

  /**
   * Makes <code>actor</code> remotely accessible on TCP port
   * <code>port</code>.
   */
  def alive(port: Int, actor: Actor)(implicit cfg: Configuration[_]) {
    alive0(port, actor, true)
  }

  /**
   * Makes <code>self</code> remotely accessible on TCP port
   * <code>port</code>.
   */
  @throws(classOf[InconsistentServiceException])
  def alive(port: Int)(implicit cfg: Configuration[_]) {
    alive0(port, Actor.self, true)
  }

  /**
   * Tries to unlisten on <code>port</code> if there are no registered actors
   * listening on it.
   */
  private def tryShutdown(ports: Iterable[Int]) {
    portToActors.synchronized {
      ports.foreach(port => {
        portToActors.get(port) match {
          case Some(_) =>
            // ignore request - this is a potential race condition to check
            // for, in the case where tryShutdown(p) is called, then a call to
            // alive(p) is made, then this thread executes. 
          case None =>
            netKernel_?.foreach(_.unlisten(port))
        }
      })
    }
  }

  private def newThread(f: => Unit) {
    val t = new Thread { override def run() = f }
    t.start()
  }

  private final val EmptyIntSet = new HashSet[Int]

  private def actorTerminated(actor: Actor) {
    Debug.info("actorTerminated(): alive actor " + actor + " terminated")
    unregister(actor)

    val portsToShutdown = portToActors.synchronized {
      // get all the ports this actor was listening on...
      val candidatePorts = actorToPorts.remove(actor).getOrElse(EmptyIntSet)

      // ... now for each of these candidates, remove this actor
      // from the set and terminate if the set becomes empty
      candidatePorts.filter(p => portToActors.get(p) match {
        case Some(actors) =>
          actors -= actor
          if (actors.isEmpty) {
            portToActors -= p
            true
          } else false
        case None => true 
      })
    }
    Debug.info("actorTerminated(" + actor + "): ports to shutdown: " + portsToShutdown)

    val terminate = remoteActors.synchronized { 
      remoteActors -= actor
      Debug.info("actorTerminated(" + actor + "): remoteActors.size (after actor removal): " + remoteActors.size)
      Debug.info(remoteActors.toString)
      remoteActors.isEmpty && !explicitlyTerminate
    }

    if (terminate) 
      // don't bother shutting down ports if we need to terminate, because
      // that will happen implicitly
      releaseResourcesInActor() // termination handler runs in actor threadpool
    else if (!explicitlyUnlisten && !portsToShutdown.isEmpty) 
      newThread { tryShutdown(portsToShutdown) }
  }

  def register(name: Symbol, actor: Actor) {
    netKernel.register(name, actor)
  }

  def unregister(actor: Actor) {
    netKernel_?.foreach(_.unregister(actor))
  }

  private def listenOnPort(port: Int, mode: ServiceMode.Value) =
    netKernel.listen(port, mode) 

  def remoteStart[A <: Actor](host: String)(implicit m: Manifest[A], cfg: Configuration[Proxy]) {
    remoteStart(Node(host, ControllerActor.defaultPort), m.erasure.asInstanceOf[Class[A]])
  }

  def remoteStart[A <: Actor](node: Node)(implicit m: Manifest[A], cfg: Configuration[Proxy]) {
    remoteStart(node, m.erasure.asInstanceOf[Class[A]])
  }

  def remoteStart[A <: Actor](host: String, actorClass: Class[A])(implicit cfg: Configuration[Proxy]) {
    remoteStart(Node(host, ControllerActor.defaultPort), actorClass)
  }

  /**
   * Start a new instance of an actor of class <code>actorClass</code> on
   * the given remote node. The port of the <code>node</code> argument is used
   * to contact the <code>ControllerActor</code> listening on
   * <code>node</code>.
   */
  def remoteStart[A <: Actor](node: Node, actorClass: Class[A])(implicit cfg: Configuration[Proxy]) {
    val remoteController = select(node, ControllerSymbol)
    remoteController !? RemoteStartInvoke(actorClass.getName) match {
      case RemoteStartResult(None) => // success, do nothing
      case RemoteStartResult(Some(e)) => throw new RuntimeException(e)
      case _ => throw new RuntimeException("Failed")
    }
  }


  def remoteStartAndListen[A <: Actor, P <: Proxy](host: String, port: Int, name: Symbol)(implicit m: Manifest[A], cfg: Configuration[P]): P =
    remoteStartAndListen(Node(host, ControllerActor.defaultPort), m.erasure.asInstanceOf[Class[A]], port, name)

  def remoteStartAndListen[A <: Actor, P <: Proxy](host: String, actorClass: Class[A], port: Int, name: Symbol)(implicit cfg: Configuration[P]): P =
    remoteStartAndListen(Node(host, ControllerActor.defaultPort), actorClass, port, name)

  def remoteStartAndListen[A <: Actor, P <: Proxy](node: Node, port: Int, name: Symbol)(implicit m: Manifest[A], cfg: Configuration[P]): P =
    remoteStartAndListen(node, m.erasure.asInstanceOf[Class[A]], port, name)

  /**
   * Start a new instance of an actor of class <code>actorClass</code> on the
   * given remote node, listening on <code>port</code> under name
   * <code>name</code>, and return a proxy handle to the new remote instance.
   *
   * Note: This function call actually explicitly registers the new instance
   * with the network kernel, so the code within <code>actorClass</code>
   * does not need to make calls to <code>alive</code> and
   * <code>register</code> to achieve the desired effect.
   */
  def remoteStartAndListen[A <: Actor, P <: Proxy](node: Node, actorClass: Class[A], port: Int, name: Symbol)(implicit cfg: Configuration[P]): P = {
    remoteStart(node, actorClass)
    select(Node(node.address, node.port), name)
  }

  /**
   * Returns a remote handle which can be serialized and sent remotely, which
   * contains the necessary information to re-establish the connection. If
   * <code>thisActor</code> is not currently listening on any port (via
   * call(s) to <code>alive</code>), a random port is selected.
   */
  def remoteActorFor[P <: Proxy](thisActor: Actor)(implicit cfg: Configuration[P]): P = {
    val serializer = cfg.newSerializer()

    // need to establish a port for this actor to listen on, so that the proxy
    // returned by remoteSelf makes sense (as in, you can connect to it)
    val port = portToActors.synchronized {

      // check actorToPorts first to see if this actor is already alive
      actorToPorts.get(thisActor).flatMap(_.headOption).getOrElse({

        // oops, this actor isn't alive anywhere. in this case, pick a random
        // port (by scanning portToActors)
        val newPort = portToActors.headOption.map(_._1).getOrElse({

          // no ports are actually listening. so pick a random one now (by
          // listening with 0 as the port)
          val listener = listenOnPort(0, cfg.aliveMode)

          // return the actual port
          listener.port
        })

        // now call alive, so that thisActor gets mapped to the new port (if it
        // already wasn't)
        alive(newPort, thisActor)

        newPort
      })
    }

    val remoteNode        = serializer.newNode(Node.localhost, port)
    val remoteConnectMode = cfg.selectMode
    val clzName           = serializer.getClass.getName
    val remoteName        = netKernel.getOrCreateName(thisActor) // auto-registers name if a new one is created
    serializer.newProxy(remoteNode, remoteConnectMode, clzName, remoteName)
  }

  /**
   * Returns a remote handle for the current running actor. Equivalent to
   * <code>remoteActorFor(Actor.self)</code>.
   */
  def remoteSelf[P <: Proxy](implicit cfg: Configuration[P]): P =
    remoteActorFor(Actor.self)

  private def connect(node: Node, serializer: Serializer[Proxy], mode: ServiceMode.Value): MessageConnection =
    netKernel.connect(node, serializer, mode)

  /**
   * Returns (a proxy for) the actor registered under
   * <code>sym</code> on <code>node</code>. Note that if you call select
   * outside of an actor, you cannot rely on explicit termination to shutdown.
   */
  @throws(classOf[InconsistentSerializerException])
  @throws(classOf[InconsistentServiceException])
  def select[P <: Proxy](node: Node, sym: Symbol)(implicit cfg: Configuration[P]): P = {
    Debug.info("select(): node: " + node + " sym: " + sym + " selectMode: " + cfg.selectMode)
    val thisActor = Actor.self
    remoteActors.synchronized { remoteActors += thisActor }
    watchActor(thisActor)
    netKernel.getOrCreateProxy(connect(node, cfg.newSerializer(), cfg.selectMode), sym).asInstanceOf[P]
  }

  /**
   * Returns (a proxy for) the actor registered under
   * <code>name</code> on <code>node</code>. Note that this method is exactly
   * the same as <code>select</code>, except that it is safe to call it
   * outside of an actor, and still rely on explicit termination. However,
   * calling this method inside an actor can cause the network kernel to
   * shutdown if no other actors are using remote services (and said actor is
   * not explicitly listening via <code>alive</code>, and explicit termination
   * is not set).
   */
  @throws(classOf[InconsistentSerializerException])
  @throws(classOf[InconsistentServiceException])
  def remoteActorAt[P <: Proxy](node: Node, sym: Symbol)(implicit cfg: Configuration[P]): P = {
    Debug.info("remoteActorAt(): node: " + node + " sym: " + sym + " selectMode: " + cfg.selectMode)
    netKernel.getOrCreateProxy(connect(node, cfg.newSerializer(), cfg.selectMode), sym).asInstanceOf[P]
  }

  /**
   * Shutdown the network kernel explicitly. Only necessary when
   * <code>setExplicitTermination(true)</code> has been called.
   *
   * Because this method may block until write buffers are drained, use
   * <code>releaseResourcesInActor</code> to shut down the network kernel from
   * within an actor.
   */
  def releaseResources() {
    synchronized {
      if (_nk ne null) {
        assert(_ctrl ne null)
        Debug.info("releaseResources(): sending Terminate to _ctrl")
        _ctrl ! Terminate
        _ctrl = null

        Debug.info("releaseResources(): calling terminateTop on _nk")
        _nk.terminateTop()
        _nk = null
      }
    }
  }

  /**
   * Shutdown the network kernel explicitly from within an actor. Only necessary when
   * <code>setExplicitTermination(true)</code> has been called. 
   *
   * This particular implementation simply spawns a new thread to call
   * <code>releaseResources</code>.
   */
  def releaseResourcesInActor() {
    newThread { releaseResources() }
  }

}

case class InconsistentSerializerException(expected: Serializer[Proxy], actual: Serializer[Proxy]) 
  extends Exception("Inconsistent serializers: Expected " + expected + " but got " + actual)

case class InconsistentServiceException(expected: ServiceMode.Value, actual: ServiceMode.Value) 
  extends Exception("Inconsistent service modes: Expected " + expected + " but got " + actual)

case class NameAlreadyRegisteredException(sym: Symbol, a: OutputChannel[Any])
  extends Exception("Name " + sym + " is already registered for channel " + a)
