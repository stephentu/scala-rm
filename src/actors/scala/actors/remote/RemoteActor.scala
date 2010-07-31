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

import java.util.concurrent.ConcurrentHashMap

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
   * TODO: Comment
   */
  type RemoteProxy = AbstractActor

  /**
   * Shorthand constructor for:
   * {{{
   * actor {
   *    alive(port)
   *    register(name, self)
   *    body 
   * }
   * }}}
   */
  def actor(port: Int, name: Symbol)(body: => Unit)(implicit cfg: Configuration): Actor = {
    Actor.actor {
      alive(port)
      register(name, Actor.self)
      body 
    }
  }

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

  private var _ctrl: Actor = _

  /**
   * TODO: Comment
   */
  def startRemoteStartListener() {
    synchronized {
      if (_ctrl eq null)
        _ctrl = new ControllerActor(ControllerSymbol)
    }
  }

  private[remote] def stopRemoteStartListener() {
    synchronized {
      if (_ctrl ne null) {
        _ctrl.send(Terminate, null)
        _ctrl = null
      }
    }
  }

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
   * Used to indicate whether explicit shutdown of the network kernel
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
   * <code>setExplicitShutdown</code> to <code>true</code>, the above
   * usage pattern would reuse the same resources.
   *
   * Note: If explicit termination is set to <code>true</code>, the JVM will
   * not shutdown (because of remaining threads running) until
   * <code>shutdown</code> is invoked by the program.
   */
  def setExplicitShutdown(isExplicit: Boolean) {
    explicitlyTerminate = isExplicit
  }

  private var _defaultConfig: Configuration = Configuration.DefaultConfig

  /**
   * Specify the <code>Configuration</code> object used to configure incoming
   * proxy handles. That is, if a remote handle is sent to this machine via a
   * message, this controls the configuration of the connection established
   * by the new remote handle.
   */
  def setProxyConfig(config: Configuration) {
    _defaultConfig = config
  }

  private[remote] def proxyConfig = _defaultConfig

  private val actors = new ConcurrentHashMap[Symbol, OutputChannel[Any]]

  /**
   * Registers <code>actor</code> to be selectable remotely via
   * <code>name</code>. 
   *
   * Note: There are two limitations to the mutability of the
   * <code>name</code>. The first is that only one <code>name</code> can be
   * valid at a time (distinct actors cannot register with the same
   * <code>name</code> at the same time). The second is that once an
   * <code>actor</code> registers with a <code>name</code>, then that
   * <code>actor</code> cannot register with a different name again until
   * <code>unregister</code> is called first.
   */
  @throws(classOf[NameAlreadyRegisteredException])
  def register(name: Symbol, actor: OutputChannel[Any]) {
    val existing = actors.putIfAbsent(name, actor)
    if (existing ne null) {
      if (existing != actor)
        throw NameAlreadyRegisteredException(name, existing)
      Debug.warning("re-registering " + name + " to channel " + actor)
    } else Debug.info(this + ": successfully mapped " + name + " to " + actor)
    actor.channelName match {
      case Some(prevName) => 
        if (prevName != name)
          // TODO: change exception type
          throw new IllegalStateException("Channel already registered as: " + prevName)
      case None =>
        actor.channelName = Some(name)
    }
  }

  /**
   * TODO: comment
   */
  def unregister(actor: OutputChannel[Any]) {
    actor.channelName.foreach(name => {
      actors.remove(name)
      Debug.info(this + ": successfully removed name " + name + " for " + actor + " from map")
    })
  }

  @inline private[remote] def getActor(s: Symbol) = actors.get(s)

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

  private[remote] def newChannel(ch: OutputChannel[Any]) = {
    val fresh = FreshNameCreator.newSessionId()
    channels.put(fresh, ch)
    fresh
  }

  private[remote] def findChannel(name: Symbol) = Option(channels.get(name))

  private[remote] def finishChannel(name: Symbol) = Option(channels.remove(name))

  private[remote] def finishSession(ch: OutputChannel[Any]) = Option(sessions.remove(ch))

  private[remote] def startSession(ch: OutputChannel[Any], name: Symbol) {
    sessions.put(ch, name)
  }

  private def watchActor(actor: Actor) {
    // set termination handler on actor
    actor onTerminate { actorTerminated(actor) }
  }

  private[remote] def alive0(port: Int, actor: Actor, addToSet: Boolean)(implicit cfg: Configuration) {
    // listen if necessary
    val listener = NetKernel.getListenerFor(port, cfg)

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

    if (addToSet) {
      remoteActors.synchronized {
        if (remoteActors.add(actor))
          watchActor(actor)
      }
    }
  }

  /**
   * Makes <code>actor</code> remotely accessible on TCP port
   * <code>port</code>. An <code>actor</code> can be remotely accessible via
   * more than one port.
   *
   * Implementation Detail: The current implementation does not provide
   * port-level isolation. This means that any port actually can be used to
   * handle requests for any registered actor. This behavior is subject to
   * change, meaning you should not (1) rely on <code>alive</code> to give
   * isolation to actors, and (2) should not only call <code>alive</code> once
   * and rely on that port to handle every incoming request for correctness.
   */
  def alive(port: Int, actor: Actor)(implicit cfg: Configuration) {
    alive0(port, actor, true)
  }

  /**
   * Makes <code>self</code> remotely accessible on TCP port
   * <code>port</code>. Is equivalent to <code>alive(port, self)</code>. 
   */
  @throws(classOf[InconsistentServiceException])
  def alive(port: Int)(implicit cfg: Configuration) {
    alive0(port, Actor.self, true)
  }

  private final val EmptyIntSet = new HashSet[Int]

  private def actorTerminated(actor: Actor) {
    Debug.info("actorTerminated(): alive actor " + actor + " terminated")
    unregister(actor)

    portToActors.synchronized {
      // get all the ports this actor was listening on...
      val candidatePorts = actorToPorts.remove(actor).getOrElse(EmptyIntSet)

      // ... now for each of these candidates, remove this actor
      // from the set and terminate if the set becomes empty
      val ports = candidatePorts.filter(p => portToActors.get(p) match {
        case Some(actors) =>
          actors -= actor
          if (actors.isEmpty) {
            portToActors -= p
            true
          } else false
        case None => true 
      })
      
      if (!explicitlyTerminate)
        ports.foreach(p => NetKernel.unlisten(p))
    }

    remoteActors.synchronized {
      remoteActors -= actor
      if (!explicitlyTerminate && remoteActors.isEmpty)
        shutdown()
    }
  }

  def remoteStart[A <: Actor](host: String)(implicit m: Manifest[A], cfg: Configuration) {
    remoteStart(Node(host, ControllerActor.defaultPort), m.erasure.getName)
  }

  def remoteStart[A <: Actor](node: Node)(implicit m: Manifest[A], cfg: Configuration) {
    remoteStart(node, m.erasure.getName)
  }

  def remoteStart(host: String, actorClass: String)(implicit cfg: Configuration) {
    remoteStart(Node(host, ControllerActor.defaultPort), actorClass)
  }

  /**
   * Start a new instance of an actor of class <code>actorClass</code> on
   * the given remote node. The port of the <code>node</code> argument is used
   * to contact the <code>ControllerActor</code> listening on
   * <code>node</code>.
   */
  def remoteStart(node: Node, actorClass: String)(implicit cfg: Configuration) {
    val remoteController = select(node, ControllerSymbol)
    remoteController !? RemoteStartInvoke(actorClass) match {
      case RemoteStartResult(None) => // success, do nothing
      case RemoteStartResult(Some(e)) => throw new RuntimeException(e)
      case _ => throw new RuntimeException("Failed")
    }
  }


  def remoteStartAndListen[A <: Actor](host: String, port: Int, name: Symbol)(implicit m: Manifest[A], cfg: Configuration): RemoteProxy =
    remoteStartAndListen(Node(host, ControllerActor.defaultPort), m.erasure.getName, port, name)

  def remoteStartAndListen(host: String, actorClass: String, port: Int, name: Symbol)(implicit cfg: Configuration): RemoteProxy =
    remoteStartAndListen(Node(host, ControllerActor.defaultPort), actorClass, port, name)

  def remoteStartAndListen[A <: Actor](node: Node, port: Int, name: Symbol)(implicit m: Manifest[A], cfg: Configuration): RemoteProxy =
    remoteStartAndListen(node, m.erasure.getName, port, name)

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
  def remoteStartAndListen(node: Node, actorClass: String, port: Int, name: Symbol)(implicit cfg: Configuration): RemoteProxy = {
    val remoteController = select(node, ControllerSymbol)
    remoteController !? RemoteStartInvokeAndListen(actorClass, port, name, cfg.aliveMode) match {
      case RemoteStartResult(None) => // success, do nothing
      case RemoteStartResult(Some(e)) => throw new RuntimeException(e)
      case _ => throw new RuntimeException("Failed")
    }
    select(Node(node.address, port), name)
  }

  /**
   * Returns a remote handle which can be serialized and sent remotely, which
   * contains the necessary information to re-establish the connection. If
   * <code>thisActor</code> is not currently listening on any port (via
   * call(s) to <code>alive</code>), a random port is selected. If
   * <code>thisActor</code> is not currently registered under any name, a
   * random name is chosen.
   */
  def remoteActorFor(thisActor: Actor)(implicit cfg: Configuration): RemoteProxy = {
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
          val listener = NetKernel.getListenerFor(0, cfg)

          // return the actual port
          listener.port
        })

        // now call alive, so that thisActor gets mapped to the new port (if it
        // already wasn't)
        alive(newPort, thisActor)

        newPort
      })
    }

    new ConfigProxy(Node(port), getOrCreateName(thisActor), cfg)
  }

  /**
   * Returns (a proxy for) the actor registered under
   * <code>sym</code> on <code>node</code>. Note that if you call select
   * outside of an actor, you cannot rely on explicit termination to shutdown.
   */
  @throws(classOf[InconsistentSerializerException])
  @throws(classOf[InconsistentServiceException])
  def select(node: Node, sym: Symbol)(implicit cfg: Configuration): RemoteProxy = {
    Debug.info("select(): node: " + node + " sym: " + sym + " selectMode: " + cfg.selectMode)
    val thisActor = Actor.self
    remoteActors.synchronized {
      if (remoteActors.add(thisActor))
        watchActor(thisActor)
    }
    remoteActorAt(node, sym)
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
  def remoteActorAt(node: Node, sym: Symbol)(implicit cfg: Configuration): RemoteProxy = {
    Debug.info("remoteActorAt(): node: " + node + " sym: " + sym + " selectMode: " + cfg.selectMode)
    new ConfigProxy(node, sym, cfg)
  }

  /**
   * Shutdown the network kernel explicitly. Only necessary when
   * <code>setExplicitTermination(true)</code> has been called.
   *
   * Because this method may block until write buffers are drained, use
   * <code>releaseResourcesInActor</code> to shut down the network kernel from
   * within an actor.
   */
  def shutdown() {
    stopRemoteStartListener()
    NetKernel.releaseResources()
  }
}


case class InconsistentSerializerException(expected: Serializer, actual: Serializer) 
  extends Exception("Inconsistent serializers: Expected " + expected + " but got " + actual)

case class InconsistentServiceException(expected: ServiceMode.Value, actual: ServiceMode.Value) 
  extends Exception("Inconsistent service modes: Expected " + expected + " but got " + actual)

case class NameAlreadyRegisteredException(sym: Symbol, a: OutputChannel[Any])
  extends Exception("Name " + sym + " is already registered for channel " + a)
