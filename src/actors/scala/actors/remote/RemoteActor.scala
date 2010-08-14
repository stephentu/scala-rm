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
 *  This object provides methods for creating, registering, configuring, and
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
 *  Or with the shorthand syntax:
 *  {{{
 *  remoteActor(9010, 'myName) {
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
 *  There are several relevant points to make about this new remote actors
 *  implementation:
 *  <ul>
 *    <li>The configuration of remote actors is done via a
 *    <code>Configuration</code> object. Most of the publically accessible methods
 *    in <code>RemoteActor</code> take an implicit parameter of type
 *    <code>Configuration</code>. See the documentation for <code>Configuration</code>
 *    for more information on the configurable parameters. By default,
 *    <code>DefaultConfiguration</code> is used (that is, if no explicit
 *    configuration is made by the user). This sets up blocking network mode,
 *    in addition to using Java serialization.<br/>
 *
 *    Blocking network mode is recommended when the number of network nodes 
 *    is small (no less than 1000), since it uses the standard one thread per 
 *    connection model. If more connections are expected, then non-blocking network mode
 *    is recommended, since it multiplexes all connections in a small,
 *    fixed-size thread pool, backed by Java NIO.</li>
 *
 *    <li>The network kernel makes its best effort to propogate network
 *    exceptions to an <code>Actor</code>'s <code>exceptionHandler</code>.<br/>
 *
 *    <li>The type returned for a remote actor is a <code>RemoteProxy</code>.
 *    See the details below for the difference in semantics between a
 *    <code>RemoteProxy</code>, and a regular <code>Actor</code>.</li>
 *  </ul>
 *
 * @author Philipp Haller
 * @author Stephen Tu
 */
object RemoteActor {

  /**
   * References to remote actors have type `RemoteProxy`. 
   * Instances of RemoteProxy are lightweight and serializable.
   * There are a few caveats, however, which make the semantics of a
   * <code>RemoteProxy</code> slightly different from a regular
   * <code>Actor</code>, due to the remote nature of the handle.<br/>
   *
   * Firstly, a <code>RemoteProxy</code> is configured via the
   * <code>Configuration</code> object passed to <code>select</code>,
   * <code>remoteActorAt</code>, or <code>remoteActorFor</code>. Each unique
   * call returns a <b>new</b> instance of a <code>RemoteProxy</code>, so it
   * is possible to call <code>select</code>, for instance, with different
   * configurations, and get back proxies which have different semantics, but
   * all represent the same remote actor.<br/>
   *
   * <code>RemoteProxy</code> instances are of course thread-safe. They are
   * also lazy in the sense that a new connection is not created unless it is
   * needed.<br/>
   *
   * Secondly, and most notably, the major difference is that send operations 
   * are not truly non-blocking, unless a <code>Configuration</code> 
   * object is used with a <code>ServiceMode</code> of <code>NonBlocking</code>. 
   * For the default mode of operation (the <code>Blocking</code> mode), 
   * send operations block until the data is written to the wire, that is, 
   * the writes happen within the same thread (an exception to this is <code>!!</code> on a 
   * <code>RemoteProxy</code>, which returns immediately regardless of mode). 
   * However long that takes is entirely determined by the Socket API, and 
   * not by the network kernel. Thus, if using <code>Blocking</code> mode with
   * a lot of remote actors, one might considering increasing the size of the thread
   * pool for the <code>Actor</code> scheduler.<br/>
   *
   * In <code>NonBlocking</code> mode, send operations have similar
   * semantics as regular actors. That is, send operations merely append to some buffer, and
   * return. If an error occurs for a write, the exception is propagated to
   * the actor's <code>exceptionHandler</code>.<br/> 
   */
  type RemoteProxy = AbstractActor

  /**
   * Shorthand factory method for the following common pattern:
   * {{{
   * actor {
   *    alive(port)
   *    register(name, self)
   *    body 
   * }
   * }}}
   *
   * Example usages:
   * {{{
   * val localActor  = actor {
   *   // actor behavior ...
   * }
   * 
   * val remoteActor = remoteActor(9000, 'anActor) {
   *   // remote actor behavior ...
   * }
   * }}}
   *
   * @param port  The TCP port to listen on
   * @param name  The name to register this actor under 
   * @param body  The behavior of the actor
   * @param cfg   The configuration passed to <code>alive</code>
   */
  def remoteActor(port: Int, name: Symbol)(body: => Unit)(implicit cfg: Configuration): Actor = {
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

  @volatile private var _ctrl: ControllerActor = _

  /**
   * Enable the remote start functionality service, exposed on port 11723.
   * The <code>Configuration</code> instance here is used to configure not
   * only the listening mode of the remote start service, but also the
   * <code>Serializer</code> used to pass control messages around, in addition
   * to the listening mode of any new actors started by this service via
   * <code>remoteStart</code>.
   *
   * @param cfg   The configuration used to determine how the remote start
   *              service listens (<code>aliveMode</code>), and how newly
   *              spawned actors listen (<code>aliveMode</code).
   */
  def enableRemoteStart()(implicit cfg: Configuration) {
    enableRemoteStart(ControllerActor.defaultPort)
  }

  /**
   * Enable the remote start functionality service, exposed on the provided
   * <code>port</code>. If the service is already running on
   * <code>port</code>, then this method does nothing.
   * 
   * @param port  The port to start the remote start service on.
   * @param cfg   The configuration used to determine how the remote start
   *              service listens (<code>aliveMode</code>), and how newly
   *              spawned actors listen (<code>aliveMode</code).
   */
  def enableRemoteStart(port: Int)(implicit cfg: Configuration) {
    Node.checkPort(port)
    synchronized {
      if (_ctrl eq null)
        _ctrl = new ControllerActor(port, ControllerSymbol)
      else if (_ctrl.port != port)
        throw new IllegalArgumentException("Remote start service already running on port: " + _ctrl.port)
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

  @volatile private var cl: ClassLoader = null

  @deprecated("Configure a JavaSerializer by hand instead")
  def classLoader: ClassLoader = cl

  /**
   * If set to <code>null</code> (default), the default class loader
   * of <code>java.io.ObjectInputStream</code> is used for deserializing
   * Java objects sent as messages. Custom serializers are free to ignore this
   * field (especially since it probably doesn't apply).
   *
   * Note: This <code>ClassLoader</code> is not the same as the
   * <code>ClassLoader</code> found in a <code>Configuration</code> object.
   * This one is just used to configure a <code>JavaSerializer</code>, and
   * will be removed in future releases.
   */
  @deprecated("Configure a JavaSerializer by hand instead")
  def classLoader_=(x: ClassLoader) { cl = x }

  @volatile private var explicitlyTerminate = false

  /**
   * Used to indicate whether explicit shutdown of the network kernel
   * resources is desired. The default value is <code>false</code>, meaning
   * that the network kernel will automatically shutdown when all actors which
   * have expressed intent to use remote functionality (via <code>alive</code>
   * and <code>select</code>) have terminated. In most simple usages, this is
   * the desirable behavior.<br/>
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
   * usage pattern would reuse the same resources.<br/>
   *
   * <b>Note</b>: If explicit termination is set to <code>true</code>, the JVM will
   * not shutdown (because of remaining threads running) until 
   * <code>shutdown</code> is invoked by the program.
   *
   * @param isExplicit  <code>true</code> if explicit termination is desired,
   *                    <code>false</code> otherwise
   */
  def setExplicitShutdown(isExplicit: Boolean) {
    explicitlyTerminate = isExplicit
  }

  private val actors = new ConcurrentHashMap[Symbol, OutputChannel[Any]]

  @inline private def checkName(s: Symbol) =
    if (s.name.isEmpty)
      throw new IllegalArgumentException("S cannot contain an empty name")
    //else if (s.name.startsWith("$"))
    //  throw new IllegalArgumentException("Names starting with `$` are reserved")

  /**
   * Registers <code>actor</code> to be selectable remotely via
   * <code>name</code>.<br/>
   *
   * <b>Note:</b> There are two limitations to the mutability of the
   * <code>name</code>. 

   * (1) The first is that only one <code>name</code> can be
   * valid at a time (distinct actors cannot register with the same
   * <code>name</code> at the same time). They can however, register with the
   * same name at different times.
   * 
   * (2) The second is that once an <code>actor</code> registers 
   * with a <code>name</code>, then that <code>actor</code> cannot 
   * register with a different name. Thus, each <code>actor</code> instance
   * effectively has a unique name throughout its lifetime.
   *
   * <b>Implementation Limitation:</b> Currently names starting with `$` are
   * reserved for internal use.
   *
   * @param name  The unique name for which to identify actor remotely. Cannot
   *              be an empty Symbol.
   * @param actor The actor to be identified via name
   */
  @throws(classOf[NameAlreadyRegisteredException])
  @throws(classOf[ChannelAlreadyRegisteredException])
  @throws(classOf[IllegalArgumentException])
  def register(name: Symbol, actor: OutputChannel[Any]) {
    checkName(name)
    register0(name, actor)
  }

  private[remote] def register0(name: Symbol, actor: OutputChannel[Any]) {
    val existing = actors.putIfAbsent(name, actor)
    if (existing ne null) {
      if (existing != actor)
        throw new NameAlreadyRegisteredException(name, existing)
      Debug.warning("re-registering " + name + " to channel " + actor)
    } else 
      Debug.info(this + ": successfully mapped " + name + " to " + actor)
    actor.channelName match {
      case Some(prevName) => 
        if (prevName != name)
          throw new ChannelAlreadyRegisteredException(actor, prevName)
      case None =>
        actor.channelName = Some(name)
    }
  }

  /**
   * Unregisters <code>actor</code> from being selectable remotely. If
   * <code>actor</code> is currently not registered, this method is a no-op.
   *
   * @param actor   The actor to be unregistered
   *
   * @see   register
   */
  def unregister(actor: OutputChannel[Any]) {
    actor match {
      case typedActor: Actor if (typedActor.getState != Actor.State.Terminated) =>
        /* Figure out if this actor is an active remote actor */
        val runHandler = remoteActors.synchronized {
          remoteActors.contains(typedActor)
        }
        if (runHandler) {
          /* If we're unregistering an active actor, we also want to invoke its
          * termination handler */
          removeActorInterest(typedActor)
          typedActor onTerminate {} // and replace the handler with a no-op
                                    // so it doesn't get run again
        } else 
          /* Since it's not active, just remove its mapping then */
          unregister0(actor)
      case _ =>
        /* Otherwise, just remove its mapping, since it has no termination
         * handler */
        unregister0(actor)
    }
  }

  @inline private def unregister0(actor: OutputChannel[Any]) {
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

  private[remote] def finishChannel(name: Symbol) =
    Option(channels.remove(name))

  private[remote] def finishSession(ch: OutputChannel[Any]) = Option(sessions.remove(ch))

  private[remote] def startSession(ch: OutputChannel[Any], name: Symbol) {
    sessions.put(ch, name)
  }

  @inline private def watchActor(actor: Actor) {
    // set termination handler on actor
    actor onTerminate { actorTerminated(actor) }
  }

  private[remote] def alive0(port: Int, actor: Actor, addToSet: Boolean)(implicit cfg: Configuration) {
    Debug.info("alive0(port: %d, actor: %s, addToSet: %s".format(port, actor, addToSet))
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
   * more than one port (via the same name).<br/>
   *
   * <b>Implementation Detail:</b> The current implementation does not provide
   * port-level isolation. This means that any port actually can be used to
   * handle requests for any registered actor. This behavior is subject to
   * change, meaning you should not (1) rely on <code>alive</code> to give
   * isolation to actors, and (2) should not only call <code>alive</code> once
   * and rely on that port to handle every incoming request for correctness.
   *
   * @param port  The TCP port to listen on
   * @param actor The actor to listen on port
   * @param cfg   The configuration object used to determine how to spawn a
   *              new listener (<code>aliveMode</code>)
   */
  @throws(classOf[InconsistentServiceException])
  def alive(port: Int, actor: Actor)(implicit cfg: Configuration) {
    alive0(port, actor, true)
  }

  /**
   * Makes <code>self</code> remotely accessible on TCP port
   * <code>port</code>. Is equivalent to <code>alive(port, self)</code>. 
   * Uses <code>aliveMode</code> from the <code>Configuration</code> to
   * determine which network mode to spawn a listener, if one is not already
   * alive on <code>port</code>.<br/>
   *
   * However, if a listener is currently alive on <code>port</code> in a
   * difference mode, then a <code>InconsistentServiceException</code>
   * exception is thrown.

   * @param port  The TCP port to listen on
   * @param cfg   The configuration object used to determine how to spawn a
   *              new listener (<code>aliveMode</code>)
   */
  @throws(classOf[InconsistentServiceException])
  def alive(port: Int)(implicit cfg: Configuration) {
    alive0(port, Actor.self, true)
  }

  private final val EmptyIntSet = new HashSet[Int]

  @inline private def actorTerminated(actor: Actor) {
    Debug.info("actorTerminated(): alive actor " + actor + " terminated")
    removeActorInterest(actor)
  }

  private def removeActorInterest(actor: Actor) {
    // remove from actor map
    unregister0(actor)

    // shut down ports if necessary
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

    // shut down the service if necessary
    remoteActors.synchronized {
      remoteActors -= actor
      if (!explicitlyTerminate && remoteActors.isEmpty)
        shutdown()
    }
  }

  /**
   * Start the actor of class <code>A</code> on the node identified by
   * <code>host</code>, with the remote start service running on the default
   * port 11723. Uses the <code>Configuration</code> to configure how to
   * connect to the starting service, in addition to the
   * <code>Serializer</code> used to pass the service control messages.<br/>
   *
   * <b>Note:</b> This method blocks until the remote side has given a success or
   * failure response, or a timeout occurs (of one minute). It however blocks
   * in a way that is safe for actors (won't cause starvation).
   *
   * <b>Note:</b> This method does NOT <code>register</code> or call
   * <code>alive</code> on the newly started actor. Used one of the other
   * overloaded <code>remoteStart</code> methods which accept a port and name to do
   * that. 
   *
   * @param host  The hostname of the remote node to start
   * @param cfg   The configuration object used to determine how to connect to
   *              (<code>selectMode</code>) the remote start listener, and how
   *              to send control messages (<code>newSerializer</code>).
   *
   * @see   Configuration
   */
  @throws(classOf[RemoteStartException])
  def remoteStart[A <: Actor](host: String)(implicit m: Manifest[A], cfg: Configuration) {
    remoteStart(Node(host, ControllerActor.defaultPort), m.erasure.getName)
  }

  /**
   * Start the actor of class <code>A</code> on the node identified by
   * <code>Node</code>
   *
   * @param node  The node identifying the remote start service
   * @param cfg   The configuration object used to determine how to connect to
   *              (<code>selectMode</code>) the remote start listener, and how
   *              to send control messages (<code>newSerializer</code>).
   *
   * @see   Configuration
   */
  @throws(classOf[RemoteStartException])
  def remoteStart[A <: Actor](node: Node)(implicit m: Manifest[A], cfg: Configuration) {
    remoteStart(node, m.erasure.getName)
  }

  /**
   * Start the actor of class name <code>actorClass</code> on the node
   * identified by <code>host</code>, with the remote start service running on
   * default port 11723.
   *
   * @param host        The hostname of the remote node to start
   * @param actorClass  The class name of the actor to start remotely
   * @param cfg         The configuration object used to determine how to connect to
   *                    (<code>selectMode</code>) the remote start listener, and how
   *                    to send control messages (<code>newSerializer</code>).
   *
   * @see   Configuration
   */
  @throws(classOf[RemoteStartException])
  def remoteStart(host: String, actorClass: String)(implicit cfg: Configuration) {
    remoteStart(Node(host, ControllerActor.defaultPort), actorClass)
  }

  /**
   * Start a new instance of an actor of class <code>actorClass</code> on
   * the given remote node. The port of the <code>node</code> argument is used
   * to contact the <code>ControllerActor</code> listening on
   * <code>node</code>.
   *
   * @param node        The node identifying the remote start service
   * @param actorClass  The class name of the actor to start remotely
   * @param cfg         The configuration object used to determine how to connect to
   *                    (<code>selectMode</code>) the remote start listener, and how
   *                    to send control messages (<code>newSerializer</code>).
   *
   * @see   Configuration
   */
  @throws(classOf[RemoteStartException])
  def remoteStart(node: Node, actorClass: String)(implicit cfg: Configuration) {
    val c = cfg.newMessageCreator()
    val remoteController = select0(node, ControllerSymbol)
    remoteController !? (cfg.waitTimeout, c.newRemoteStartInvoke(actorClass)) match {
      case Some(RemoteStartResult(None))    => // success, do nothing
      case Some(RemoteStartResult(Some(e))) => throw new RemoteStartException(e)
      case Some(_) => throw new RemoteStartException("Failed: Invalid response")
      case None    => throw new RemoteStartException("Timed-out")
    }
  }

  /**
   * Start a new instance of <code>A</code> on <code>host</code> with the
   * start service listener running on port 11723, and make <code>A</code>
   * alive on <code>port</code>, registered with <code>name</code>.
   *
   * @param host  The hostname of the remote node to start
   * @param port  The port to make the newly started remote actor listen on
   * @param name  The name to register to the newly started remote actor
   * @param cfg   The configuration object used to determine how to connect to
   *              (<code>selectMode</code>) the remote start listener, and how
   *              to send control messages (<code>newSerializer</code>).
   *
   * @see   Configuration
   */
  @throws(classOf[RemoteStartException])
  def remoteStart[A <: Actor](host: String, port: Int, name: Symbol)(implicit m: Manifest[A], cfg: Configuration): RemoteProxy =
    remoteStart(Node(host, ControllerActor.defaultPort), m.erasure.getName, port, name)

  /**
   * Start a new instance of <code>actorClass</code> on <code>host</code> with the
   * start service listener running on port 11723, and make <code>actorClass</code>
   * alive on <code>port</code>, registered with <code>name</code>.
   *
   * @param host        The hostname of the remote node to start
   * @param actorClass  The class name of the actor to start remotely
   * @param port        The port to make the newly started remote actor listen on
   * @param name        The name to register to the newly started remote actor
   * @param cfg         The configuration object used to determine how to connect to
   *                    (<code>selectMode</code>) the remote start listener, and how
   *                    to send control messages (<code>newSerializer</code>).
   *
   * @see   Configuration
   */
  @throws(classOf[RemoteStartException])
  def remoteStart(host: String, actorClass: String, port: Int, name: Symbol)(implicit cfg: Configuration): RemoteProxy =
    remoteStart(Node(host, ControllerActor.defaultPort), actorClass, port, name)

  /**
   * Start a new instance of <code>A</code> on <code>node</code>, and make
   * <code>A</code> alive on <code>port</code>, registered with <code>name</code>.
   *
   * @param node  The node identifying the remote start service
   * @param port  The port to make the newly started remote actor listen on
   * @param name  The name to register to the newly started remote actor
   * @param cfg   The configuration object used to determine how to connect to
   *              (<code>selectMode</code>) the remote start listener, and how
   *              to send control messages (<code>newSerializer</code>).
   *
   * @see   Configuration
   */
  @throws(classOf[RemoteStartException])
  def remoteStart[A <: Actor](node: Node, port: Int, name: Symbol)(implicit m: Manifest[A], cfg: Configuration): RemoteProxy =
    remoteStart(node, m.erasure.getName, port, name)

  /**
   * Start a new instance of an actor of class <code>actorClass</code> on the
   * given remote node, listening on <code>port</code> under name
   * <code>name</code>, and return a proxy handle to the new remote
   * instance.<br/>
   *
   * <b>Note:</b> This function call actually explicitly registers the new instance
   * with the network kernel, so the code within <code>actorClass</code>
   * does not need to make calls to <code>alive</code> and
   * <code>register</code> to achieve the desired effect.
   *
   * @param node        The node identifying the remote start service
   * @param actorClass  The class name of the actor to start remotely
   * @param port        The port to make the newly started remote actor listen on
   * @param name        The name to register to the newly started remote actor
   * @param cfg         The configuration object used to determine how to connect to
   *                    (<code>selectMode</code>) the remote start listener, and how
   *                    to send control messages (<code>newSerializer</code>).
   *
   * @see   Configuration
   */
  @throws(classOf[RemoteStartException])
  def remoteStart(node: Node, actorClass: String, port: Int, name: Symbol)(implicit cfg: Configuration): RemoteProxy = {
    checkName(name)
    Node.checkPort(port, true)
    val remoteController = select0(node, ControllerSymbol)
    val c = cfg.newMessageCreator()
    remoteController !? (cfg.waitTimeout, c.newRemoteStartInvokeAndListen(actorClass, port, name.name)) match {
      case Some(RemoteStartResult(None))    => // success, do nothing
      case Some(RemoteStartResult(Some(e))) => throw new RemoteStartException(e)
      case Some(_) => throw new RemoteStartException("Failed: invalid response")
      case None    => throw new RemoteStartException("Timed-out")
    }
    select0(Node(node.address, port), name)
  }

  /**
   * Returns a remote handle which can be serialized and sent remotely, which
   * contains the necessary information to re-establish the connection. If
   * <code>actor</code> is not currently listening on any port (via
   * call(s) to <code>alive</code>), a random port is selected. If
   * <code>actor</code> is not currently registered under any name, a
   * random name is chosen.
   *
   * @param   actor   The actor to return a remote proxy handle to
   *
   * @return  A serializable handle to <code>actor</code>
   *
   * @see     Configuration
   */
  def remoteActorFor(actor: Actor)(implicit cfg: Configuration): RemoteProxy = {
    // need to establish a port for this actor to listen on, so that the proxy
    // returned by remoteSelf makes sense (as in, you can connect to it)
    val port = portToActors.synchronized {

      // check actorToPorts first to see if this actor is already alive
      actorToPorts.get(actor).flatMap(_.headOption).getOrElse({

        // oops, this actor isn't alive anywhere. in this case, pick a random
        // port (by scanning portToActors)
        val newPort = portToActors.headOption.map(_._1).getOrElse({

          // no ports are actually listening. so pick a random one now (by
          // listening with 0 as the port)
          val listener = NetKernel.getListenerFor(0, cfg)

          // return the actual port
          listener.port
        })

        // now call alive, so that actor gets mapped to the new port (if it
        // already wasn't)
        alive(newPort, actor)

        newPort
      })
    }

    new ConfigProxy(Node(port), getOrCreateName(actor), cfg)
  }

  /**
   * Returns (a proxy for) the actor registered under
   * <code>sym</code> on <code>node</code>. Note that if you call
   * <code>select</code> outside of an actor, you cannot rely on explicit 
   * termination to shutdown. Use <code>remoteActorAt</code> to select a
   * remote actor out of an actor.<br/>
   *
   * <b>Note:</b> This method simple creates a new proxy instance which is
   * lazy; no TCP connections are made until the return handle is used.
   *
   * @param   node  The node of the remote actor to select
   * @param   sym   The unique identifier of the remote actor to select
   * @param   cfg   The configuration object used to determine how the remote
   *                actor should be select (<code>selectMode</code>), and how
   *                messages sent to the remote actor should be serialized
   *                (<code>newSerializer</code>).
   *
   * @return  A serializable remote handle to the remote proxy.
   *
   * @see     remoteActorAt
   * @see     Configuration
   * @see     RemoteProxy
   */
  def select(node: Node, sym: Symbol)(implicit cfg: Configuration): RemoteProxy = {
    checkName(sym)
    Debug.info("select(): node: " + node + " sym: " + sym + " selectMode: " + cfg.selectMode)
    select0(node, sym)
  }

  private def select0(node: Node, sym: Symbol)(implicit cfg: Configuration) = {
    val thisActor = Actor.self
    remoteActors.synchronized {
      if (remoteActors.add(thisActor))
        watchActor(thisActor)
    }
    remoteActorAt0(node, sym)
  }

  /**
   * Returns (a proxy for) the actor registered under
   * <code>name</code> on <code>node</code>. Note that this method is exactly
   * the same as <code>select</code>, except that it is safe to call it
   * outside of an actor, and still rely on explicit termination. However,
   * calling this method inside an actor can cause the network kernel to
   * shutdown if no other actors are using remote services (and said actor is
   * not explicitly listening via <code>alive</code>, and explicit termination
   * is not set).<br/>
   *
   * <b>Note:</b> This method simple creates a new proxy instance which is
   * lazy; no TCP connections are made until the return handle is used.
   *
   * @param   node  The node of the remote actor to select
   * @param   sym   The unique identifier of the remote actor to select
   * @param   cfg   The configuration object used to determine how the remote
   *                actor should be select (<code>selectMode</code>), and how
   *                messages sent to the remote actor should be serialized
   *                (<code>newSerializer</code>).
   *
   * @return  A serializable remote handle to the remote proxy.
   *
   * @see     select
   * @see     Configuration
   * @see     RemoteProxy
   */
  def remoteActorAt(node: Node, sym: Symbol)(implicit cfg: Configuration): RemoteProxy = {
    checkName(sym)
    Debug.info("remoteActorAt(): node: " + node + " sym: " + sym + " selectMode: " + cfg.selectMode)
    remoteActorAt0(node, sym)
  }

  @inline private def remoteActorAt0(node: Node, sym: Symbol)(implicit cfg: Configuration) =
    new ConfigProxy(node, sym, cfg)

  /**
   * Shutdown the network kernel explicitly. Only necessary when
   * <code>setExplicitTermination(true)</code> has been called. This method
   * also has the effect of stopping the remote start listener, if it had been
   * started.
   *
   * @see setExplicitTermination
   * @see enableRemoteStart
   */
  def shutdown() {
    stopRemoteStartListener()
    NetKernel.releaseResources()
  }

}

/**
 * Thrown by <code>alive</code> to indicate that the port was already being
 * listened on with a <code>ServiceMode</code> different from the one that was requested to
 * be used.
 *
 * @see alive
 * @see ServiceMode
 */
@serializable
case class InconsistentServiceException(expected: ServiceMode.Value, actual: ServiceMode.Value) 
  extends Exception("Inconsistent service modes: Expected " + expected + " but got " + actual)

/**
 * Thrown by <code>alive</code> to indicate that the port was already being
 * listened on with a <code>Serializer</code> different from the one that was requested to
 * be used.
 *
 * @see alive
 * @see Serializer
 */
case class InconsistentSerializerException(expected: Serializer, actual: Serializer) 
  extends Exception("Inconsistent serializers: Expected " + expected + " but got " + actual)

/**
 * Thrown by <code>register</code> to indicate that the name <code>sym</code> requested has
 * already been taken by actor <code>a</code>. See the documention for
 * <code>register</code> for the restrictions on names.
 *
 * @see register
 */
case class NameAlreadyRegisteredException(sym: Symbol, a: OutputChannel[Any])
  extends Exception("Name " + sym + " is already registered for channel " + a)

/**
 * Thrown by <code>register</code> to indicate that the <code>channel</code>
 * has already been registered under a different name <code>sym</code>. 
 * See the documention for <code>register</code> for the restrictions on names.
 *
 * @see register
 */
case class ChannelAlreadyRegisteredException(channel: OutputChannel[Any], sym: Symbol)
  extends Exception("The channel %s is already registered as %s".format(channel, sym))

/**
 * Thrown by the various send methods (<code>!</code>, <code>!!</code>, and
 * <code>!?</code>) of <code>RemoteProxy</code> to indicate that the remote
 * actor selected does not exist on that machine. Depending on the
 * <code>ConnectPolicy</code> of a particlar <code>RemoteProxy</code>, this
 * exception can also get passed to an actor's <code>exceptionHandler</code>.
 *
 * @see RemoteProxy
 */
@serializable
case class NoSuchRemoteActorException(name: Symbol)
  extends Exception("No such remote actor: " + name)

/**
 * Thrown by the various <code>remoteStart</code> methods to indicate an
 * exception on the remote end when trying to process the remote start
 * request.
 *
 * @see remoteStart
 */
@serializable
case class RemoteStartException(message: String)
  extends Exception(message)
