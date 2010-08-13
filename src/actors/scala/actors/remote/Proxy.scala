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

import java.io.{ ObjectInputStream, ObjectOutputStream, 
                 IOException, NotSerializableException }
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/**
 * This class is necessary so that <code>sender.receiver</code> can return a
 * type of <code>actor</code> which properly responds to messages. This
 * implementation just delegates back to the proxy.
 */
private[remote] class ProxyActor(val p: Proxy) extends Actor {
  override def start() = { throw new RuntimeException("Should never call start() on a ProxyActor") }
  override def act()     { throw new RuntimeException("Should never call act() on a ProxyActor")   }

  override def !(msg: Any) {
    p.!(msg)
  }

  override def send(msg: Any, replyTo: OutputChannel[Any]) {
    p.send(msg, replyTo)
  }

  override def forward(msg: Any) {
    p.forward(msg)
  }

  override def !?(msg: Any) =
    p.!?(msg)

  override def !?(msec: Long, msg: Any) =
    p.!?(msec, msg)

  override def !!(msg: Any) =
    p.!!(msg)

  override def !![A](msg: Any, handler: PartialFunction[Any, A]) =
    p.!!(msg, handler)

  override def linkTo(to: AbstractActor) {
    p.linkTo(to)
  }

  override def unlinkFrom(from: AbstractActor) {
    p.unlinkFrom(from)
  }

  override def exit(from: AbstractActor, reason: AnyRef) {
    p.exit(from, reason)
  }

}

private[remote] abstract class Proxy extends AbstractActor 
                                     with    ReplyReactor 
                                     with    ActorCanReply {

  /**
   * Target node of this proxy
   */
  def remoteNode: Node

  /**
   * Name of the (remote) actor which is the target of this proxy
   */
  def name: Symbol

  override def receiver = new ProxyActor(this)

  protected def conn(a: Option[AbstractActor]): MessageConnection
  protected def config: Configuration
  protected def terminateConn(usingConn: MessageConnection): Unit 
  protected def numRetries: Int   

  override def start() = { throw new RuntimeException("Should never call start() on a Proxy") }
  override def act()     { throw new RuntimeException("Should never call act() on a Proxy")   }

  private def tryRemoteAction(a: Option[AbstractActor])(f: MessageConnection => Unit) {
    var triesLeft = numRetries + 1
    var success   = false
    assert(triesLeft > 0)
    while (!success && triesLeft > 0) {
      triesLeft -= 1
      var usingConn: MessageConnection = null 
      try { 
        usingConn = conn(a)
        assert(usingConn ne null)
        f(usingConn)
        success = true
      } catch {
        case e: Exception =>
          Debug.error(this + ": tryRemoteAction(): Caught exception: " + e.getMessage)
          Debug.doError { e.printStackTrace() }
          if (usingConn ne null)
            terminateConn(usingConn)
          if (triesLeft == 0)
            throw e
          else
            Debug.error(this + ": as per retry policy, retrying (%d tries left)".format(triesLeft))
      }
    }
  }

  private[remote] def handleMessage(m: ProxyCommand) {
    m match {
      case cmd @ RemoteApply0(actor, rfun) =>
        Debug.info("cmd@Apply0: " + cmd)
        tryRemoteAction(Some(actor)) { usingConn =>
          // TODO: not sure if the cast is safe- will we wever receive a
          // RemoteApply0 request from a non reactor?
          NetKernel.remoteApply(usingConn, name.name, actor.asInstanceOf[Reactor[Any]], rfun, config)
        }
      case cmd @ LocalApply0(rfun @ ExitFun(_), target) =>
        Debug.info("cmd@LocalApply0: " + cmd)
        Debug.info("target: " + target + ", creator: " + this)
        // exit() has to be run in an actor, otherwise we'll get an exception
        Actor.actor { rfun(target, this) }
      case cmd @ LocalApply0(rfun, target) =>
        Debug.info("cmd@LocalApply0: " + cmd)
        Debug.info("target: " + target + ", creator: " + this)
        rfun(target, this)
      // Request from remote proxy.
      // `this` is local proxy.
      case cmd @ SendTo(out, msg) =>
        Debug.info("cmd@SendTo: " + cmd)
        // local send
        out.send(msg, this) // use the proxy as the reply channel
      case cmd @ StartSession(out, msg, session) =>
        Debug.info(this + ": creating new reply channel for session: " + session)

        // create a new reply channel...
        val replyCh = new Channel[Any](new ProxyActor(this))
        // ...that maps to session
        RemoteActor.startSession(replyCh, session)

        Debug.info(this + ": sending msg " + msg + " to out: " + out)
        // local send
        out.send(msg, replyCh)
      case _ => throw new IllegalArgumentException("Cannot handle command: " + m)
    }
  }

  override def !![A](msg: Any, handler: PartialFunction[Any, A]) = {
    // check connection, before starting a future actor
    // this allows us to give the same blocking semantics for !! as for other
    // methods, w.r.t. the connect policy
    conn(Some(Actor.self(scheduler)))
    super.!!(msg, handler)
  }

  override def send(msg: Any, sender: OutputChannel[Any]) {
    msg match {
      // local proxy receives response to
      // reply channel
      case ch ! resp =>
        Debug.info("ch ! resp: " + resp)
        // lookup session ID
        RemoteActor.finishSession(ch) match {
          case Some(sid) =>
            Debug.info(this + ": found session " + sid + " for channel " + ch)
            val msg = resp.asInstanceOf[AnyRef]

            //Debug.info("sender: " + sender)
            //Debug.info("sender.receiver: " + sender.receiver)

            // send back response - the sender (from) field is null here,
            // because you cannot reply to a request-response cycle more
            // than once.
            tryRemoteAction(None) { usingConn => 
              NetKernel.syncReply(usingConn, name.name, msg, sid.name, config)
            }
          case None =>
            Debug.error(this+": cannot find session for "+ch)
        }

      // this case is when a proxy object had !, !!, !?, or send called,
      // as in `proxy ! message`, where proxy is obtained from select() or
      // `sender`. From our perspective, the `sender` variable determines
      // which type of call it was
      case msg: AnyRef =>
        Debug.info("msg: AnyRef = " + msg)

        // find out whether it's a synchronous send
        sender match {
          case (_: Actor) | null =>  /** Comes from !, or send(m, null) */
            Debug.info(this + ": async send: sender: " + sender)
            tryRemoteAction(Option(sender.asInstanceOf[AbstractActor])) { usingConn =>
              NetKernel.asyncSend(usingConn, name.name, Option(sender.asInstanceOf[Actor]), msg, config)
            }
          case _ =>                  /** Comes from !! and !? */
            Debug.info(this + ": sync send: sender: " + sender)
            val session = RemoteActor.newChannel(sender)
            Debug.info(this + ": mapped " + session + " -> " + sender)
            tryRemoteAction(Some(sender.receiver)) { usingConn =>
              NetKernel.syncSend(usingConn, name.name, sender.receiver, msg, session.name, config)
            }
        }
      case e =>
        Debug.error("Unknown message for delegate: " + e)
    }
  }

  override def linkTo(to: AbstractActor) { 
    handleMessage(RemoteApply0(to, LinkToFun))
  }

  override def unlinkFrom(from: AbstractActor) {
    handleMessage(RemoteApply0(from, UnlinkFromFun))
  }

  override def exit(from: AbstractActor, reason: AnyRef) {
    val reason0 = reason match {
      case UncaughtException(_, message, _, _, cause) =>
        /* This is a bit of a hack. UncaughtException, which is thrown when
         * an actor fails to catch an exception, is not serializable. The
         * actor, possibly the sender, and the thread fields are all not
         * serializable. Therefore, set them to null (but keep the message and
         * cause fields, because those are probably the most useful) */
        UncaughtException(null, message, null, null, cause)
      case _ => reason
    }
    handleMessage(RemoteApply0(from, ExitFun(reason0)))
  }

  override def toString = "<" + name + "@" + remoteNode + ">"

} 

/**
 * This class represents a restartable, lazy configurable proxy. It is
 * serializable also
 */
@serializable
private[remote] class ConfigProxy(override val remoteNode: Node,
                                  override val name: Symbol,
                                  @transient _config: Configuration) extends Proxy {

  assert(_config ne null) /** _config != null from default ctor (NOT from serialized form though) */

  // save enough of the configuration to recreate it, if serialized. We don't
  // just serializer the Configuration, because Configuration instances are
  // most likely NOT serializable (since they can be created very ad-hoc-ly),
  // and also we don't need ALL the fields of the Configuration

  // TODO: these extra fields make proxies waste more space. write custom
  // read/write object serializers, so we don't have to save these values in
  // VALs

  private val _selectMode        = _config.selectMode
  private val _numRetries        = _config.numRetries
  private val _connectPolicy     = _config.connectPolicy
  private val _lookupValidPeriod = _config.lookupValidPeriod

  // IMPLEMENTATION limitation: can only reconstruct serializer remotely which
  // have a no-arg ctor
  private val _serializerClassName = _config.cachedSerializer.getClass.getName

  @transient @volatile
  private var _conn: MessageConnection = _

  override def conn(a: Option[AbstractActor]) = {
    val testConn = _conn
    if (testConn ne null) testConn
    else {

      val safeReactor =
        a.filter(_.isInstanceOf[Reactor[_]])
         .map(_.asInstanceOf[Reactor[Any]])

      // try to (re-)initialize connection from the NetKernel
      val tmpConn = NetKernel.getConnectionFor(remoteNode, safeReactor, config)

      if (config.connectPolicy == ConnectPolicy.NoWait ||
          config.connectPolicy == ConnectPolicy.WaitEstablished) {
        val errorCallback = (t: Throwable) => {
          // ignore ActorProxy because no way the user can define its
          // exceptionHandler
          safeReactor.filter(f => !f.isInstanceOf[ActorProxy]).foreach(reactor => {
            if (t.isInstanceOf[Exception]) {
              val ex = t.asInstanceOf[Exception]
              if (reactor.exceptionHandler.isDefinedAt(ex))
                reactor.exceptionHandler(ex)
            } else {
              Debug.error("Got unhandlable error (throwable): " + t)
              Debug.doError { t.printStackTrace() }
            }
          })
        }

        // if the connect policy is NoWait, have to register handlers for both
        // the connectFuture and the handshakeFuture
        if (config.connectPolicy == ConnectPolicy.NoWait) {
          tmpConn.connectFuture.notifyOnError(errorCallback)
          tmpConn.handshakeFuture.notifyOnError(errorCallback)
        }
        // if the connect policy is WaitEstablished, have to register handler
        // for only the handshakeFuture
        else {
          assert(tmpConn.connectFuture.isFinished)
          tmpConn.handshakeFuture.notifyOnError(errorCallback)
        }
      }

      // WaitHandshake and WaitEstablished don't need to regsiter any handlers
      // on the connection, since they already have blocked

      // now, check to see if a locate request for this actor has been made
      // already
      val cache = tmpConn.attachment_!.asInstanceOf[ConcurrentHashMap[Symbol, (Boolean, Long)]]

      val tuple = cache.get(name)

      val makeNewRequest = 
        (tuple eq null) || // no request ever made before
        (tuple._2 + config.lookupValidPeriod) <= System.currentTimeMillis // the look up has expired

      if (makeNewRequest) {
        NetKernel.locateRequest(tmpConn, 
                                name.name, 
                                safeReactor,
                                new CallbackFuture(() => {
                                  // on success, cache the lookup result, and
                                  // set _conn = tmpConn
                                  cache.put(name, (true, System.currentTimeMillis))
                                  _conn = tmpConn
                                },
                                (t: Throwable) => {
                                  // on error. cache the lookup result.  no
                                  // need to set _conn = null since we never sent it
                                  // to anything valid
                                  cache.put(name, (false, System.currentTimeMillis))
                                  Debug.error("Locate request failed: " + t.getMessage)
                                  Debug.doError { t.printStackTrace() }
                                }),
                                config)
      
        // NOTE: we don't set _conn = tmpConn here, because we still aren't
        // sure whether or not the connection is really valid for this proxy.
        tmpConn
      } else {
        if (tuple._1) {
          // valid lookup, use the result
          _conn = tmpConn
          tmpConn
        } else
          // oops, lookup resolved badly. throw an exception
          // YES, this means that connections with a policy of NoWait CAN get
          // immediate exceptions the second time around on a bad connection.
          // Oh well.
          throw new NoSuchRemoteActorException(name)
      }
    }
  }

  override lazy val config = 
    if (_config eq null) 
      new Configuration with HasDefaultMessageCreator {

        override val selectMode        = _selectMode
        override val aliveMode         = _selectMode /** Should never be called */
        override val numRetries        = _numRetries
        override val connectPolicy     = _connectPolicy
        override val lookupValidPeriod = _lookupValidPeriod

        override def newSerializer() = {
          // TODO: this needs to be sandboxed somehow
          val clz = Class.forName(_serializerClassName)
          if (classOf[Serializer].isAssignableFrom(clz)) {
            clz.asInstanceOf[Class[Serializer]].newInstance
          } else
            throw new ClassCastException(_serializerClassName)
        }
      }
    else 
      _config

  override def numRetries = 
    config.numRetries

  override def terminateConn(usingConn: MessageConnection) {
    usingConn.terminateBottom()
    _conn = null
  }

}

/**
 * This class is NOT serializable (since it is from a ephemeral connection).
 */
private[remote] class ConnectionProxy(override val remoteNode: Node,
                                      override val name: Symbol,
                                      _conn: MessageConnection) extends Proxy {

  @volatile private var _fail = false

  override def conn(a: Option[AbstractActor]) = {
    if (_fail) throw new ConnectionAlreadyClosedException
    else _conn
  }

  override val config = _conn.config

  override def numRetries = 0 /** Cannot retry */

  override def terminateConn(usingConn: MessageConnection) {
    usingConn.terminateBottom()
    _fail = true
  }

  private def writeObject(out: ObjectOutputStream) {
    throw new NotSerializableException(getClass.getName)
  }

  private def readObject(in: ObjectInputStream) {
    throw new NotSerializableException(getClass.getName)
  }
}

/**
 * Base class for all functions which can be remotely applied
 */
sealed abstract class RemoteFunction extends Function2[AbstractActor, Proxy, Unit]

/**
 * Remote application of `link`
 */
@serializable object LinkToFun extends RemoteFunction {
  def apply(target: AbstractActor, creator: Proxy) {
    target.linkTo(creator)
  }
  override def toString =
    "<LinkToFun>"
}

/**
 * Remote application of `unlink`
 */
@serializable object UnlinkFromFun extends RemoteFunction {
  def apply(target: AbstractActor, creator: Proxy) {
    target.unlinkFrom(creator)
  }
  override def toString =
    "<UnlinkFromFun>"
}

/**
 * Remote application of `exit`
 */
@serializable case class ExitFun(reason: AnyRef) extends RemoteFunction {
  def apply(target: AbstractActor, creator: Proxy) {
    target.exit(creator, reason)
  }
  override def toString =
    "<ExitFun>("+reason.toString+")"
}

// proxy commands
private[remote] sealed trait ProxyCommand
private[remote] case class SendTo(a: OutputChannel[Any], msg: Any) extends ProxyCommand
private[remote] case class StartSession(a: OutputChannel[Any], msg: Any, session: Symbol) extends ProxyCommand
private[remote] case class FinishSession(a: OutputChannel[Any], msg: Any, session: Symbol) extends ProxyCommand
private[remote] case class LocalApply0(rfun: RemoteFunction, a: AbstractActor) extends ProxyCommand
private[remote] case class RemoteApply0(actor: AbstractActor, rfun: RemoteFunction) extends ProxyCommand
