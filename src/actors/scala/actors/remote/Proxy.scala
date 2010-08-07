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

import java.util.WeakHashMap
import java.lang.ref.WeakReference
import java.io.{ ObjectInputStream, ObjectOutputStream, 
                 IOException, NotSerializableException }

/**
 * This class is necessary so that <code>sender.receiver</code> can return a
 * type of <code>actor</code> which properly responds to messages. This
 * implementation just delegates
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

  protected def conn: MessageConnection
  protected def terminateConn(usingConn: MessageConnection): Unit 
  protected def numRetries: Int   

  override def start() = { throw new RuntimeException("Should never call start() on a Proxy") }
  override def act()     { throw new RuntimeException("Should never call act() on a Proxy")   }

  private def tryRemoteAction(f: MessageConnection => Unit) {
    var triesLeft = numRetries + 1
    assert(triesLeft > 0)
    while (triesLeft > 0) {
      triesLeft -= 1
      val usingConn = conn
      try { f(usingConn) } catch {
        case e @ ((_: IOException) | (_: AlreadyTerminatedException)) =>
          Debug.error(this + ": Caught exception: " + e.getMessage)
          Debug.doError { e.printStackTrace() }
          terminateConn(usingConn)
          if (triesLeft == 0)
            throw e
      }
    }
  }

  private[remote] def handleMessage(m: ProxyCommand) {
    m match {
      case cmd @ RemoteApply0(actor, rfun) =>
        Debug.info("cmd@Apply0: " + cmd)
        tryRemoteAction { usingConn =>
          NetKernel.remoteApply(usingConn, name.name, RemoteActor.getOrCreateName(actor).name, rfun)
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

            Debug.info("sender: " + sender)
            Debug.info("sender.receiver: " + sender.receiver)

            // send back response - the sender (from) field is null here,
            // because you cannot reply to a request-response cycle more
            // than once.
            tryRemoteAction { usingConn => 
              NetKernel.syncReply(usingConn, name.name, msg, sid.name)
            }
          case None =>
            Debug.info(this+": cannot find session for "+ch)
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
            val fromName = if (sender eq null) None else Some(RemoteActor.getOrCreateName(sender))
            tryRemoteAction { usingConn =>
              NetKernel.asyncSend(usingConn, name.name, fromName.map(_.name), msg)
            }
          case _ =>                  /** Comes from !! and !? */
            Debug.info(this + ": sync send: sender: " + sender)
            val session = RemoteActor.newChannel(sender)
            Debug.info(this + ": mapped " + session + " -> " + sender)
            tryRemoteAction { usingConn =>
              NetKernel.syncSend(usingConn, name.name, RemoteActor.getOrCreateName(sender.receiver).name, msg, session.name)
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
    handleMessage(RemoteApply0(from, ExitFun(reason.toString)))
  }

  override def toString = "<" + name + "@" + remoteNode + ">"

} 

/**
 * This class explicitly defines <code>writeObject</code> and
 * <code>readObject</code> beacuse the <code>Actor</code> trait does not take
 * care to serialize properly
 */
@serializable
private[remote] class ConfigProxy(override val remoteNode: Node,
                                  override val name: Symbol,
                                  @transient config: Configuration) extends Proxy {

  assert(config ne null) /** config != null from default ctor (NOT from serialized form though) */

  // save enough of the configuration to recreate it, if seralized 

  private val _selectMode = config.selectMode
  private val _numRetries = config.numRetries
  // IMPLEMENTATION limitation: can only reconstruct serializer remotely which
  // have a no-arg ctor
  private val _serializerClassName = config.cachedSerializer.getClass.getName

  @transient @volatile
  private var _conn: MessageConnection = _

  override def conn = {
    val testConn = _conn
    if (testConn ne null) testConn
    else {
      synchronized {
        if (_conn ne null) _conn
        else {
          // try to (re-)initialize connection from the NetKernel
          _conn = NetKernel.getConnectionFor(remoteNode, getConfig)
          _conn
        }
      }
    }
  }

  private lazy val getConfig = 
    if (config eq null) 
      new Configuration {
        override val selectMode = _selectMode
        override val aliveMode  = _selectMode /** Shouldn't be used though */
        override val numRetries = _numRetries

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
      config

  override def numRetries = 
    getConfig.numRetries

  override def terminateConn(usingConn: MessageConnection) {
    usingConn.terminateBottom()
    synchronized { _conn = null }
  }

}

/**
 * This class is NOT serializable (since it is from a ephemeral connection).
 */
private[remote] class ConnectionProxy(override val remoteNode: Node,
                                      override val name: Symbol,
                                      _conn: MessageConnection) extends Proxy {

  @volatile private var _fail = false

  override def conn = {
    if (_fail)
      // TODO: change exception type
      throw new RuntimeException("Connection failed")
    else _conn
  }

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

sealed abstract class RemoteFunction extends Function2[AbstractActor, Proxy, Unit]

@serializable object LinkToFun extends RemoteFunction {
  def apply(target: AbstractActor, creator: Proxy) {
    target.linkTo(creator)
  }
  override def toString =
    "<LinkToFun>"
}

@serializable object UnlinkFromFun extends RemoteFunction {
  def apply(target: AbstractActor, creator: Proxy) {
    target.unlinkFrom(creator)
  }
  override def toString =
    "<UnlinkFromFun>"
}

@serializable case class ExitFun(reason: AnyRef) extends RemoteFunction {
  def apply(target: AbstractActor, creator: Proxy) {
    target.exit(creator, reason)
  }
  override def toString =
    "<ExitFun>("+reason.toString+")"
}

private[remote] sealed trait ProxyCommand
private[remote] case class SendTo(a: OutputChannel[Any], msg: Any) extends ProxyCommand
private[remote] case class StartSession(a: OutputChannel[Any], msg: Any, session: Symbol) extends ProxyCommand
private[remote] case class FinishSession(a: OutputChannel[Any], msg: Any, session: Symbol) extends ProxyCommand
private[remote] case class LocalApply0(rfun: RemoteFunction, a: AbstractActor) extends ProxyCommand
private[remote] case class RemoteApply0(actor: AbstractActor, rfun: RemoteFunction) extends ProxyCommand
