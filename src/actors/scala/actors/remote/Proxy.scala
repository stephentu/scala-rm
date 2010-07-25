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

import java.io.{ ObjectInputStream, ObjectOutputStream }

/**
 * This is a message forwarded from a proxy to the connection's delegate
 * The delegate uses this class to derive context in handling a message
 */
case class MFromProxy(proxy: Proxy, msg: Any)

/**
 * Proxy is a trait so that users of other serialization frameworks can define
 * their own implementation of the members, and pass proxy handles in messages
 * transparently.
 */
trait Proxy extends AbstractActor 
            with    AbstractProxyWrapper {

  /**
   * Target node of this proxy
   */
  def remoteNode: Node

  /**
   * Name of the (remote) actor which is the target of this proxy
   */
  def name: Symbol

  /**
   * Method of remote connection 
   */
  def mode: ServiceMode.Value
  
  /**
   * Serializer class name used in this connection
   */
  def serializerClassName: String


  @transient
  @volatile
  private[this] var _del: DelegateActor = _

  /**
   * Only to be used by NetKernel
   */
  private[remote] def del_=(delegate: DelegateActor) {
    assert(delegate ne null)
    synchronized {
      if (_del ne null)
        throw new IllegalStateException("cannot assign del more than once")
      assert(delegate ne null)
      assert(delegate.getState != Actor.State.New)
      assert(delegate.getState != Actor.State.Terminated)
      _del = delegate
    }
  }

  private[remote] def del: DelegateActor = 
    if (_del ne null) _del
    else
      synchronized {
        if (_del eq null) { // check again
          val newDel = RemoteActor.netKernel.delegateFor(this)
          assert(newDel ne null)
          assert(newDel.getState != Actor.State.New)
          assert(newDel.getState != Actor.State.Terminated)
          _del = newDel
        }
        _del
      }

  override def proxy    = this
  override def target   = del
  override def toString = "<" + name + "@" + remoteNode + ">"

  override def receiver = new ProxyActor(this)
} 

trait AbstractProxyWrapper { _: AbstractActor =>
  protected def proxy: Proxy
  protected def target: Actor

  @inline private def wrap(m: Any) = MFromProxy(proxy, m)

  override def !(msg: Any): Unit =
    target ! wrap(msg) 

  override def send(msg: Any, replyCh: OutputChannel[Any]): Unit =
    target.send(wrap(msg), replyCh)

  override def forward(msg: Any): Unit =
    target.forward(wrap(msg))

  override def !?(msg: Any): Any =
    target !? wrap(msg)

  override def !?(msec: Long, msg: Any): Option[Any] =
    target !? (msec, wrap(msg))

  override def !!(msg: Any): Future[Any] =
    target !! wrap(msg)

  override def !![A](msg: Any, f: PartialFunction[Any, A]): Future[A] =
    target !! (wrap(msg), f)

  override def linkTo(to: AbstractActor): Unit =
    target ! wrap(Apply0(to, new LinkToFun))

  override def unlinkFrom(from: AbstractActor): Unit =
    target ! wrap(Apply0(from, new UnlinkFromFun))

  override def exit(from: AbstractActor, reason: AnyRef): Unit =
    target ! wrap(Apply0(from, new ExitFun(reason)))

}

/**
 * TODO: Unfortunately, AbstractProxyWrapper cannot be mixed-in with Actor, so
 * for now copy and paste the methods.
 */
class ProxyActor(proxy: Proxy) extends Actor {
  override def start() = { throw new RuntimeException("Should never call start() on a ProxyActor") }
  override def act()     { throw new RuntimeException("Should never call run() on a ProxyActor")   }

  @inline private def wrap(m: Any): Any = MFromProxy(proxy, m)

  private def target = proxy.del

  override def !(msg: Any): Unit =
    target ! wrap(msg) 

  override def send(msg: Any, replyCh: OutputChannel[Any]): Unit =
    target.send(wrap(msg), replyCh)

  override def forward(msg: Any): Unit =
    target.forward(wrap(msg))

  override def !?(msg: Any): Any =
    target !? wrap(msg)

  override def !?(msec: Long, msg: Any): Option[Any] =
    target !? (msec, wrap(msg))

  override def !!(msg: Any): Future[Any] =
    target !! wrap(msg)

  override def !![A](msg: Any, f: PartialFunction[Any, A]): Future[A] =
    target !! (wrap(msg), f)

  override def linkTo(to: AbstractActor): Unit =
    target ! wrap(Apply0(to, new LinkToFun))

  override def unlinkFrom(from: AbstractActor): Unit =
    target ! wrap(Apply0(from, new UnlinkFromFun))

  override def exit(from: AbstractActor, reason: AnyRef): Unit =
    target ! wrap(Apply0(from, new ExitFun(reason)))
}

/**
 * Note: This class defines readObject and writeObject because flagging
 * the _del field in the Proxy trait is not sufficient to prevent
 * Java serialization from trying to serialize _del when it's not null.
 * Therefore, we do it manually.
 *
 * TODO: fix this if possible
 */
@serializable
class DefaultProxyImpl(var _remoteNode: Node,
                       var _mode: ServiceMode.Value,
                       var _serializerClassName: String,
                       var _name: Symbol) extends Proxy {
  
  override def remoteNode          = _remoteNode
  override def mode                = _mode
  override def serializerClassName = _serializerClassName
  override def name                = _name

  private def writeObject(out: ObjectOutputStream) {
    out.writeObject(_remoteNode)
    out.writeObject(_mode)
    out.writeObject(_serializerClassName)
    out.writeObject(_name)
  }

  private def readObject(in: ObjectInputStream) {
    _remoteNode          = in.readObject().asInstanceOf[Node]
    _mode                = in.readObject().asInstanceOf[ServiceMode.Value]
    _serializerClassName = in.readObject().asInstanceOf[String]
    _name                = in.readObject().asInstanceOf[Symbol]
  }

}

@serializable
class LinkToFun extends Function2[AbstractActor, Proxy, Unit] {
  def apply(target: AbstractActor, creator: Proxy) {
    target.linkTo(creator)
  }
  override def toString =
    "<LinkToFun>"
}

@serializable
class UnlinkFromFun extends Function2[AbstractActor, Proxy, Unit] {
  def apply(target: AbstractActor, creator: Proxy) {
    target.unlinkFrom(creator)
  }
  override def toString =
    "<UnlinkFromFun>"
}

@serializable
class ExitFun(reason: AnyRef) extends Function2[AbstractActor, Proxy, Unit] {
  def apply(target: AbstractActor, creator: Proxy) {
    target.exit(creator, reason)
  }
  override def toString =
    "<ExitFun>("+reason.toString+")"
}

private[remote] case class Apply0(actor: AbstractActor, rfun: Function2[AbstractActor, Proxy, Unit])

private[remote] class ProxyChannel(proxy: Proxy) 
  extends Channel[Any](new ProxyActor(proxy))

/**
 * Each MessageConnection instance will have its own DelegateActor
 *
 * @author Philipp Haller
 */
private[remote] class DelegateActor(conn: MessageConnection, kernel: NetKernel) extends Actor {

  private val channelMap = new HashMap[Symbol, OutputChannel[Any]]
  private val sessionMap = new HashMap[OutputChannel[Any], Symbol]

  override def exceptionHandler: PartialFunction[Exception, Unit] = {
    case e: Exception =>
      Debug.error(this + ": caught exception with message: " + e.getMessage)
      Debug.doError { e.printStackTrace }
  }

  def act() {
    Actor.loop {
        react {
          case cmd @ MFromProxy(proxy, Apply0(actor, rfun)) =>
            Debug.info("cmd@Apply0: " + cmd)
            kernel.remoteApply(conn, proxy.name, actor, rfun)

          case cmd @ MFromProxy(proxy, LocalApply0(rfun, target)) =>
            Debug.info("cmd@LocalApply0: " + cmd)
            Debug.info("target: " + target + ", creator: " + proxy)
            rfun(target, proxy)

          // Request from remote proxy.
          // `this` is local proxy.
          case cmd @ MFromProxy(proxy, SendTo(out, msg, None)) =>
            Debug.info("cmd@SendTo: " + cmd)
            // local send
            out.send(msg, proxy) // use the proxy as the reply channel

          case cmd @ MFromProxy(proxy, SendTo(out, msg, Some(session))) =>
            Debug.info("cmd@SendTo: " + cmd)
            // is this an active session?
            channelMap.get(session) match {
              case None =>
                Debug.info(this + ": creating new reply channel for session: " + session)

                // create a new reply channel...
                val replyCh = new ProxyChannel(proxy)
                // ...that maps to session
                sessionMap += Pair(replyCh, session)
                // local send
                out.send(msg, replyCh)

              // finishes request-reply cycle
              case Some(replyCh) =>
                Debug.info(this + ": finishing request-reply cycle for session: " + session + " on replyCh " + replyCh)
                channelMap -= session
                replyCh ! msg
            }

          case cmd @ Terminate =>
            Debug.info("cmd@Terminate: terminating delegate actor for connection " + conn)
            exit()

          // local proxy receives response to
          // reply channel
          case MFromProxy(proxy, ch ! resp) =>
            Debug.info("ch ! resp: " + resp)
            // lookup session ID
            sessionMap.get(ch) match {
              case Some(sid) =>
                Debug.info(this + ": found session " + sid + " for channel " + ch)
                sessionMap -= ch
                val msg = resp.asInstanceOf[AnyRef]
                // send back response
                kernel.forward(sender, conn, proxy.name, msg, Some(sid))

              case None =>
                Debug.info(this+": cannot find session for "+ch)
            }

          // remote proxy receives request
          case MFromProxy(proxy, msg: AnyRef) =>
            Debug.info("msg: AnyRef = " + msg)
            // find out whether it's a synchronous send
            val sessionName = sender match {
              case (_: Actor) | null =>  /** Comes from !, or send(m, null) */
                Debug.info(this + ": async send: sender: " + sender)
                None
              case _ =>                  /** Comes from !! and !? */
                Debug.info(this + ": sync send: sender: " + sender)
                // create fresh session ID...
                val fresh = FreshNameCreator.newName(conn.remoteNode + "@" + proxy.name)
                // ...that maps to reply channel
                channelMap += Pair(fresh, sender)
                Debug.info(this + ": mapped " + fresh + " -> " + sender)
                Some(fresh)
            }

            kernel.forward(sender, conn, proxy.name, msg, sessionName)
          case e =>
            Debug.error("Unknown message for delegate: " + e)
        }
    }
  }

  override def toString = "<DelegateActor for connection " + conn + ">"
}
