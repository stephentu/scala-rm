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
trait Proxy extends AbstractActor {

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

  @inline private def wrap(m: Any) = MFromProxy(this, m)

  def !(msg: Any): Unit =
    del ! wrap(msg) 

  def send(msg: Any, replyCh: OutputChannel[Any]): Unit =
    del.send(wrap(msg), replyCh)

  def forward(msg: Any): Unit =
    del.forward(wrap(msg))

  def receiver: Actor =
    del

  def !?(msg: Any): Any =
    del !? wrap(msg)

  def !?(msec: Long, msg: Any): Option[Any] =
    del !? (msec, wrap(msg))

  def !!(msg: Any): Future[Any] =
    del !! wrap(msg)

  def !![A](msg: Any, f: PartialFunction[Any, A]): Future[A] =
    del !! (wrap(msg), f)

  def linkTo(to: AbstractActor): Unit =
    del ! wrap(Apply0(to, new LinkToFun))

  def unlinkFrom(from: AbstractActor): Unit =
    del ! wrap(Apply0(from, new UnlinkFromFun))

  def exit(from: AbstractActor, reason: AnyRef): Unit =
    del ! wrap(Apply0(from, new ExitFun(reason)))

  override def toString = "<" + name + "@" + remoteNode + ">"

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

/**
 * Because the implementation of Channel simply forwards message directly
 * to the receiver, we have to manually override each of the methods to do a
 * wrap
 */
private[remote] class ProxyChannel(proxy: Proxy) extends Channel[Any](proxy.del) {
  @inline private def wrap(m: Any) = MFromProxy(proxy, m)
  override def !(msg: Any) { 
    super.!(wrap(proxy, msg))
  }
  override def send(msg: Any, replyTo: OutputChannel[Any]) {
    super.send(wrap(proxy, msg), replyTo)
  }
  override def forward(msg: Any) {
    super.forward(wrap(proxy, msg))
  }
  override def !?(msg: Any) =
    super.!?(wrap(proxy, msg))
  override def !?(msec: Long, msg: Any) =
    super.!?(msec, wrap(proxy, msg))
  override def !![A](msg: Any, f: PartialFunction[Any, A]) =
    super.!!(wrap(proxy, msg), f)
}

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
          case cmd @ MFromProxy(proxy, SendTo(out, msg, session)) =>
            Debug.info("cmd@SendTo: " + cmd)
            if (session.name == "nosession") {
              // local send
              out.send(msg, proxy) // use the proxy as the reply channel
            } else {
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
            }

          case cmd @ Terminate =>
            Debug.info("cmd@Terminate: terminating delegate actor for connection " + conn)
            exit()

          // local proxy receives response to
          // reply channel
          case ch ! MFromProxy(proxy, resp) =>
            Debug.info("ch ! resp")
            // lookup session ID
            sessionMap.get(ch) match {
              case Some(sid) =>
                Debug.info(this + ": found session " + sid + " for channel " + ch)
                sessionMap -= ch
                val msg = resp.asInstanceOf[AnyRef]
                // send back response
                kernel.forward(sender, conn, proxy.name, msg, sid)

              case None =>
                Debug.info(this+": cannot find session for "+ch)
            }

          // remote proxy receives request
          case MFromProxy(proxy, msg: AnyRef) =>
            Debug.info("msg: AnyRef = " + msg)

            // find out whether it's a synchronous send
            val sessionName = 
              if (sender.getClass.toString.contains("Channel")) { // how bout sender.isInstanceOf[Channel[_]] ?
                Debug.info(this + ": sync send: sender: " + sender)
                // create fresh session ID...
                val fresh = FreshNameCreator.newName(conn.remoteNode + "@" + proxy.name)
                // ...that maps to reply channel
                channelMap += Pair(fresh, sender)
                Debug.info(this + ": mapped " + fresh + " -> " + sender)
                fresh
              } else {
                Debug.info(this + ": async send: sender: " + sender)
                'nosession // TODO: use Option instead of some sentinel name
              }

            kernel.forward(sender, conn, proxy.name, msg, sessionName)
          case e =>
            Debug.error("Unknown message for delegate: " + e)
        }
    }
  }

  override def toString = "<DelegateActor for connection " + conn + ">"
}
