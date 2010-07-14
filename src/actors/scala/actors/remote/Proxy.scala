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
 * Proxy is a trait so that users of other serialization frameworks can define
 * their own implementation of the members, and pass proxy handles in messages
 * transparently.
 */
trait Proxy extends AbstractActor {
  def remoteNode: Node
  def mode: ServiceMode.Value
  def serializerClassName: String
  def name: Symbol

  @transient
  private[this] var _del: DelegateActor = _

  /**
   * Used by createProxy() in NetKernel to set the delegate actor accordingly.
   */
  private[remote] def del_=(delegate: DelegateActor) {
    synchronized {
      if (_del ne null) throw new IllegalStateException("Delegate is already set for: " + this)
      _del = delegate
    }
  }

  private[remote] def del: DelegateActor = synchronized {
    if (_del eq null) RemoteActor.netKernel.setupProxy(this) // this happens when a serializer instantiates
                                                             // a Proxy object (from a message for instance)
    assert(_del ne null)
    _del
  }

  private[remote] def newDelegate(conn: MessageConnection): DelegateActor =
    new DelegateActor(this, conn, name, RemoteActor.netKernel) 

  def !(msg: Any): Unit =
    del ! msg

  def send(msg: Any, replyCh: OutputChannel[Any]): Unit =
    del.send(msg, replyCh)

  def forward(msg: Any): Unit =
    del.forward(msg)

  def receiver: Actor =
    del

  def !?(msg: Any): Any =
    del !? msg

  def !?(msec: Long, msg: Any): Option[Any] =
    del !? (msec, msg)

  def !!(msg: Any): Future[Any] =
    del !! msg

  def !![A](msg: Any, f: PartialFunction[Any, A]): Future[A] =
    del !! (msg, f)

  def linkTo(to: AbstractActor): Unit =
    del ! Apply0(new LinkToFun)

  def unlinkFrom(from: AbstractActor): Unit =
    del ! Apply0(new UnlinkFromFun)

  def exit(from: AbstractActor, reason: AnyRef): Unit =
    del ! Apply0(new ExitFun(reason))

  override def toString = "<" + name + "@" + remoteNode + ">"

} 

import java.io.{ ObjectInputStream, ObjectOutputStream }

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
  
  override def remoteNode = _remoteNode
  override def mode = _mode
  override def serializerClassName = _serializerClassName
  override def name = _name

  private def writeObject(out: ObjectOutputStream) {
    out.writeObject(_remoteNode)
    out.writeObject(_mode)
    out.writeObject(_serializerClassName)
    out.writeObject(_name)
  }

  private def readObject(in: ObjectInputStream) {
    _remoteNode = in.readObject().asInstanceOf[Node]
    _mode = in.readObject().asInstanceOf[ServiceMode.Value]
    _serializerClassName = in.readObject().asInstanceOf[String]
    _name = in.readObject().asInstanceOf[Symbol]
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

private[remote] case class Apply0(rfun: Function2[AbstractActor, Proxy, Unit])

@serializable
private[remote] class DelegateChannel(proxy: Proxy) extends Channel[Any] {
  override val receiver = proxy.del
}

/**
 * @author Philipp Haller
 */
private[remote] class DelegateActor(creator: Proxy, val conn: MessageConnection, name: Symbol, kernel: NetKernel) extends Actor {
  var channelMap = new HashMap[Symbol, OutputChannel[Any]]
  var sessionMap = new HashMap[OutputChannel[Any], Symbol]

  override def exceptionHandler: PartialFunction[Exception, Unit] = {
    case e: Exception =>
      Debug.error(this + ": caught exception with message: " + e.getMessage)
      Debug.doError { e.printStackTrace }
  }

  /** 
   * Override linkTo and unlinkFrom so that linking to a delegate actor links
   * to the remote side. note: a delegate actor is exposed to the user via 
   * sender.receiver 
   */
  
  override def linkTo(to: AbstractActor): Unit =
    this ! Apply0(new LinkToFun)

  override def unlinkFrom(from: AbstractActor): Unit =
    this ! Apply0(new UnlinkFromFun)

  def act() {
    Actor.loop {
        react {
          case cmd@Apply0(rfun) =>
            Debug.info("cmd@Apply0: " + cmd)
            kernel.remoteApply(conn, name, sender, rfun)

          case cmd@LocalApply0(rfun, target) =>
            Debug.info("cmd@LocalApply0: " + cmd)
            Debug.info("target: " + target + ", creator: " + creator)
            rfun(target, creator)

          // Request from remote proxy.
          // `this` is local proxy.
          case cmd@SendTo(out, msg, session) =>
            Debug.info("cmd@SendTo: " + cmd)
            if (session.name == "nosession") {
              // local send
              out.send(msg, creator) // use the proxy as the reply channel
            } else {
              // is this an active session?
              channelMap.get(session) match {
                case None =>
                  Debug.info(this + ": creating new reply channel for session: " + session)

                  // create a new reply channel...
                  val replyCh = new DelegateChannel(creator)
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

          case cmd@Terminate =>
            Debug.info("cmd@Terminate: terminating delegate actor to node " + conn.remoteNode)
            exit()

          // local proxy receives response to
          // reply channel
          case ch ! resp =>
            Debug.info("ch ! resp")
            // lookup session ID
            sessionMap.get(ch) match {
              case Some(sid) =>
                Debug.info(this + ": found session " + sid + " for channel " + ch)
                sessionMap -= ch
                val msg = resp.asInstanceOf[AnyRef]
                // send back response
                kernel.forward(sender, conn, name, msg, sid)

              case None =>
                Debug.info(this+": cannot find session for "+ch)
            }

          // remote proxy receives request
          case msg: AnyRef =>
            Debug.info("msg: AnyRef = " + msg)

            // find out whether it's a synchronous send
            val sessionName = 
              if (sender.getClass.toString.contains("Channel")) { // how bout sender.isInstanceOf[Channel[_]] ?
                Debug.info(this + ": sync send: sender: " + sender)
                // create fresh session ID...
                val fresh = FreshNameCreator.newName(conn.remoteNode+"@"+name)
                // ...that maps to reply channel
                channelMap += Pair(fresh, sender)
                Debug.info(this + ": mapped " + fresh + " -> " + sender)
                fresh
              } else {
                Debug.info(this + ": async send: sender: " + sender)
                'nosession 
              }

            kernel.forward(sender, conn, name, msg, sessionName)
        }
    }
  }

  override def toString = "<DelegateActor for: " + creator + ">"
}
