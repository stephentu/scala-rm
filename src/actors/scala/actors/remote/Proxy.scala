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
import java.io.{ ObjectInputStream, ObjectOutputStream }


/**
 * Proxy is a trait so that users of other serialization frameworks can define
 * their own implementation of the members, and pass proxy handles in messages
 * transparently.
 */
trait Proxy extends Actor {

  /**
   * Target node of this proxy
   */
  def remoteNode: Node

  /**
   * Name of the (remote) actor which is the target of this proxy
   */
  def name: Symbol

  @volatile @transient
  private[this] var _cfg: Configuration[Proxy] =  
    RemoteActor.configuration
  private[remote] def config_=(cfg: Configuration[Proxy]) {
    _cfg = cfg
  }
  def config = _cfg

  override def start() = { throw new RuntimeException("Should never call start() on a ProxyActor") }
  override def act()     { throw new RuntimeException("Should never call act() on a ProxyActor")   }

  def handleMessage(m: Any) {
    m match {
      case cmd @ Apply0(actor, rfun) =>
        Debug.info("cmd@Apply0: " + cmd)
        NetKernel.remoteApply(remoteNode, name, actor, rfun, config)
      case cmd @ LocalApply0(rfun, target) =>
        Debug.info("cmd@LocalApply0: " + cmd)
        Debug.info("target: " + target + ", creator: " + this)
        rfun(target, this)
      // Request from remote proxy.
      // `this` is local proxy.
      case cmd @ SendTo(out, msg, None) =>
        Debug.info("cmd@SendTo: " + cmd)
        // local send
        out.send(msg, this) // use the proxy as the reply channel
      case cmd @ SendTo(out, msg, Some(session)) =>
        Debug.info("cmd@SendTo: " + cmd)
        // is this an active session?
        RemoteActor.finishChannel(session) match {
          case None =>
            Debug.info(this + ": creating new reply channel for session: " + session)

            // create a new reply channel...
            val replyCh = new Channel[Any](this)
            // ...that maps to session
            RemoteActor.startSession(replyCh, session)

            Debug.info(this + ": sending msg " + msg + " to out: " + out)
            // local send
            out.send(msg, replyCh)

          // finishes request-reply cycle
          case Some(replyCh) =>
            assert(name == Symbol("$$NoSender$$"))
            Debug.info(this + ": finishing request-reply cycle for session: " + session + " on replyCh " + replyCh)
            replyCh ! msg
        }

    }
  }

  override def send(msg: Any, replyCh: OutputChannel[Any]) {
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
            kernel.forward(remoteNode, name, null, Some(sid), msg, config)

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
        val sessionName = sender match {
          case (_: Actor) | null =>  /** Comes from !, or send(m, null) */
            Debug.info(this + ": async send: sender: " + sender)
            None
          case _ =>                  /** Comes from !! and !? */
            Debug.info(this + ": sync send: sender: " + sender)
            val fresh = RemoteActor.newChannel(sender)
            Debug.info(this + ": mapped " + fresh + " -> " + sender)
            Some(fresh)
        }

        //Debug.info("sender.receiver: " + sender.receiver) 
        //Debug.info("proxy.name: " + proxy.name)
        kernel.forward(remoteNode, name, if (sender eq null) null else sender.receiver, sessionName, msg, config)
      case e =>
        Debug.error("Unknown message for delegate: " + e)

    }


  }

  override def linkTo(to: AbstractActor): Unit = { }

  override def unlinkFrom(from: AbstractActor): Unit = { }

  override def exit(from: AbstractActor, reason: AnyRef): Unit = { }

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
                       var _name: Symbol) extends Proxy {
  
  override def remoteNode          = _remoteNode
  override def name                = _name

  private def writeObject(out: ObjectOutputStream) {
    out.writeObject(_remoteNode)
    out.writeObject(_name)
  }

  private def readObject(in: ObjectInputStream) {
    _remoteNode          = in.readObject().asInstanceOf[Node]
    _name                = in.readObject().asInstanceOf[Symbol]
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

private[remote] case class Apply0(actor: AbstractActor, rfun: RemoteFunction)
