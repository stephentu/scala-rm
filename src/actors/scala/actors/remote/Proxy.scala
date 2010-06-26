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
 * @author Philipp Haller
 */
@serializable
private[remote] class Proxy(node: Node, name: Symbol, @transient var kernel: NetKernel) extends AbstractActor {

  @transient
  private[remote] var del: Actor = null
  startDelegate()

  private def startDelegate() {
    del = new DelegateActor(this, node, name, kernel)
    del.start()
  }

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

  override def toString() =
    name+"@"+node
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

/**
 * @author Philipp Haller
 */
private[remote] class DelegateActor(creator: Proxy, node: Node, name: Symbol, kernel: NetKernel) extends Actor {
  var channelMap = new HashMap[Symbol, OutputChannel[Any]]
  var sessionMap = new HashMap[OutputChannel[Any], Symbol]

  private def safely(f: => Unit) {
    try {
      f
    } catch {
      case t: Throwable =>
        Debug.error(this + ": caught " + t.getMessage + " in react loop")
        t.printStackTrace
    }
  }

  def act() {
    Actor.loop {
        react {
          case cmd@Apply0(rfun) =>
            Debug.info("cmd@Apply0: " + cmd)
            safely { kernel.remoteApply(node, name, sender, rfun) }

          case cmd@LocalApply0(rfun, target) =>
            Debug.info("cmd@LocalApply0: " + cmd)
            safely { rfun(target, creator) }

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
                  // create a new reply channel...
                  val replyCh = new Channel[Any](this) // TODO: change this to wrap the proxy
                  // ...that maps to session
                  sessionMap += Pair(replyCh, session)
                  // local send
                  out.send(msg, replyCh)

                // finishes request-reply cycle
                case Some(replyCh) =>
                  channelMap -= session
                  replyCh ! msg
              }
            }

          case cmd@Terminate =>
            Debug.info("cmd@Terminate: terminating delegate actor to node " + node)
            exit()

          // local proxy receives response to
          // reply channel
          case ch ! resp =>
            Debug.info("ch ! resp")
            // lookup session ID
            sessionMap.get(ch) match {
              case Some(sid) =>
                sessionMap -= ch
                val msg = resp.asInstanceOf[AnyRef]
                // send back response
                safely { kernel.forward(sender, node, name, msg, sid) }

              case None =>
                Debug.info(this+": cannot find session for "+ch)
            }

          // remote proxy receives request
          case msg: AnyRef =>
            Debug.info("msg: AnyRef = " + msg)

            // find out whether it's a synchronous send
            val sessionName = 
              if (sender.getClass.toString.contains("Channel")) { // how bout sender.isInstanceOf[OutputChannel[_]] ?
                // create fresh session ID...
                val fresh = FreshNameCreator.newName(node+"@"+name)
                // ...that maps to reply channel
                channelMap += Pair(fresh, sender)

                fresh
              } else 'nosession 

            safely { kernel.forward(sender, node, name, msg, sessionName) }
        }
    }
  }

}
