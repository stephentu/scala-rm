/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

import scala.actors._
import Actor._
import remote._
import RemoteActor._

import java.io.IOException

object RemoteStartInvoke {
  def apply(actorClass: String): RemoteStartInvoke = 
    DefaultRemoteStartInvokeImpl(actorClass)
  def unapply(r: RemoteStartInvoke): Option[(String)] = Some((r.actorClass))
}

trait RemoteStartInvoke {
  def actorClass: String
}

case class DefaultRemoteStartInvokeImpl(override val actorClass: String) extends RemoteStartInvoke

object RemoteStartInvokeAndListen {
  def apply(actorClass: String, port: Int, name: Symbol): RemoteStartInvokeAndListen =
    DefaultRemoteStartInvokeAndListenImpl(actorClass, port, name)
  def unapply(r: RemoteStartInvokeAndListen): Option[(String, Int, Symbol)] = 
    Some((r.actorClass, r.port, r.name))
}

trait RemoteStartInvokeAndListen {
  def actorClass: String
  def port: Int
  def name: Symbol
}

case class DefaultRemoteStartInvokeAndListenImpl(override val actorClass: String,
                                                 override val port: Int,
                                                 override val name: Symbol)
  extends RemoteStartInvokeAndListen

object RemoteStartResult {
  def apply(errorMessage: Option[String]): RemoteStartResult = 
    DefaultRemoteStartResultImpl(errorMessage)
  def unapply(r: RemoteStartResult): Option[(Option[String])] =
    Some((r.errorMessage))
}

trait RemoteStartResult {
  def success: Boolean = errorMessage.isEmpty
  def errorMessage: Option[String]
}

case class DefaultRemoteStartResultImpl(override val errorMessage: Option[String]) extends RemoteStartResult

object ControllerActor {
  val defaultPort = 11723 
  val defaultMode = ServiceMode.Blocking
}

private[remote] class ControllerActor(port: Int, thisSym: Symbol)(implicit cfg: Configuration) extends Actor {

  import ControllerActor._

  // TODO: let user specify class loader
  private def newActor(actorClass: String): Actor =
    Class.forName(actorClass).asInstanceOf[Class[Actor]].newInstance()

  override def exceptionHandler: PartialFunction[Exception, Unit] = {
    case e: Exception =>
      Debug.error(this + ": Caught exception: " + e.getMessage)
      Debug.doError { e.printStackTrace() }
  }

  override def act() {
    try {
      // call alive0 so that this actor doesn't prevent shutdown
      alive0(port, self, false)
    } catch { 
      case e: IOException =>
        // oops, the specified port is already taken
        Debug.error(this + ": Could not listen on port: " + port)
        Debug.doError { e.printStackTrace() }
        exit()
    }
    register(thisSym, self)
    Debug.info(this + ": started")
    loop {
      react {
        case r @ RemoteStartInvokeAndListen(actorClass, port, name) =>
          /** Assume actor class does not set itself up, and we need to register it */
          val errorMessage = 
            try {
              alive(port)
              val actor = newActor(actorClass)
              register(name, actor) 
              actor.start()
              None
            } catch {
              case e: Exception => Some(e.getMessage)
            }
          sender ! RemoteStartResult(errorMessage)
        case r @ RemoteStartInvoke(actorClass) =>
          /** Just invoke actor class, assume it sets itself up  */
          val errorMessage = 
            try {
              val a = newActor(actorClass) 
              a.start()
              None
            } catch {
              case e: Exception => 
                Some(e.getMessage)
            }
          sender ! RemoteStartResult(errorMessage)
        case Terminate =>
          Debug.info(this + ": Got terminate message")
          exit()
        case m =>
          Debug.info(this + ": Ignoring unknown message: " + m)
      }
    }
  }

  start() // ctor starts

  override def toString = "<ControllerActor>"
}
