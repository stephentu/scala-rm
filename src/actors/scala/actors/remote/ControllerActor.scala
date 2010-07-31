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
  def apply(actorClass: String, port: Int, name: Symbol, mode: ServiceMode.Value): RemoteStartInvokeAndListen =
    DefaultRemoteStartInvokeAndListenImpl(actorClass, port, name, mode)
  def unapply(r: RemoteStartInvokeAndListen): Option[(String, Int, Symbol, ServiceMode.Value)] = 
    Some((r.actorClass, r.port, r.name, r.mode))
}

trait RemoteStartInvokeAndListen {
  def actorClass: String
  def port: Int
  def name: Symbol
  def mode: ServiceMode.Value
}

case class DefaultRemoteStartInvokeAndListenImpl(override val actorClass: String,
                                                 override val port: Int,
                                                 override val name: Symbol,
                                                 override val mode: ServiceMode.Value) 
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

private[remote] class ControllerActor(thisSym: Symbol) extends Actor {

  import ControllerActor._

  private def getProperty(prop: String) = Option(System.getProperty(prop))

  private def port = getProperty("scala.actors.remote.controller.port")
  private def mode = getProperty("scala.actors.remote.controller.mode")

  private def getPort = port match {
    case Some(s) => 
      try { s.toInt } catch { case _ =>
        Debug.error(this + ": Bad port specified: " + port)
        defaultPort
      }
    case None => defaultPort
  }

  private def getMode = mode match {
    case Some(m) =>
      if (m.equalsIgnoreCase("blocking"))
        ServiceMode.Blocking
      else if (m.equalsIgnoreCase("nonblocking"))
        ServiceMode.NonBlocking
      else {
        Debug.error(this + ": Bad mode specified: " + mode)
        defaultMode
      }
    case None => defaultMode
  }

  // TODO: let user specify class loader
  private def newActor(actorClass: String): Actor =
    Class.forName(actorClass).asInstanceOf[Class[Actor]].newInstance()

  override def exceptionHandler: PartialFunction[Exception, Unit] = {
    case e: Exception =>
      Debug.error(this + ": Caught exception: " + e.getMessage)
      Debug.doError { e.printStackTrace() }
  }

  override def act() {
		implicit val cfg = new DefaultConfiguration {
			override def aliveMode = getMode
		}
    try {
      // call alive0 so that this actor doesn't prevent shutdown
      alive0(getPort, self, false)
    } catch { 
      case e: IOException =>
        // oops, the specified port is already taken
        Debug.error(this + ": Could not listen on port: " + getPort)
        Debug.doError { e.printStackTrace() }
        exit()
    }
    register(thisSym, self)
    Debug.info(this + ": started")
    loop {
      react {
        case r @ RemoteStartInvokeAndListen(actorClass, port, name, mode) =>
          /** Assume actor class does not set itself up, and we need to register it */
          val errorMessage = 
            try {
							implicit val cfg = new DefaultConfiguration {
								override def aliveMode = mode
							}
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
