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


private[remote] object ControllerActor {
  val defaultPort = 11723 
  val defaultMode = ServiceMode.Blocking
}

/**
 * The remote starting service is just implemented as a remote actor.
 */
private[remote] class ControllerActor(val port: Int, thisSym: Symbol)(implicit cfg: Configuration) extends Actor {

  import ControllerActor._

  private val messageCreator = cfg.newMessageCreator()

  private def newActor(actorClass: String): Actor = {
    val clz = Class.forName(actorClass, true, cfg.classLoader)
    if (classOf[Actor].isAssignableFrom(clz)) {
      clz.asInstanceOf[Class[Actor]].newInstance()
    } else
      throw new ClassCastException(actorClass)
  }

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
              register(Symbol(name), actor) 
              actor.start()
              None
            } catch {
              case e: Exception => Some(e.getMessage)
            }
          sender ! messageCreator.newRemoteStartResult(errorMessage)
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
          sender ! messageCreator.newRemoteStartResult(errorMessage)
        case Terminate =>
          Debug.info(this + ": Got terminate message")
          unregister(self) /** Explicit unregistration because we use alive0 */
          exit()
        case m =>
          Debug.info(this + ": Ignoring unknown message: " + m)
      }
    }
  }

  start() // ctor starts

  override def toString = "<ControllerActor>"
}
