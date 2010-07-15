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

case class RemoteStart(actorClass: String,
                       port: Int,
                       name: Symbol,
                       serializerClass: Option[String])

private[scala] object RemoteStartActor extends Actor {

  private[remote] val port = System.getProperty("scala.actors.remote.listener.port")
  private val serializerClass = System.getProperty("scala.actors.remote.listener.serializerClass")

  private val defaultPort = 9000
  private def getPort = 
    try {
      port.toInt
    } catch {
      case _ =>
        Debug.info(this + ": Bad port specified: " + port)
        defaultPort
    }

  private def newSerializer: Serializer[Proxy] = newSerializer(serializerClass)

  private def newSerializer(serializerClass: String): Serializer[Proxy] = 
    try {
      if (serializerClass eq null)
        RemoteActor.defaultSerializer
      else if (!classOf[Serializer[Proxy]].isAssignableFrom(Class.forName(serializerClass))) 
        RemoteActor.defaultSerializer
      else
        Class.forName(serializerClass).asInstanceOf[Class[Serializer[Proxy]]].newInstance
    } catch {
      case e =>
        Debug.error("Could not initialize serializer: " + serializerClass + ". Using default instead")
        e.printStackTrace
        RemoteActor.defaultSerializer
    }

  private def newActor(actorClass: String): Actor = {
    Class.forName(actorClass).asInstanceOf[Class[Actor]].newInstance
  }

  def act {
    //alive(getPort)
    //register('remoteStartActor, this)
    //Debug.info(this + ": started")
    //loop {
    //  react {
    //    case r @ RemoteStart(actorClass, port, name, serviceFactory, serializerClass) =>
    //      Debug.info(this + ": remote start message: " + r)
    //      try {
    //        val actor = newActor(actorClass) 
    //        val serializer = serializerClass match {
    //          case Some(clz) => newSerializer(clz)
    //          case None      => RemoteActor.defaultSerializer
    //        }
    //        val factory = serviceFactory.getOrElse(TcpServiceFactory) 
    //        val kernel = RemoteActor.createNetKernelOnPort(actor, port, serializer, factory)
    //        kernel.register(name, actor)
    //        Debug.info(this + ": Starting new actor: " + actor)
    //        actor.start()
    //      } catch {
    //        case e =>
    //          Debug.error(this + ": Error on remote start: " + e.getMessage)
    //      }
    //    case Terminate =>
    //      Debug.info(this + ": Got terminate message")
    //      exit()
    //    case m =>
    //      Debug.info(this + ": Ignoring unknown message: " + m)
    //  }
    //}
  }

  override def toString = "<RemoteStartActor>"
}

