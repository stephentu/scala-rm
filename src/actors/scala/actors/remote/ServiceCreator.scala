/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

import java.io.IOException
import java.net.ServerSocket
import scala.collection.mutable.HashMap

trait ServiceCreator {

  type MyService <: Service
  private val ports = new HashMap[Int, MyService]

  protected def newService(port: Int, serializer: Serializer): MyService

  def apply(port: Int, serializer: Serializer = new JavaSerializer(RemoteActor.classLoader)): MyService =
    ports.synchronized {
      ports.get(port) match {
        case Some(service) =>
          if (service.serializer != serializer)
            throw new IllegalArgumentException("Cannot apply to ServiceCreator with different serializer")
          service
        case None =>
          val service        = newService(port, serializer)
          serializer.service = service
          ports             += Pair(port, service)
          service.start()
          Debug.info("created service: " + service)
          service
      }
    }

  def generatePort: Int = 
    try {
      val socket = new ServerSocket(0) // try any random port 
      val portNum = socket.getLocalPort
      socket.close()
      portNum
    } catch {
      case ioe: IOException =>
        // all ports taken?
        Debug.error("Could not generate a random port: " + ioe.getMessage)
        throw ioe
    }

}
