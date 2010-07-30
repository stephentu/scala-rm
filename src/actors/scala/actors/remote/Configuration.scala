/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

object Configuration {
  implicit object DefaultConfig extends DefaultConfiguration
}

abstract class Configuration {
  def aliveMode: ServiceMode.Value 
  def selectMode: ServiceMode.Value 
  def newSerializer(): Serializer

  private[remote] lazy val cachedSerializer: Serializer = newSerializer()
}

class DefaultConfiguration 
  extends Configuration
  with    HasJavaSerializer
  with    HasBlockingMode 

class DefaultNonBlockingConfiguration
  extends Configuration
  with    HasJavaSerializer
  with    HasNonBlockingMode 

trait HasJavaSerializer { this: Configuration =>
  override def newSerializer() = new JavaSerializer(RemoteActor.classLoader)
}

trait DefaultMessageCreator extends MessageCreator
                            with    DefaultEnvelopeMessageCreator 
                            with    DefaultControllerMessageCreator

trait HasBlockingMode { this: Configuration =>
  override def aliveMode  = ServiceMode.Blocking
  override def selectMode = ServiceMode.Blocking
}

trait HasNonBlockingMode { this: Configuration =>
  override def aliveMode  = ServiceMode.NonBlocking
  override def selectMode = ServiceMode.NonBlocking
}
