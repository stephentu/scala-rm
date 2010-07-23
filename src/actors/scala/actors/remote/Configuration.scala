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
  implicit object defaultConfig extends DefaultConfiguration
}

abstract class Configuration[+P <: Proxy] {
  def aliveMode: ServiceMode.Value 
  def selectMode: ServiceMode.Value 
  def newSerializer(): Serializer[P]
}

class DefaultConfiguration extends Configuration[DefaultProxyImpl] {
  override def aliveMode       = ServiceMode.Blocking
  override def selectMode      = ServiceMode.Blocking
  override def newSerializer() = new JavaSerializer(RemoteActor.classLoader)
}
