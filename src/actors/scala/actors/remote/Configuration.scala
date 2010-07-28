/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

abstract class Configuration[+P <: Proxy] {
  def aliveMode: ServiceMode.Value 
  def selectMode: ServiceMode.Value 
  def newSerializer(): Serializer
	def messageCreator: MessageCreator[P]

  private[remote] lazy val cachedSerializer = newSerializer()
}

class DefaultConfiguration 
  extends HasJavaSerializer
	with    DefaultMessageCreator
  with    HasBlockingAlive 
  with    HasBlockingSelect

object DefaultConfig extends DefaultConfiguration

trait HasJavaSerializer { this: Configuration[_] =>
  override def newSerializer() = new JavaSerializer(RemoteActor.classLoader)
}

trait HasDefaultMessageCreator { this: Configuration[DefaultProxyImpl] =>
	override def messageCreator = DefaultMessageCreator
}

object DefaultMessageCreator extends MessageCreator[DefaultProxyImpl]
														 with    DefaultProxyCreator 
                             with    DefaultEnvelopeMessageCreator 
                             with    DefaultControllerMessageCreator

trait HasBlockingAlive { this: Configuration[_] =>
  override def aliveMode = ServiceMode.Blocking
}

trait HasBlockingSelect { this: Configuration[_] =>
  override def selectMode = ServiceMode.Blocking
}

trait HasNonBlockingAlive { this: Configuration[_] =>
  override def aliveMode = ServiceMode.NonBlocking
}

trait HasNonBlockingSelect { this: Configuration[_] =>
  override def selectMode = ServiceMode.NonBlocking
}
