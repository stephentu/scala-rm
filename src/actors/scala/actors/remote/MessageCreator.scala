/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

/**
 * This trait defines factory methods for some control messages used by the
 * network kernel. Currently, these control messages are used only for the
 * remote starting service, to allow users to run this service which send messages
 * in their serializer of choice.
 *
 * @see RemoteStartInvoke
 * @see RemoteStartInvokeAndListen
 * @see RemoteStartResult
 */
trait MessageCreator {

  def newRemoteStartInvoke(actorClass: String): AnyRef

  def newRemoteStartInvokeAndListen(actorClass: String, port: Int, name: String): AnyRef

  def newRemoteStartResult(errorMessage: Option[String]): AnyRef

}

/**
 * Provides implementations of each of the remote start actor messages, as standard
 * Scala case classes.
 */
class DefaultMessageCreator extends MessageCreator { 

  override def newRemoteStartInvoke(actorClass: String) = 
    DefaultRemoteStartInvokeImpl(actorClass)

  override def newRemoteStartInvokeAndListen(actorClass: String, port: Int, name: String) =
    DefaultRemoteStartInvokeAndListenImpl(actorClass, port, name)

  override def newRemoteStartResult(errorMessage: Option[String]) =
    DefaultRemoteStartResultImpl(errorMessage)
}
