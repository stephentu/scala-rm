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
 */
trait MessageCreator {

  def newRemoteStartInvoke(actorClass: String): AnyRef

  def newRemoteStartInvokeAndListen(actorClass: String, port: Int, name: String): AnyRef

  def newRemoteStartResult(errorMessage: Option[String]): AnyRef

  protected def doIntercept: PartialFunction[AnyRef, AnyRef] = {
    case RemoteStartInvoke(actorClass) => 
      newRemoteStartInvoke(actorClass)
    case RemoteStartInvokeAndListen(actorClass, port, name) =>
      newRemoteStartInvokeAndListen(actorClass, port, name)
    case RemoteStartResult(errorMessage) =>
      newRemoteStartResult(errorMessage)
  }

  /**
   * Intercept is called for every non-envelope message. Its current use case
   * is so the remote start actor does not have to have a handle to the
   * Serializer when returning responses.
   */
  def intercept(m: AnyRef) =
    if (doIntercept.isDefinedAt(m)) 
      doIntercept(m)
    else
      m
}

/**
 * Provides implementations of each of the remote start actor messages, as standard
 * Scala case classes.
 */
trait DefaultMessageCreator { _: MessageCreator =>

  override def newRemoteStartInvoke(actorClass: String) = 
    DefaultRemoteStartInvokeImpl(actorClass)

  override def newRemoteStartInvokeAndListen(actorClass: String, port: Int, name: String) =
    DefaultRemoteStartInvokeAndListenImpl(actorClass, port, name)

  override def newRemoteStartResult(errorMessage: Option[String]) =
    DefaultRemoteStartResultImpl(errorMessage)
}
