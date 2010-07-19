/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors

/**
 * The <code>OutputChannel</code> trait provides a common interface
 * for all channels to which values can be sent.
 *
 * @author Philipp Haller
 *
 * @define actor `OutputChannel`
 */
trait OutputChannel[-Msg] {

  /**
   * Sends <code>msg</code> to this $actor (asynchronous).
   *
   * @param  msg      the message to send
   */
  def !(msg: Msg): Unit

  /**
   * Sends <code>msg</code> to this $actor (asynchronous) supplying
   * explicit reply destination.
   *
   * @param  msg      the message to send
   * @param  replyTo  the reply destination
   */
  def send(msg: Msg, replyTo: OutputChannel[Any]): Unit

  /**
   * Forwards <code>msg</code> to this $actor (asynchronous).
   *
   * @param  msg      the message to forward
   */
  def forward(msg: Msg): Unit

  /**
   * Returns the <code>Actor</code> that is receiving from this $actor.
   */
  def receiver: Actor


  /**
   * Contains the channel name of this $actor
   */
  private[this] var _channelName: Option[Symbol] = None

  /**
   * Returns the channel name of this $actor
   */
  private[actors] def channelName: Option[Symbol] = _channelName

  /**
   * Sets the channel name of this $actor. Silently clobbers any existing name
   *
   * @param newName the channel name to set this $actor to
   */
  private[actors] def channelName_=(newName: Option[Symbol]) { _channelName = newName }

}
