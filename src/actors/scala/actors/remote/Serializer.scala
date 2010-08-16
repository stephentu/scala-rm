/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

import java.io.{ InputStream, OutputStream }

/**
 * This exception is thrown when a <code>Serializer</code> encounters a bad
 * handshake state.
 *
 * @see Serializer
 */
@serializable
class IllegalHandshakeStateException(msg: String) extends Exception(msg) {
  def this() = this("Unknown cause")
}

/**
 * Base trait for all events that a serializer is concerned with in a
 * handshake
 */
sealed trait HandshakeEvent

/**
 * Base trait for all events that a serializer can respond to in a handshake
 */
sealed trait ReceivableEvent extends HandshakeEvent

/**
 * Base trait for all valid responses that a serializer can make in a
 * handshake
 */
sealed trait TriggerableEvent extends HandshakeEvent

/**
 * Indicates that a handshake is beginning with remote node <code>node</code>.
 *
 * @see ReceivableEvent
 * @see Serializer
 */
case class StartEvent(node: Node) extends ReceivableEvent

/**
 * Indicates that message <code>msg</code> was received from the remote end.
 *
 * @see ReceivableEvent
 * @see Serializer
 */
case class RecvEvent(msg: Any) extends ReceivableEvent

/**
 * Indicates to the network layer to send <code>msgs</code> to the remote end.
 *
 * @see TriggerableEvent
 * @see Serializer
 */
case class SendEvent(msgs: Any*) extends TriggerableEvent

/**
 * Indicates to the network layer to send <code>msgs</code> to the remote end,
 * followed by completion of the handshake.
 *
 * @see TriggerableEvent
 * @see SendEvent
 * @see Success
 * @see Serializer
 */
case class SendWithSuccessEvent(msgs: Any*) extends TriggerableEvent

/**
 * Indicates to the network layer to send <code>msgs</code> to the remote end,
 * followed by a failure of the handshake for reason <code>reason</code>.
 *
 * @see TriggerableEvent
 * @see SendEvent
 * @see Error
 * @see Serializer
 */
case class SendWithErrorEvent(reason: String, msgs: Any*) extends TriggerableEvent

/**
 * Indicates a success of the handshake
 *
 * @see TriggerableEvent
 */
case object Success extends TriggerableEvent

/**
 * Indicates a failure of the handshake for reason <code>reason</code>.
 *
 * @see TriggerableEvent
 */
case class Error(reason: String) extends TriggerableEvent

/**
 * The base class which defines the facilities necessary to take messages sent
 * from remote actors, write them to <code>OutputStream</code>s, and vice versa.
 * Extending this class allows a user to supply his/her own facility, to take
 * advantage of other serialization frameworks. The default facility supplied
 * is one which utilizes Java serialization.<br/>
 *
 * Each serializer is idenitified by a unique 64-bit integer, given in the
 * <code>uniqueId</code> field. This allows easy comparison of two serializers
 * over the network with high probability.<br/>
 *
 * In order to provide the ability to supply more robust serializers, an event
 * driven handshake framework is provided. Serializers which wish to exchange
 * protocols with each other before serializing messages can supply handlers,
 * which are guaranteed to run to completion before a message is serialized via the
 * framework.<br/>
 *
 * The default <code>JavaSerializer</code> simply performs a simple handshaking which
 * checks the <code>uniqueId</code> field, and only proceeds if they are
 * equal. Much more complicated protocols can be implemented with this
 * framework however.<br/>
 *
 * Serializers are intended to be unique per connection, meaning they do NOT
 * need to be thread-safe, as long as the <code>Configuration</code> object
 * makes sure to return new instances of a serializer in its
 * <code>newSerializer</code> method. Having said that, if a particular
 * serializer does NOT contain any state, then it is perfectly fine to reuse
 * a single serializer instance.<br/>
 *
 * @see NetKernelMessage
 */
abstract class Serializer {

  // Handshake management 

  /**
   * True if this Serializer needs to participate in a handshake. If
   * <code>isHandshaking</code> is false, <code>handleNextEvent</code> is
   * never called (and can thus just be <code>Map.empty</code>).
   */
  val isHandshaking: Boolean

  /**
   * Main event handler for a serializer handshake. Is only called if
   * <code>isHandshaking</code> is <code>true</code>.<br/>
   *
   * <code>ReceivableEvent</code>s are passed to this handler from the network
   * layer. If <code>handleNextEvent</code> is not defined at a particular
   * event, then that event is simply ignored (the handshake is still alive).
   * If a <code>None</code> is returned as a result of application of
   * <code>handleNextEvent</code>, then nothing happens (the handshake is
   * still alive).<br/>
   *
   * Note that the messages sent from <code>handleNextEvent</code> MUST ONLY
   * be simple messages (primitives, Strings, and byte arrays). See
   * <code>PrimitiveSerializer</code> for a specification of exactly what kind
   * of messages are allowed.<br/>
   *
   * This method should not throw any exceptions. To signal an error in the
   * handshake, use <code>Error</code>. Returning an <code>Error</code> will
   * appropriately cause an <code>IllegalHandshakeStateException</code>
   * exception to be propogated to the appropriate actors<br/>.
   *
   * For non-blocking modes, <code>handleNextEvent</code> runs right in the
   * same thread as the NIO multiplexing event loop. Therefore, it should not
   * perform very expensive operations, since it will block the event loop,
   * causing other connections to possibly starve.
   *
   * @see ReceivableEvent
   * @see TriggerableEvent
   * @see PrimitiveSerializer
   */
  def handleNextEvent: PartialFunction[ReceivableEvent, Option[TriggerableEvent]]

  // Message serialization

  /**
   * Write the necessary sequence of bytes to <code>outputStream</code> to
   * indicate a <code>LocateRequest</code> command to the other side.
   *
   * @param outputStream  The output stream to write the command to. This is
   *                      backed by a byte array, so there is no need to wrap
   *                      a <code>BufferedOutputStream</code> around
   *                      <code>outputStream</code>.
   * @param sessionId     The session ID associated with the request.
   * @param receiverName  The receiver of the message. Can NOT be null
   * 
   * @see   LocateRequest
   */
  def writeLocateRequest(outputStream: OutputStream, sessionId: Long, receiverName: String): Unit

  /**
   * Write the necessary sequence of bytes to <code>outputStream</code> to
   * indicate a <code>LocateResponse</code> command to the other side.
   *
   * @param outputStream  The output stream to write the command to. This is
   *                      backed by a byte array, so there is no need to wrap
   *                      a <code>BufferedOutputStream</code> around
   *                      <code>outputStream</code>.
   * @param sessionId     The session ID associated with the request.
   * @param receiverName  The receiver of the message. Can NOT be null
   * @param found         True if the request was successful, false otherwise.
   * 
   * @see   LocateResponse
   */
  def writeLocateResponse(outputStream: OutputStream, sessionId: Long, receiverName: String, found: Boolean): Unit

  /**
   * Write the necessary sequence of bytes to <code>outputStream</code> to
   * indicate a <code>AsyncSend</code> command to the other side.
   *
   * @param outputStream  The output stream to write the command to. This is
   *                      backed by a byte array, so there is no need to wrap
   *                      a <code>BufferedOutputStream</code> around
   *                      <code>outputStream</code>.
   * @param senderName    The sender of the message. Can be null
   * @param receiverName  The receiver of the message. Can NOT be null
   * @param message       The message being sent. Is typed <code>AnyRef</code>
   *                      for flexibility. An exception should be thrown if
   *                      <code>message</code> cannot be serialized by this
   *                      serializers.
   * 
   * @see   AsyncSend
   */
  def writeAsyncSend(outputStream: OutputStream, senderName: String, receiverName: String, message: AnyRef): Unit

  /**
   * Write the necessary sequence of bytes to <code>outputStream</code> to
   * indicate a <code>SyncSend</code> command to the other side.
   *
   * @param outputStream  The output stream to write the command to. This is
   *                      backed by a byte array, so there is no need to wrap
   *                      a <code>BufferedOutputStream</code> around
   *                      <code>outputStream</code>.
   * @param senderName    The sender of the message. Can NOT be null
   * @param receiverName  The receiver of the message. Can NOT be null
   * @param message       The message being sent. Is typed <code>AnyRef</code>
   *                      for flexibility. An exception should be thrown if
   *                      <code>message</code> cannot be serialized by this
   *                      serializers.
   * @param session       The session id for the message. Can NOT be null
   * 
   * @see   SyncSend
   */
  def writeSyncSend(outputStream: OutputStream, senderName: String, receiverName: String, message: AnyRef, session: String): Unit

  /**
   * Write the necessary sequence of bytes to <code>outputStream</code> to
   * indicate a <code>SyncSend</code> command to the other side.
   *
   * @param outputStream  The output stream to write the command to. This is
   *                      backed by a byte array, so there is no need to wrap
   *                      a <code>BufferedOutputStream</code> around
   *                      <code>outputStream</code>.
   * @param receiverName  The receiver of the message. Can NOT be null
   * @param message       The message being sent. Is typed <code>AnyRef</code>
   *                      for flexibility. An exception should be thrown if
   *                      <code>message</code> cannot be serialized by this
   *                      serializers.
   * @param session       The session id for the message. Can NOT be null
   * 
   * @see   SyncReply
   */
  def writeSyncReply(outputStream: OutputStream, receiverName: String, message: AnyRef, session: String): Unit

  /**
   * Write the necessary sequence of bytes to <code>outputStream</code> to
   * indicate a <code>RemoteApply</code> command to the other side.
   *
   * @param outputStream  The output stream to write the command to. This is
   *                      backed by a byte array, so there is no need to wrap
   *                      a <code>BufferedOutputStream</code> around
   *                      <code>outputStream</code>.
   * @param senderName    The sender of the message. Can NOT be null
   * @param receiverName  The receiver of the message. Can NOT be null
   * @param rfun          The <code>RemoteFunction</code> to be applied
   * 
   * @see   RemoteApply
   * @see   RemoteFunction
   */
  def writeRemoteApply(outputStream: OutputStream, senderName: String, receiverName: String, rfun: RemoteFunction): Unit

  // message deserialization

  /**
   * Interpret a sequence of bytes, written by the other side, and turn it
   * into a <code>NetKernelMessage</code> to be processed by the network
   * kernel. The bytes here are exactly as was written by the other end.
   *
   * @param   bytes   The sequence of bytes sent from the other side, exactly
   *                  how it was written. Note that the entire byte array is
   *                  the message.
   *
   * @return  The control message to forward to the network kernel
   *
   * @see     NetKernelMessage
   * @see     writeLocateRequest
   * @see     writeLocateResponse
   * @see     writeAsyncSend
   * @see     writeSyncSend
   * @see     writeSyncReply
   * @see     writeRemoteApply
   */
  def read(inputStream: InputStream): NetKernelMessage

  /** 
   * Unique identifier used for this serializer 
   */
  val uniqueId: Long

  /** 
   * Default equality method for serializers. Looks simply at the uniqueId field 
   */
  override def equals(o: Any): Boolean = o match {
    case s: Serializer => uniqueId == s.uniqueId
    case _             => false
  }

  /**
   * Default hashCode method for serializers. Returns uniqueId (cast as an
   * int)
   */
  override def hashCode = uniqueId.toInt

}

private[remote] class NonHandshakingSerializerException extends Exception

/**
 * Convenience trait for Serializers which do not participate in a handshake
 *
 * @see Serializer
 */
trait NonHandshakingSerializer { _: Serializer =>
  override val isHandshaking = false
  override def handleNextEvent: PartialFunction[ReceivableEvent, Option[TriggerableEvent]] = {
    case _ => throw new NonHandshakingSerializerException
  }
}

/**
 * Convenience trait for Serializers which participate in a simple
 * <code>uniqueId</code> exchange handshake. 
 *
 * @see Serializer
 */
trait IdResolvingSerializer { _: Serializer =>
  override val isHandshaking = true
  override def handleNextEvent: PartialFunction[ReceivableEvent, Option[TriggerableEvent]] = {
    case StartEvent(_)                     => Some(SendEvent(uniqueId))
    case RecvEvent(id) if (id == uniqueId) => Some(Success)
    case RecvEvent(id)                     => Some(Error("ID's do not match. Expected " + uniqueId + ", but got " + id))
  }
}
