/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote


class IllegalHandshakeStateException(msg: String) extends Exception(msg) {
  def this() = this("Unknown cause")
}

sealed trait HandshakeEvent
sealed trait ReceivableEvent extends HandshakeEvent
sealed trait TriggerableEvent extends HandshakeEvent

/**
 * Indicates that a handshake is beginning with remote node <code>node</code>.
 */
case class StartEvent(node: Node) extends ReceivableEvent

/**
 * Indicates that message <code>msg</code> was received from the remote end.
 */
case class RecvEvent(msg: Any) extends ReceivableEvent

/**
 * Indicates to the network layer to send <code>msgs</code> to the remote end.
 */
case class SendEvent(msgs: Any*) extends TriggerableEvent

/**
 * Indicates to the network layer to send <code>msgs</code> to the remote end,
 * followed by completion of the handshake.
 */
case class SendWithSuccessEvent(msgs: Any*) extends TriggerableEvent

/**
 * Indicates to the network layer to send <code>msgs</code> to the remote end,
 * followed by a failure of the handshake for reason <code>reason</code>.
 */
case class SendWithErrorEvent(reason: String, msgs: Any*) extends TriggerableEvent

/**
 * Indicates a success of the handshake
 */
case object Success extends TriggerableEvent

/**
 * Indicates a failure of the handshake for reason <code>reason</code>.
 */
case class Error(reason: String) extends TriggerableEvent

/**
 * The base class which defines the facilities necessary to take messages sent
 * from remote actors, and turn them into byte arrays, and vice versa.
 * Extending this class allows a user to supply his/her own facility, to take
 * advantage of other serialization frameworks. The default facility supplied
 * is one which utilizes Java serialization.
 *
 * Each serializer is idenitified by a unique 64-bit integer, given in the
 * <code>uniqueId</code> field. This allows easy comparison of two serializers
 * over the newtork with high probability.
 *
 * In order to provide the ability to supply more robust serializers, an event
 * driven handshake framework is provided. Serializers which wish to exchange
 * protocols with each other before serializing messages can supply handlers,
 * which are guaranteed to run to completion before a message is serialized via the
 * framework.
 *
 * The default Java serializer simply performs a simple handshaking which
 * checks the <code>uniqueId</code> field, and only proceeds if they are
 * equal. Much more complicated protocols can be implemented with this
 * framework however.
 *
 * Serializers are intended to be unique per connection, meaning they do NOT
 * need to be thread-safe, as long as the <code>Configuration</code> object
 * makes sure to return new instances of a serializer in its
 * <code>newSerializer</code> method. Having said that, if a particular
 * serializer does NOT contain any state, then it is perfectly fine to reuse
 * a single serializer instance.
 *
 * In order to provide as much control over the message serialization as
 * possible, Serializers must also supply envelope message factory methods.
 * See the documentation for <code>MessageCreator</code>.
 */
abstract class Serializer extends MessageCreator {

  // Handshake management 

  /**
   * True if this Serializer needs to participate in a handshake. If
   * <code>isHandshaking</code> is false, <code>handleNextEvent</code> is
   * never called.
   */
  val isHandshaking: Boolean

  /**
   * Main event handler for a serializer handshake. Is only called if
   * <code>isHandshaking</code> is <code>true</code>.
   *
   * <code>ReceivableEvent</code>s are passed to this handler from the network
   * layer. If <code>handleNextEvent</code> is not defined at a particular
   * event, then that event is simply ignored (the handshake is still alive).
   * If a <code>None</code> is returned as a result of application of
   * <code>handleNextEvent</code>, then nothing happens (the handshake is
   * still alive).
   *
   * Note that the messages sent from <code>handleNextEvent</code> MUST ONLY
   * be simple messages (primitives, Strings, and byte arrays). See
   * <code>PrimitiveSerializer</code> for a specification of exactly what kind
   * of messages are allowed.
   */
  def handleNextEvent: PartialFunction[ReceivableEvent, Option[TriggerableEvent]]

  // Message serialization

  /**
   * Given a message, optionally return any metadata about the message
   * serialized into a byte array. Note that this method is called for the
   * envelope messages, in addition to the user messages.
   */
  def serializeMetaData(message: AnyRef): Option[Array[Byte]]

  /** 
   * Given a message, return a byte array representation of the message
   * without any of its associated metadata. Note that this method is called
   * for the envelope messages, in addition to user messages.
   */  
  def serialize(message: AnyRef): Array[Byte]

  /**
   * Given an optional metadata byte array and a data byte array, 
   * deserialize the data byte array into an object. Note that this method is
   * called for envelope messages, in addition to user messages.
   */
  def deserialize(metaData: Option[Array[Byte]], data: Array[Byte]): AnyRef

  /**
   * Returns the class name used to bootstrap an equivalent serializer on the
   * remote side. The default implementation returns
   * <code>getClass.getName</code>. Implementations which want to separate out
   * the client side logic from the server side logic can supply the class
   * name of the server side serializer.
   */
  def bootstrapClassName: String = 
    getClass.getName

  /** 
   * Helper method to serialize the class name of a message
   */
  protected def serializeClassName(o: AnyRef): Array[Byte] = o.getClass.getName.getBytes

  /** Unique identifier used for this serializer */
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

class NonHandshakingSerializerException extends Exception

/**
 * Convenience trait for Serializers which do not participate in a handshake
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
 */
trait IdResolvingSerializer { _: Serializer =>
  override val isHandshaking = true
  override def handleNextEvent: PartialFunction[ReceivableEvent, Option[TriggerableEvent]] = {
    case StartEvent(_)         => Some(SendEvent(uniqueId))
    case RecvEvent(MyUniqueId) => Some(Success)
    case RecvEvent(id)         => Some(Error("ID's do not match. Expected " + MyUniqueId + ", but got " + id))
  }
  private val MyUniqueId = uniqueId
}
