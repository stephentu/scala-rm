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

case object StartEvent extends ReceivableEvent
case class RecvEvent(msg: Any) extends ReceivableEvent

case class SendEvent(msg: Any) extends TriggerableEvent
case object Success extends TriggerableEvent
case class Error(reason: String) extends TriggerableEvent

abstract class Serializer extends MessageCreator {

  // Handshake management 

  /**
   * True if this Serializer needs to participate in a handshake
   */
  def isHandshaking: Boolean

  def handleNextEvent: PartialFunction[ReceivableEvent, Option[TriggerableEvent]]

  def isHandshakeError(evt: ReceivableEvent) = !(handleNextEvent isDefinedAt evt)

  // Message serialization

  /**
   * Given a message, optionally return any metadata about the message
   * serialized into a byte array
   */
  def serializeMetaData(message: AnyRef): Option[Array[Byte]]

  /** 
   * Given a message, return a byte array representation of the message
   * without any of its associated metadata
   */  
  def serialize(message: AnyRef): Array[Byte]

  /**
   * Given an optional metadata byte array and a data byte array, 
   * deserialize the data byte array into an object
   */
  def deserialize(metaData: Option[Array[Byte]], data: Array[Byte]): AnyRef

  /** 
   * Helper method to serialize the class name of a message
   */
  protected def serializeClassName(o: AnyRef): Array[Byte] = o.getClass.getName.getBytes

  /** Unique identifier used for this serializer */
  def uniqueId: Long

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

trait NonHandshakingSerializer { this: Serializer =>
  override def isHandshaking = false
  override def handleNextEvent: PartialFunction[ReceivableEvent, Option[TriggerableEvent]] = {
    case _ => throw new NonHandshakingSerializerException
  }
}

trait IdResolvingSerializer { this: Serializer =>
  override def isHandshaking = true
  override def handleNextEvent: PartialFunction[ReceivableEvent, Option[TriggerableEvent]] = {
    case StartEvent            => Some(SendEvent(uniqueId))
    case RecvEvent(MyUniqueId) => Some(Success)
    case RecvEvent(_)          => Some(Error("ID's do not match"))
  }
  private val MyUniqueId = uniqueId
}
