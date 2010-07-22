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

abstract class Serializer[+T <: Proxy] {

  // Handshake management 

  /** 
   * Starting state of the handshake. 
   * Return None if no handshake is desired, in which case the next two
   * methods will never be called (and can thus return null)
   */
  def initialState: Option[Any]

  /**
   * Returns the next message to send to the other side in the handshake.
   * The input is the current state, output is a tuple of 
   * (next state, next message). Next message is None to signal completion
   * of handshake.
   */
  def nextHandshakeMessage: PartialFunction[Any, (Any, Option[Any])]

  /**
   * Callback to receive the next message of the handshake from the other side.
   * Input is a tuple of (current state, message to handle). Output is the
   * next state transition.
   */
  def handleHandshakeMessage: PartialFunction[(Any, Any), Any]

  def isHandshakeError(m: Any) = !(nextHandshakeMessage isDefinedAt m)

  def isHandshakeError(m: (Any, Any)) = !(handleHandshakeMessage isDefinedAt m)

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
    case s: Serializer[Proxy] => uniqueId == s.uniqueId
    case _                    => false
  }

  /**
   * Default hashCode method for serializers. Returns uniqueId (cast as an
   * int)
   */
  override def hashCode = uniqueId.toInt

  type MyNode <: Node
  type MyNamedSend <: NamedSend
  type MyLocator <: Locator
  type MyRemoteStartInvoke <: RemoteStartInvoke
  type MyRemoteStartInvokeAndListen <: RemoteStartInvokeAndListen

  def newNode(address: String, port: Int): MyNode

  def newNamedSend(senderLoc: MyLocator, receiverLoc: MyLocator, metaData: Array[Byte], data: Array[Byte], session: Symbol): MyNamedSend

  def newLocator(node: MyNode, name: Symbol): MyLocator

  def newProxy(remoteNode: MyNode, mode: ServiceMode.Value, serializerClassName: String, name: Symbol): T 

  def newRemoteStartInvoke(actorClass: String): MyRemoteStartInvoke

  def newRemoteStartInvokeAndListen(actorClass: String, port: Int, name: Symbol, mode: ServiceMode.Value): MyRemoteStartInvokeAndListen

  protected def intercept(m: AnyRef): AnyRef = m match {
    case RemoteStartInvoke(actorClass) => 
      newRemoteStartInvoke(actorClass)
    case RemoteStartInvokeAndListen(actorClass, port, name, mode) =>
      newRemoteStartInvokeAndListen(actorClass, port, name, mode)
    case e => 
      e
  }

}

trait DefaultProxyCreator { this: Serializer[DefaultProxyImpl] =>
  override def newProxy(remoteNode: MyNode, mode: ServiceMode.Value, serializerClassName: String, name: Symbol): DefaultProxyImpl =
    new DefaultProxyImpl(remoteNode, mode, serializerClassName, name)
}

trait DefaultEnvelopeMessageCreator { this: Serializer[_ <: Proxy] =>
  override type MyNode = DefaultNodeImpl
  override type MyNamedSend = DefaultNamedSendImpl
  override type MyLocator = DefaultLocatorImpl

  override def newNode(address: String, port: Int): DefaultNodeImpl = DefaultNodeImpl(address, port)

  override def newNamedSend(senderLoc: DefaultLocatorImpl, receiverLoc: DefaultLocatorImpl, metaData: Array[Byte], data: Array[Byte], session: Symbol): DefaultNamedSendImpl =
    DefaultNamedSendImpl(senderLoc, receiverLoc, metaData, data, session)

  override def newLocator(node: DefaultNodeImpl, name: Symbol): DefaultLocatorImpl =
    DefaultLocatorImpl(node, name)
}

trait DefaultControllerMessageCreator { this: Serializer[_ <: Proxy] =>
  override type MyRemoteStartInvoke = DefaultRemoteStartInvokeImpl
  override type MyRemoteStartInvokeAndListen = DefaultRemoteStartInvokeAndListenImpl

  override def newRemoteStartInvoke(actorClass: String): DefaultRemoteStartInvokeImpl = 
    DefaultRemoteStartInvokeImpl(actorClass)

  override def newRemoteStartInvokeAndListen(actorClass: String, port: Int, name: Symbol, mode: ServiceMode.Value): DefaultRemoteStartInvokeAndListenImpl =
    DefaultRemoteStartInvokeAndListenImpl(actorClass, port, name, mode)
}

class NonHandshakingSerializerException extends Exception

trait NonHandshakingSerializer { this: Serializer[_ <: Proxy] =>
  override def initialState = None
  override def nextHandshakeMessage: PartialFunction[Any, (Any, Option[Any])] = {
    case _ => throw new NonHandshakingSerializerException
  }
  override def handleHandshakeMessage: PartialFunction[(Any, Any), Any] = {
    case _ => throw new NonHandshakingSerializerException
  }
}

case object SendID
case object ExpectID
case object Resolved

trait IdResolvingSerializer { this: Serializer[_ <: Proxy] =>
  override def initialState: Option[Any] = Some(SendID)
  override def nextHandshakeMessage: PartialFunction[Any, (Any, Option[Any])] = {
    case SendID   => (ExpectID, Some(uniqueId))
    case Resolved => (Resolved, None)
  }
  override def handleHandshakeMessage: PartialFunction[(Any, Any), Any] = {
    case (ExpectID, MyUniqueId) => Resolved
  }
  private val MyUniqueId = uniqueId
}
