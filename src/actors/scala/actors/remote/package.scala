package scala.actors

package object remote {
  type BytesReceiveCallback   = (ByteConnection, Array[Byte]) => Unit
  type MessageReceiveCallback = (MessageConnection, Serializer, AnyRef) => Unit
  type ConnectionCallback     = (Listener, ByteConnection) => Unit
}
