package scala.actors

package object remote {
  type BytesReceiveCallback   = (ByteConnection, Array[Byte]) => Unit
  type MessageReceiveCallback = (MessageConnection, Serializer[Proxy], AnyRef) => Unit
  type ConnectionCallback[C <: Connection] = (Listener, C) => Unit
}
