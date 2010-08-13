package scala.actors

package object remote {
  private[remote] type BytesReceiveCallback = (ByteConnection, Array[Byte]) => Unit
  private[remote] type MessageReceiveCallback = (MessageConnection, Serializer, AnyRef) => Unit
  private[remote] type ConnectionCallback[C <: Connection] = (Listener, C) => Unit
}
