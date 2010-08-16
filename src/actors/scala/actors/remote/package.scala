package scala.actors

import java.io.InputStream

package object remote {
  private[remote] type BytesReceiveCallback = (ByteConnection, InputStream) => Unit
  private[remote] type MessageReceiveCallback = (MessageConnection, Serializer, AnyRef) => Unit
  private[remote] type ConnectionCallback[C <: Connection] = (Listener, C) => Unit
}
