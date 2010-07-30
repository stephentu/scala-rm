/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

import java.util.concurrent.ConcurrentHashMap
import java.net.{ InetAddress, InetSocketAddress }

object Node {
  final val localhost = InetAddress.getLocalHost.getCanonicalHostName
  // TODO: error check the port number
  def apply(address: String, port: Int): Node = 
    if (address eq null) DefaultNodeImpl(localhost, port)
    else                 DefaultNodeImpl(address, port)
  def apply(port: Int): Node = apply(localhost, port)
  def unapply(n: Node): Option[(String, Int)] = Some((n.address, n.port))

  private final val addresses = new ConcurrentHashMap[String, String]
  private[remote] def getCanonicalAddress(s: String): String = {
    val testAddress = addresses.get(s)
    if (testAddress ne null)
      testAddress
    else {
      val resolved = InetAddress.getByName(s).getCanonicalHostName
      val testAddress0 = addresses.putIfAbsent(s, resolved)
      if ((testAddress0 ne null) && testAddress0 != resolved)
        Debug.error("Address " + s + " resolved differently: " + testAddress0 + " and " + resolved)
      resolved
    }
  }
}

trait Node {

  def address: String
  def port: Int

  /**
   * Returns an InetSocketAddress representation of this Node
   */
  def toInetSocketAddress = new InetSocketAddress(address, port)

  /**
   * Returns the canonical representation of this form, resolving the
   * address into canonical form (as determined by the Java API)
   */
  def canonicalForm =
    newNode(Node.getCanonicalAddress(address), port)

  protected def newNode(a: String, p: Int): Node

  def isCanonical = this == canonicalForm

  override def equals(o: Any) = o match {
    case n: Node => n.address == this.address && n.port == this.port
    case _       => false
  }

  override def hashCode = address.hashCode ^ port.hashCode

}

case class DefaultNodeImpl(override val address: String, override val port: Int) extends Node {
  override def newNode(a: String, p: Int) = DefaultNodeImpl(a, p)
}

object AsyncSend {
  def apply(senderName: Option[Symbol], receiverName: Symbol, metaData: Array[Byte], data: Array[Byte]): AsyncSend =
    DefaultAsyncSendImpl(senderName, receiverName, metaData, data)
  def unapply(n: AsyncSend): Option[(Option[Symbol], Symbol, Array[Byte], Array[Byte])] =
    Some((n.senderName, n.receiverName, n.metaData, n.data))
}

trait AsyncSend {
  def senderName: Option[Symbol]
  def receiverName: Symbol
  def metaData: Array[Byte]
  def data: Array[Byte]
}

case class DefaultAsyncSendImpl(override val senderName: Option[Symbol],
                                override val receiverName: Symbol, 
                                override val metaData: Array[Byte], 
                                override val data: Array[Byte]) extends AsyncSend

object SyncSend {
  def apply(senderName: Symbol, receiverName: Symbol, metaData: Array[Byte], data: Array[Byte], session: Symbol): SyncSend =
    DefaultSyncSendImpl(senderName, receiverName, metaData, data, session)
  def unapply(n: SyncSend): Option[(Symbol, Symbol, Array[Byte], Array[Byte], Symbol)] =
    Some((n.senderName, n.receiverName, n.metaData, n.data, n.session))
}

trait SyncSend {
  def senderName: Symbol
  def receiverName: Symbol
  def metaData: Array[Byte]
  def data: Array[Byte]
  def session: Symbol
}

case class DefaultSyncSendImpl(override val senderName: Symbol, 
                               override val receiverName: Symbol, 
                               override val metaData: Array[Byte], 
                               override val data: Array[Byte],
                               override val session: Symbol) extends SyncSend

object SyncReply {
  def apply(receiverName: Symbol, metaData: Array[Byte], data: Array[Byte], session: Symbol): SyncReply =
    DefaultSyncReplyImpl(receiverName, metaData, data, session)
  def unapply(n: SyncReply): Option[(Symbol, Array[Byte], Array[Byte], Symbol)] =
    Some((n.receiverName, n.metaData, n.data, n.session))
}

trait SyncReply {
  def receiverName: Symbol
  def metaData: Array[Byte]
  def data: Array[Byte]
  def session: Symbol
}

case class DefaultSyncReplyImpl(override val receiverName: Symbol, 
                                override val metaData: Array[Byte], 
                                override val data: Array[Byte],
                                override val session: Symbol) extends SyncReply

object RemoteApply {
  def apply(senderName: Symbol, receiverName: Symbol, rfun: RemoteFunction): RemoteApply = DefaultRemoteApplyImpl(senderName, receiverName, rfun)
  def unapply(r: RemoteApply): Option[(Symbol, Symbol, RemoteFunction)] = Some((r.senderName, r.receiverName, r.function))
}

trait RemoteApply {
  def senderName: Symbol
  def receiverName: Symbol
  def function: RemoteFunction
}

case class DefaultRemoteApplyImpl(override val senderName: Symbol,
                                  override val receiverName: Symbol,
                                  override val function: RemoteFunction) extends RemoteApply
