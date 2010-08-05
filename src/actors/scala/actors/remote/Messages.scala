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

  @throws(classOf[IllegalArgumentException])
  def apply(address: String, port: Int): Node = 
    NodeImpl(checkAddress(address), checkPort(port))

  @throws(classOf[IllegalArgumentException])
  def apply(port: Int): Node = apply(localhost, port)

  def unapply(n: Node): Option[(String, Int)] = Some(n.address, n.port)

  private def checkAddress(a: String) =
    if (a eq null) localhost else a

  private def checkPort(p: Int) = 
    if (p > 65535)
      throw new IllegalArgumentException("Port number too large")
    else if (p < 0)
      throw new IllegalArgumentException("Port number cannot be negative")
    else if (p == 0)
      throw new IllegalArgumentException("No ephemeral port specification allowed")
    else p

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
  def canonicalForm = newNode(Node.getCanonicalAddress(address), port)

  def isCanonical = address == Node.getCanonicalAddress(address) 

  protected def newNode(address: String, port: Int): Node
}

private[remote] case class NodeImpl(override val address: String, 
                                    override val port: Int) extends Node {
  override def newNode(a: String, p: Int) = NodeImpl(a, p)
}

object AsyncSend {
  def apply(senderName: String, receiverName: String, message: AnyRef): AsyncSend =
    DefaultAsyncSendImpl(senderName, receiverName, message)
  def unapply(n: AsyncSend): Option[(String, String, AnyRef)] =
    Some((n.senderName, n.receiverName, n.message))
}

sealed trait NetKernelMessage

trait AsyncSend extends NetKernelMessage {
  def senderName: String
  def receiverName: String
  def message: AnyRef
}

case class DefaultAsyncSendImpl(override val senderName: String,
                                override val receiverName: String, 
                                override val message: AnyRef) extends AsyncSend

object SyncSend {
  def apply(senderName: String, receiverName: String, message: AnyRef, session: String): SyncSend =
    DefaultSyncSendImpl(senderName, receiverName, message, session)
  def unapply(n: SyncSend): Option[(String, String, AnyRef, String)] =
    Some((n.senderName, n.receiverName, n.message, n.session))
}

trait SyncSend extends NetKernelMessage {
  def senderName: String
  def receiverName: String
  def message: AnyRef
  def session: String
}

case class DefaultSyncSendImpl(override val senderName: String, 
                               override val receiverName: String, 
                               override val message: AnyRef,
                               override val session: String) extends SyncSend

object SyncReply {
  def apply(receiverName: String, message: AnyRef, session: String): SyncReply =
    DefaultSyncReplyImpl(receiverName, message, session)
  def unapply(n: SyncReply): Option[(String, AnyRef, String)] =
    Some((n.receiverName, n.message, n.session))
}

trait SyncReply extends NetKernelMessage {
  def receiverName: String
  def message: AnyRef
  def session: String
}

case class DefaultSyncReplyImpl(override val receiverName: String, 
                                override val message: AnyRef,
                                override val session: String) extends SyncReply

object RemoteApply {
  def apply(senderName: String, receiverName: String, rfun: RemoteFunction): RemoteApply = DefaultRemoteApplyImpl(senderName, receiverName, rfun)
  def unapply(r: RemoteApply): Option[(String, String, RemoteFunction)] = Some((r.senderName, r.receiverName, r.function))
}

trait RemoteApply extends NetKernelMessage {
  def senderName: String
  def receiverName: String
  def function: RemoteFunction
}

case class DefaultRemoteApplyImpl(override val senderName: String,
                                  override val receiverName: String,
                                  override val function: RemoteFunction) extends RemoteApply
