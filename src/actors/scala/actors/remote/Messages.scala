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
  val localhost = InetAddress.getLocalHost.getCanonicalHostName
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
    case n: Node =>
      n.address == this.address && n.port == this.port
    case _ => 
      false
  }

  override def hashCode = address.hashCode + port.hashCode

}

case class DefaultNodeImpl(override val address: String, override val port: Int) extends Node {
  override def newNode(a: String, p: Int) = DefaultNodeImpl(a, p)
}

object Locator {
  def apply(node: Node, name: Symbol): Locator = DefaultLocatorImpl(node, name)
  def unapply(l: Locator): Option[(Node, Symbol)] = Some((l.node, l.name))
}

trait Locator {
  def node: Node
  def name: Symbol
  override def equals(o: Any) = o match {
    case l: Locator =>
      l.node == this.node && l.name == this.name
    case _ => false
  }
  override def hashCode = node.hashCode + name.hashCode
}

case class DefaultLocatorImpl(override val node: Node, override val name: Symbol) extends Locator

object NamedSend {
  def apply(senderLoc: Locator, receiverLoc: Locator, metaData: Array[Byte], data: Array[Byte], session: Option[Symbol]): NamedSend =
    DefaultNamedSendImpl(senderLoc, receiverLoc, metaData, data, session)
  def unapply(n: NamedSend): Option[(Locator, Locator, Array[Byte], Array[Byte], Option[Symbol])] =
    Some((n.senderLoc, n.receiverLoc, n.metaData, n.data, n.session))
}

trait NamedSend {
  def senderLoc: Locator
  def receiverLoc: Locator
  def metaData: Array[Byte]
  def data: Array[Byte]
  def session: Option[Symbol]
}

case class DefaultNamedSendImpl(override val senderLoc: Locator, 
                                override val receiverLoc: Locator, 
                                override val metaData: Array[Byte], 
                                override val data: Array[Byte], 
                                override val session: Option[Symbol]) extends NamedSend

object RemoteApply {
  def apply(senderLoc: Locator, receiverLoc: Locator, rfun: RemoteFunction): RemoteApply = DefaultRemoteApplyImpl(senderLoc, receiverLoc, rfun)
  def unapply(r: RemoteApply): Option[(Locator, Locator, RemoteFunction)] = Some((r.senderLoc, r.receiverLoc, r.function))
}

trait RemoteApply {
  def senderLoc: Locator
  def receiverLoc: Locator
  def function: RemoteFunction
}

case class DefaultRemoteApplyImpl(override val senderLoc: Locator, 
                                  override val receiverLoc: Locator, 
                                  override val function: RemoteFunction) extends RemoteApply
