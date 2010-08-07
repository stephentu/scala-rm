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

/**
 * A <code>Node</code> is a hostname and port combination, identifying a
 * remote node on the network. This object contains factory methods for the
 * creation of nodes.
 */
object Node {

  /**
   * Contains the canonical hostname of this local machine
   */
  private[remote] final val localhost = InetAddress.getLocalHost.getCanonicalHostName

  /**
   * Create a new Node representing <code>address</code>:<code>port</code>.
   * 
   * @param   address     The hostname of the remote machine. If null, then
   *                      assumes localhost.
   * @param   port        The port number of the remote machine
   *
   * @return  The newly created node
   */
  @throws(classOf[IllegalArgumentException])
  def apply(address: String, port: Int): Node = 
    NodeImpl(checkAddress(address), checkPort(port))

  /**
   * Creates a new Node representing <code>localhost</code>:<code>port</code>.
   * Equivalent to <code>apply(null, port)</code>.
   *
   * @param   port        The port number
   *
   * @return  The newly created node
   */
  @throws(classOf[IllegalArgumentException])
  def apply(port: Int): Node = apply(localhost, port)

  def unapply(n: Node): Option[(String, Int)] = Some(n.address, n.port)

  private def checkAddress(a: String) =
    if (a eq null) localhost else a

  private[remote] def checkPort(p: Int, allowEphemeral: Boolean = false) = 
    if (p > 65535)
      throw new IllegalArgumentException("Port number too large")
    else if (p < 0)
      throw new IllegalArgumentException("Port number cannot be negative")
    else if (p == 0 && !allowEphemeral)
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

sealed trait Node {
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
