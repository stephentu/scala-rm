/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

/**
 * @version 0.9.10
 * @author Philipp Haller
 */
trait Service {
  val kernel = new NetKernel(this)
  val serializer: Serializer

  /**
   * Returns the node that this service is listening on
   */
  def node: Node

  /**
   * Returns the local endpoint to connect to remote node n
   * @param   n - remote node
   * @returns local node
   */
  def localNodeFor(n: Node): Node

  def send(node: Node, data: Array[Byte]): Unit

  def start(): Unit
  def terminate(): Unit
}
