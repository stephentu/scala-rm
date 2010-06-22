/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

import java.io.{ ByteArrayOutputStream, DataOutputStream }

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

  /**
   * Send message datums to the given node, as separate messages, but 
   * preserving the order on the wire. This should be an atomic operation
   * in the sense that no other data should be mixed in with this data stream
   * when it is sent
   */
  def send(node: Node, data: Array[Byte]*) {
    rawSend(node, encodeAndConcat(data.toArray))
  }

  /**
   * Implemented by subclasses to perform the actual transmission
   * of the data across the wire. The implementation should make
   * no effort to encode the data, but should only send the raw
   * bytes over the wire.
   */
  protected def rawSend(node: Node, data: Array[Byte]): Unit

  /**
   * Takes datums, encodes each datum separately, and returns
   * a single byte array which is the concatenation of the encoded
   * datums. 
   */
  protected def encodeAndConcat(datums: Array[Array[Byte]]): Array[Byte] =
    datums.map(encode(_)).flatMap(x => x)

  /**
   * Takes an unencoded byte array, and returns an encoded
   * version of the data, suitable for sending over the wire.
   * Default encoding is to simply prepend the length of the
   * unencoded message as a 4-byte int.
   */
  def encode(unenc: Array[Byte]): Array[Byte] = {
    val baos    = new ByteArrayOutputStream(unenc.length + 4)
    val dataout = new DataOutputStream(baos)
    dataout.writeInt(unenc.length)
    dataout.write(unenc)
    baos.toByteArray
  }

  def start(): Unit
  def terminate(): Unit
}
