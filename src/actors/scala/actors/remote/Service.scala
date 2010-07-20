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
import java.nio.ByteBuffer

import scala.collection.mutable.{ HashMap, ListBuffer }

/**
 * Mode of operation. Can be blocking or non-blocking
 */
object ServiceMode extends Enumeration {
  val Blocking, NonBlocking = Value
}

trait HasServiceMode {
  def mode: ServiceMode.Value
}

trait Listener 
  extends HasServiceMode 
  with    CanTerminate {

  def port: Int

  protected val connectionCallback: ConnectionCallback

  protected def receiveConnection(conn: ByteConnection) {
    try {
      connectionCallback(this, conn)
    } catch {
      case e: Exception =>
        Debug.error("Caught exception calling connectionCallback: " + e.getMessage)
        Debug.doError { e.printStackTrace }
    }
  }
}

trait Connection 
  extends HasServiceMode 
  with    CanTerminate {

  /**
   * Returns the (canonical) remote node
   */
  def remoteNode: Node

  /**
   * Returns the (canonical) local node
   */
  def localNode: Node

  // TODO: use syncvar here
  @volatile private var _attachment: Option[AnyRef] = None
  private val attachLock = new Object

  def attach(attachment: AnyRef) {
    assert(attachment ne null)
    attachLock.synchronized {
      _attachment = Some(attachment)
      attachLock.notifyAll()
    }
  }

  def attachment_? = attachLock.synchronized { _attachment }

  def attachment_! = attachLock.synchronized {
    while (!_attachment.isDefined) attachLock.wait()
    assert(_attachment.isDefined)
    _attachment.get
  }

}

trait ByteConnection extends Connection {

  def send(data: Array[Byte]): Unit

  def send(data0: Array[Byte], data1: Array[Byte]): Unit

  protected val receiveCallback: BytesReceiveCallback

  protected def receiveBytes(bytes: Array[Byte]) {
    try {
      receiveCallback(this, bytes)
    } catch {
      case e: Exception =>
        Debug.error("Caught exception calling receiveCallback: " + e.getMessage)
        Debug.doError { e.printStackTrace }
    }
  }

}

trait MessageConnection extends Connection {

  def send(f: Serializer[Proxy] => AnyRef): Unit 

  def send(msg: AnyRef) { send { _: Serializer[Proxy] => msg } }

  def activeSerializer: Serializer[Proxy]

  protected val receiveCallback: MessageReceiveCallback

  protected def receiveMessage(s: Serializer[Proxy], message: AnyRef) {
    try {
      receiveCallback(this, s, message)
    } catch {
      case e: Exception =>
        Debug.error("Caught exception calling receiveCallback: " + e.getMessage)
        Debug.doError { e.printStackTrace }
    }
  }

}

trait ServiceProvider extends HasServiceMode with CanTerminate {
  def connect(node: Node, receiveCallback: BytesReceiveCallback): ByteConnection
  def listen(port: Int, connectionCallback: ConnectionCallback, receiveCallback: BytesReceiveCallback): Listener
}

trait EncodingHelpers {

  /**
   * Takes an unencoded byte array, and returns an encoded
   * version of the data, suitable for sending over the wire.
   * Default encoding is to simply prepend the length of the
   * unencoded message as a 4-byte int.
   */
  protected def encodeToArray(unenc: Array[Byte]): Array[Byte] = {
    // TODO: handle overflow
    val baos    = new ByteArrayOutputStream(unenc.length + 4)
    val dataout = new DataOutputStream(baos)
    dataout.writeInt(unenc.length)
    dataout.write(unenc)
    baos.toByteArray
  }

  protected def encodeToByteBuffer(unenc: Array[Byte]): ByteBuffer = {
    // TODO: handle overflow
    val buf = ByteBuffer.allocate(unenc.length + 4)
    buf.putInt(unenc.length)
    buf.put(unenc)
    buf.rewind()
    buf
  }

}

/**
 * @version 0.9.10
 * @author Philipp Haller
 */
abstract class Service extends CanTerminate {

  protected def serviceProviderFor(mode: ServiceMode.Value): ServiceProvider

  private var nonBlockServiceSet = false
  private var blockServiceSet    = false

  private lazy val nonBlockingService = {
    nonBlockServiceSet = true
    serviceProviderFor(ServiceMode.NonBlocking)
  }

  private lazy val blockingService = {
    blockServiceSet = true
    serviceProviderFor(ServiceMode.Blocking)
  }

  protected def serviceProviderFor0(mode: ServiceMode.Value) = mode match {
    case ServiceMode.NonBlocking => nonBlockingService
    case ServiceMode.Blocking    => blockingService
  }

  def connect(node: Node, 
              serializer: Serializer[Proxy], 
              mode: ServiceMode.Value,
              recvCallback: MessageReceiveCallback): MessageConnection

  def listen(port: Int, mode: ServiceMode.Value, recvCallback: MessageReceiveCallback): Listener

  override def doTerminateImpl(isBottom: Boolean) {
    assert(!isBottom)
    if (nonBlockServiceSet) nonBlockingService.terminateTop()
    if (blockServiceSet) blockingService.terminateTop()
  }

}

class ProviderAlreadyClosedException extends RuntimeException
class ConnectionAlreadyClosedException extends RuntimeException
