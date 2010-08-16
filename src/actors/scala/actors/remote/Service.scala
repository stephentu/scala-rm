/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

import java.io.{ ByteArrayOutputStream, DataOutputStream, InputStream }
import java.nio.ByteBuffer

import scala.collection.mutable.{ HashMap, ListBuffer }

/**
 * Mode of operation. Can be blocking or non-blocking. This mode of operation
 * corresponds to how the network operations should behave. In the current
 * implementation, <code>Blocking</code> implies all network socket
 * reads/writes block the current thread (using regular Java IO), and
 * </code>NonBlocking</code> implies all network reads/writes happen
 * asynchronously (using Java NIO). 
 * 
 * @see Configuration
 * @see RemoteActor#alive
 * @see RemoteActor#select
 */
object ServiceMode extends Enumeration {
  val Blocking, NonBlocking = Value
}

private[remote] trait HasServiceMode {
  def mode: ServiceMode.Value
}

private[remote] trait HasAttachment {

  // We don't use SyncVar for _attachment because this way we can avoid
  // grabbing a lock once its set (and use the double check locking idiom)
  @volatile private[this] var _attachment: AnyRef = _ 
  private[this] val attachLock = new Object

  /**
   * Can only attach once. Null attachments forbidden
   */
  def attach(attachment: AnyRef) {
    assert(attachment ne null)
    attachLock.synchronized {
      if (_attachment ne null)
        throw new IllegalStateException("Can only attach once")
      _attachment = attachment
      attachLock.notifyAll()
    }
  }

  /**
   * Returns the current attachment as an Option
   */
  def attachment_? : Option[AnyRef] = 
    if (_attachment eq null) 
      None 
    else 
      Some(_attachment) 

  /**
   * Waits until attachment is set to something 
   */
  def attachment_! : AnyRef = 
    if (_attachment ne null) 
      _attachment 
    else {
      attachLock.synchronized {
        while (_attachment eq null) attachLock.wait()
        _attachment
      }
    }

}

private[remote] trait Listener extends HasServiceMode 
                               with    HasAttachment 
                               with    CanTerminate {

  def port: Int

  protected val connectionCallback: ConnectionCallback[ByteConnection]

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

private[remote] trait Connection extends HasServiceMode 
                                 with    HasAttachment 
                                 with    CanTerminate {

  /**
   * Returns the (canonical) remote node
   */
  def remoteNode: Node

  /**
   * Returns the (canonical) local node
   */
  def localNode: Node

  def isEphemeral: Boolean


  /**
   * Returns a RFuture which can be blocked on until the connection is
   * established. Established means the TCP connection has been setup
   */
  def connectFuture: RFuture

}

private[remote] trait ByteConnection extends Connection {

  /**
   * 
   */
  def send(data: ByteSequence, ftch: Option[RFuture]): Unit

  protected val receiveCallback: BytesReceiveCallback

  protected def receiveBytes(inputStream: InputStream) {
    try {
      receiveCallback(this, inputStream)
    } catch {
      case e: Exception =>
        Debug.error("Caught exception calling receiveCallback: " + e.getMessage)
        Debug.doError { e.printStackTrace }
    }
  }

}

private[remote] trait MessageConnection extends Connection {

  /**
   * Send a message. The message is passed via a closure, because at any point
   * this connection might not have completed the handshake, so a callback is
   * necessary.
   */
  def send(ftch: Option[RFuture])(msg: Serializer => ByteSequence): Unit

  /**
   * Returns a future which can be blocked on until the connection's handshake
   * is finished.
   */
  def handshakeFuture: RFuture

  def activeSerializer: Serializer

  protected val receiveCallback: MessageReceiveCallback

  protected def receiveMessage(s: Serializer, message: AnyRef) {
    try {
      receiveCallback(this, s, message)
    } catch {
      case e: Exception =>
        Debug.error("Caught exception calling receiveCallback: " + e.getMessage)
        Debug.doError { e.printStackTrace }
    }
  }

  /** Configuration object used to create this connection */
  def config: Configuration

}

private[remote] trait ServiceProvider extends HasServiceMode with CanTerminate {
  def connect(node: Node, receiveCallback: BytesReceiveCallback): ByteConnection
  def listen(port: Int, 
             connectionCallback: ConnectionCallback[ByteConnection], 
             receiveCallback: BytesReceiveCallback): Listener
}

private[remote] abstract class Service extends CanTerminate {

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
              config: Configuration,
              recvCallback: MessageReceiveCallback): MessageConnection

  def listen(port: Int, 
             config: Configuration,
  					 connCallback: ConnectionCallback[MessageConnection],
  					 recvCallback: MessageReceiveCallback): Listener

  override def doTerminateImpl(isBottom: Boolean) {
    assert(!isBottom)
    if (nonBlockServiceSet) nonBlockingService.terminateTop()
    if (blockServiceSet) blockingService.terminateTop()
  }

}

/**
 * Low level exception used to indicate that a service provider has already
 * terminated
 */
private[remote] class ProviderAlreadyClosedException extends AlreadyTerminatedException

/**
 * Low level exception thrown when a connection is already terminated.
 * This is a signal that either the connection is not stable, or a stale
 * connection handle was used.
 */
class ConnectionAlreadyClosedException extends AlreadyTerminatedException
