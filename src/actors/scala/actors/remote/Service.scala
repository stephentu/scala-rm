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

/**
 * Can only be terminated once
 */
trait CanTerminate {

  final def terminateTop() { doTerminate(false) }

  final def terminateBottom() { doTerminate(true) }

  protected final var preTerminate: Option[() => Unit] = None
  final def beforeTerminate(f: => Unit) {
    terminateLock.synchronized {
      if (!terminateInitiated) {
        assert(!terminateCompleted)
        preTerminate match {
          case Some(_) =>
            Debug.warning(this + ": beforeTerminate() - terminate sequence already registered - replacing")
            preTerminate = Some(() => f)
          case None =>
            preTerminate = Some(() => f)
        }
      } else 
        Debug.info(this + ": beforeTerminate() - terminate sequence already started")
    }
  }

  protected final var postTerminate: Option[() => Unit] = None
  final def afterTerminate(f: => Unit) {
    terminateLock.synchronized {
      if (!terminateInitiated) {
        assert(!terminateCompleted)
        postTerminate match {
          case Some(_) =>
            Debug.warning(this + ": afterTerminate() - terminate sequence already registered - replacing")
            postTerminate = Some(() => f)
          case None =>
            postTerminate = Some(() => f)
        }
      } else 
        Debug.info(this + ": afterTerminate() - terminate sequence already started")
    }
  }

  // assumes terminateLock already acquired
  protected final def doTerminate(isBottom: Boolean) {
    terminateLock.synchronized {
      if (!terminateInitiated) {
        assert(!terminateCompleted)
        preTerminate match {
          case Some(f) => f()
          case None =>
        }
        terminateInitiated = true
        doTerminateImpl(isBottom)
        terminateCompleted = true
        postTerminate match {
          case Some(f) => f()
          case None =>
        }
      }
    }
  }

  /**
   * Guaranteed to only execute once. TerminateLock is acquired when
   * doTerminateImpl() executes
   */
  protected def doTerminateImpl(isBottom: Boolean): Unit

  protected final val terminateLock = new Object

  protected final var terminateInitiated = false
  protected final var terminateCompleted = false

  final def hasTerminateStarted = terminateLock.synchronized { terminateInitiated }
  final def hasTerminateFinished = terminateLock.synchronized { terminateCompleted }
}

trait Listener 
  extends HasServiceMode 
  with    CanTerminate {

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

  def port: Int
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

  private var _attachment: Option[AnyRef] = None
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

  def send(data: Array[Byte]): Unit

  def send(data0: Array[Byte], data1: Array[Byte]): Unit

}

trait MessageConnection extends Connection {

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

  def send(f: Serializer => AnyRef): Unit 

  def send(msg: AnyRef) { send { _: Serializer => msg } }

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
              serializer: Serializer, 
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
