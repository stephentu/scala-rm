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

trait CanTerminate {
  /**
   * Can assume that terminate() will only be invoked at most once
   */
  def terminate(): Unit
}

private class HandlerGroup {
  val handlers = new ListBuffer[() => Unit]
  def addHandler(f: => Unit) {
    handlers += (() => f)
  }
  def invokeHandlers() {
    handlers.foreach(f => f())
  }
}

trait TerminationHandlers extends CanTerminate {

  private val preTerminateHandlers  = new HandlerGroup
  private val postTerminateHandlers = new HandlerGroup

  protected def doTerminate(): Unit
  
  final override def terminate() {
    preTerminateHandlers.invokeHandlers()
    doTerminate() 
    postTerminateHandlers.invokeHandlers() 
  }

  def preTerminate(f: => Unit) {
    preTerminateHandlers addHandler f
  }

  def postTerminate(f: => Unit) {
    postTerminateHandlers addHandler f
  }

}

trait BytesReceiveCallbackAware {
  type BytesReceiveCallback = (Connection, Array[Byte]) => Unit
}

trait ObjectReceiveCallbackAware {
  type ObjectReceiveCallback = (Connection, Serializer, AnyRef) => Unit
}

trait Listener 
  extends HasServiceMode 
  with    TerminationHandlers 
  with    ConnectionCallbackAware {

  protected val connectionCallback: ConnectionCallback

  protected def receiveConnection(conn: Connection) {
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
  with    TerminationHandlers 
  with    BytesReceiveCallbackAware {

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

  /**
   * Returns the (canonical) remote node
   */
  def remoteNode: Node

  /**
   * Returns the (canonical) local node
   */
  def localNode: Node

  def send(data: Array[Byte]): Unit

  def send(data0: Array[Byte], data1: Array[Byte]): Unit

  var attachment: Option[AnyRef] = None

}

trait ConnectionCallbackAware {
  type ConnectionCallback = (Listener, Connection) => Unit
}

trait ServiceProvider 
  extends HasServiceMode 
  with    CanTerminate 
  with    BytesReceiveCallbackAware 
  with    ConnectionCallbackAware {

  def connect(node: Node, receiveCallback: BytesReceiveCallback): Connection
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
abstract class Service(objectReceiveCallack: (Connection, Serializer, AnyRef) => Unit)
  extends CanTerminate 
  with    BytesReceiveCallbackAware 
  with    ConnectionCallbackAware 
  with    ObjectReceiveCallbackAware {

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

  private def serviceProviderFor0(mode: ServiceMode.Value) = mode match {
    case ServiceMode.NonBlocking => nonBlockingService
    case ServiceMode.Blocking    => blockingService
  }

  protected def receiveCallback: BytesReceiveCallback
  protected def connectionCallback: ConnectionCallback

  private val connections = new HashMap[(Node, Serializer, ServiceMode.Value), Connection]

  protected def attachmentFor(newConn: Connection, serializer: Serializer): Option[AnyRef]

  def connect(node: Node, 
              serializer: Serializer, 
              mode: ServiceMode.Value): Connection = 
    connections.synchronized {
      connections.get((node, serializer, mode)) match {
        case Some(conn) => conn
        case None =>
          val newConn = serviceProviderFor0(mode).connect(node, receiveCallback)
          connections += (node, serializer, mode) -> newConn
          newConn.preTerminate { 
            connections.synchronized {
              connections -= ((node, serializer, mode))
            }
          }
          newConn.attachment = attachmentFor(newConn, serializer)
          newConn
      }
    }

  def send(conn: Connection)(f: Serializer => AnyRef): Unit 

  def send(conn: Connection, msg: AnyRef) { send(conn) { _ => msg } }

  private val listeners = new HashMap[Int, Listener]

  /**
   * Start listening on this port, using mode
   */
  def listen(port: Int, mode: ServiceMode.Value) {
    listeners.synchronized { 
      listeners.get(port) match {
        case Some(listener) =>
          // do nothing
        case None =>
          val listener = serviceProviderFor0(mode).listen(port, connectionCallback, receiveCallback)
          listeners += port -> listener
          listener
      }
    }
  }

  /**
   * Stop listening on this port 
   */
  def unlisten(port: Int) {
    listeners.synchronized {
      listeners.get(port) match {
        case Some(listener) =>
          listener.terminate()
        case None =>
          throw new IllegalArgumentException("No listener on port " + port)
      }
    }
  }

  override def terminate() {

    if (nonBlockServiceSet)
      nonBlockingService.terminate()

    if (blockServiceSet)
      blockingService.terminate()

  }

}

class ProviderAlreadyClosedException extends RuntimeException
class ConnectionAlreadyClosedException extends RuntimeException
