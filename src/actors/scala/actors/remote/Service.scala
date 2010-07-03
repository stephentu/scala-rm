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
 * Mode of operation. Can be blocking or non-blocking
 */
object ServiceMode extends Enumeration {
  val Blocking, NonBlocking = Value
}

trait HasServiceMode {
  def mode: ServiceMode
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
    handler += () => f
  }
  def invokeHandlers() {
    handlers.foreach(f => f())
  }
}

trait TerminateOnError { this: CanTerminate =>
  protected def terminateOnError(f: => Unit) {
    try {
      f
    } catch {
      case e: Exception =>
        Debug.error(this + ": caught " + e.getMessage)
        Debug.doError { e.printStackTrace }
        terminate()
    }
  }
}

trait TerminationHandlers { this: CanTerminate =>

  private val preTerminateHandlers  = new HandlerGroup
  private val postTerminateHandlers = new HandlerGroup

  protected def doTerminate(): Unit
  
  override def terminate() {
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

trait ReceiveCallbackAware {
  type ReceiveCallback = (Connection, Array[Byte]) => Unit
}

trait Listener 
  extends HasServiceMode 
  with    CanTerminate
  with    ConnectionCallbackAware {

  protected val connectionCallback: ConnectionCallback

  protected def receiveConnection(conn: Connection) {
    connectionCallback(this, conn)
  }

  def port: Int
}

trait Connection 
  extends HasServiceMode 
  with    CanTerminate 
  with    TerminationHandlers 
  with    ReceiveCallbackAware {

  protected val receiveCallback: ReceiveCallback

  protected def receiveBytes(bytes: Array[Byte]) {
    receiveCallback(this, bytes)
  }

  /**
   * Returns the (canonical) remote node
   */
  def remoteNode: Node

  /**
   * Returns the (canonical) local node
   */
  def localNode: Node

  /**
   * Send data down the wire. There are no specified semantics of
   * send other than the data arrives in the same order (for instance,
   * send could be async or not, it is implementation dependent) and 
   * together on the wire
   */
  def send(data: Array[Byte]*): Unit

}

trait ConnectionCallbackAware {
  type ConnectionCallback = (Listener, Connection) => Unit
}

trait ServiceProvider 
  extends HasServiceMode 
  with    CanTerminate 
  with    ReceiveCallbackAware 
  with    ConnectionCallbackAware {

  def connect(node: Node, receiveCallback: ReceiveCallback): Connection
  def listen(port: Int, connectionCallback: ConnectionCallback, receiveCallback: ReceiveCallback): Listener

}

trait EncodingHelpers {
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
  protected def encode(unenc: Array[Byte]): Array[Byte] = {
    // TODO: handle overflow
    val baos    = new ByteArrayOutputStream(unenc.length + 4)
    val dataout = new DataOutputStream(baos)
    dataout.writeInt(unenc.length)
    dataout.write(unenc)
    baos.toByteArray
  }
}

/**
 * @version 0.9.10
 * @author Philipp Haller
 */
trait Service extends CanTerminate {

  protected def serviceProviderFor(mode: ServiceMode.Value): ServiceProvider

  private var nonBlockServiceSet = false
  private var blockServiceSet    = false

  private lazy val nonBlockingService = {
    nonBlockServiceSet = true
    serviceProviderFor ServiceMode.NonBlocking
  }

  private lazy val blockingService = {
    blockServiceSet = true
    serviceProviderFor ServiceMode.Blocking
  }

  private def serviceProviderFor0(mode: ServiceMode.Value) = mode match {
    case ServiceMode.NonBlocking => nonBlockingService
    case ServiceMode.Blocking    => blockingService
  }

  private val connections = new HashMap[(Node, Serializer, Mode), Connection]

  def connect(node: Node, serializer: Serializer, mode: ServiceMode.Value): Connection = connections.synchronized {
    connections.get((node, serializer, mode)) match {
      case Some(conn) => conn
      case None =>
        val newConn = serviceProviderFor0(mode).connect(node, /* TODO: CALLBACK */ null)
        connections += (node, serializer, mode) -> newConn
        newConn.preTerminate { connections -= (node, serializer, mode) }
        newConn
    }
  }

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
          val listener = serviceProviderFor0(mode).listen(port)
          listerens += port -> listener
          listener
      }
    }
  }

  /**
   * Stop listening on this port 
   */
  def unlisten(port: Int) {
    listeners.synchronized [
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
