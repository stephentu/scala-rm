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


trait Listener extends HasServiceMode with CanTerminate {
  def port: Int
}

trait Connection extends HasServiceMode with CanTerminate with TerminationHandlers {

  def serializer: Serializer

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

trait ServiceProvider extends HasServiceMode with CanTerminate {
  def connect(node: Node, serializer: Serializer): Connection
  def listen(port: Int): Listener
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

  private val nonBlockingService = serviceProviderFor ServiceMode.NonBlocking
  private val blockingService    = serviceProviderFor ServiceMode.Blocking

  private def serviceProviderFor0(mode: ServiceMode.Value) = mode match {
    case ServiceMode.NonBlocking => nonBlockingService
    case ServiceMode.Blocking    => blockingService
  }

  private val connections = new HashMap[(Node, Serializer, Mode), Connection]

  def connect(node: Node, serializer: Serializer, mode: ServiceMode.Value): Connection = synchronized {
    connections.get((node, serializer, mode)) match {
      case Some(conn) => conn
      case None =>
        val newConn = serviceProviderFor0(mode).connect(node, serializer)
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
    serviceProviderFor0(mode).listen(port)
  }

  /**
   * Stop listening on this port 
   */
  def unlisten(port: Int) {
    // TODO: error handle
    listeners(port).stopListening
  }

  override def terminate() {

  }

}
