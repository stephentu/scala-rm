/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

import scala.collection.mutable.{ HashMap, Queue }

object ConnectionStatus extends Enumeration {
  val New, 
      WaitingForSerializer,
      Handshaking, 
      Established, 
      Terminated = Value
}

class HandshakeState(serializer: Serializer) {
  private var curState = serializer.initialState.getOrElse(null)
  private var done     = serializer.initialState.isEmpty

  def isDone = done

  private var sending  = true

  def isSending   = sending
  def isReceiving = !isSending

  def nextHandshakeMessage() = {
    assert(!done && isSending) 
    if (serializer.nextHandshakeMessage.isDefinedAt(curState)) {
      val (nextState, nextMsg) = serializer.nextHandshakeMessage.apply(curState)
      curState = nextState
      flip()
      nextMsg match {
        case Some(_) => nextMsg
        case None    =>
          done = true
          None
      }
    } else throw new IllegalHandshakeStateException
  }

  def flip() {
    sending = !sending
  }

  def handleNextMessage(m: Any) {
    assert(!done && isReceiving)
    if (serializer.handleHandshakeMessage.isDefinedAt((curState, m))) {
      val nextState = serializer.handleHandshakeMessage((curState, m))
      curState = nextState
      flip()
    } else throw new IllegalHandshakeStateException
  }


}

class ConnectionState(conn: Connection, 
                      var serializer: Option[Serializer],
                      objectReceiveCallback: (Connection, Serializer, AnyRef) => Unit) {

  private var _status: ConnectionStatus.Value = ConnectionStatus.New
  def status = _status
  def status_=(newStatus: ConnectionStatus.Value) { _status = newStatus }

  var handshakeState: Option[HandshakeState] = serializer.map(new HandshakeState(_))

  def isWaitingForSerializer = status == ConnectionStatus.WaitingForSerializer
  def isNew         = status == ConnectionStatus.New
  def isHandshaking = status == ConnectionStatus.Handshaking
  def isEstablished = status == ConnectionStatus.Established

  private val messageQueue = new Queue[Array[Byte]]

  def bootstrapClient() {
    // clients start out in New mode, and with a serializer defined
    assert(isNew && serializer.isDefined)

    // send class name of serializer to remote side
    conn.send(serializer.get.getClass.getName.getBytes)

    if (!handshakeState.get.isDone) {
      // if serializer requires handshake, place in handshake mode...
      synchronized { status = ConnectionStatus.Handshaking }
      // ... and send the first message from this end
      sendNextMessage()
    } else 
      // otherwise, in established mode (ready to send messages)
      synchronized { status = ConnectionStatus.Established }
  }

  def bootstrapServer() {
    // servers start out in New mode, with no serializer defined
    assert(isNew && !serializer.isDefined)

    synchronized { status = ConnectionStatus.WaitingForSerializer }
  }

  def sendNextMessage() {
    assert(isNew || isHandshaking)
    handshakeState.get.nextHandshakeMessage() match {
      case Some(msg) =>
        val data = serializer.get.javaSerialize(msg.asInstanceOf[AnyRef])
        conn.send(data)
      case None =>
        // done
        synchronized { 
          status = ConnectionStatus.Established 
          // run this synchronized b/c we try to respect the order for which
          // messages queued up in send queue
          sendQueue.foreach { msg => 
            val t = serialize(msg)
            conn.send(t._1, t._2)
          }
        }
    }
  }

  def receive(bytes: Array[Byte]) {
    messageQueue enqueue bytes
    while (hasNextAction) {
      if (isWaitingForSerializer) {
        val clzName = new String(nextMessage())
        val _serializer = Class.forName(clzName).newInstance.asInstanceOf[Serializer]
        serializer = Some(_serializer)
        handshakeState = Some(new HandshakeState(_serializer))

        // same logic as in bootstrapClient()
        if (!handshakeState.get.isDone) {
          synchronized { status = ConnectionStatus.Handshaking }
          sendNextMessage()
        } else 
          synchronized { status = ConnectionStatus.Established }
      } else if (isHandshaking) {
        handshakeState.get.handleNextMessage(nextJavaMessage())
        sendNextMessage()
      } else if (isEstablished) {
        objectReceiveCallback(conn, serializer.get, nextSerializerMessage())   
      }
    }
  }

  private def serialize(msg: AnyRef) = serializer match {
    case Some(s) => (s.serializeMetaData(msg).getOrElse(Array[Byte]()), s.serialize(msg))
    case None =>
      throw new IllegalStateException("Cannot serialize message, no serializer agreed upon")
  }

  /**
   * Messages which need to be sent out (in order), but could not have been at
   * the time send() was called (due to things like not finishing handshake
   * yet, etc)
   */
  private val sendQueue = new Queue[Serializer => AnyRef]

  def send(msg: Serializer => AnyRef) {
    val doSend = synchronized {
      status match {
        case ConnectionStatus.New =>
          throw new IllegalStateException("should not be NEW")
        case ConnectionStatus.Terminated =>
          throw new IllegalStateException("Cannot send on terminated channel")
        case ConnectionStatus.WaitingForSerializer | ConnectionStatus.Handshaking =>
          sendQueue enqueue msg      
          false
        case ConnectionStatus.Established => true
      }
    }
    if (doSend) {
      val t = serialize(msg(serializer.get))
      conn.send(t._1, t._2)
    }
  }

  private def hasNextAction = status match {
    case ConnectionStatus.WaitingForSerializer | ConnectionStatus.Handshaking =>
      hasSimpleMessage
    case ConnectionStatus.Established =>
      hasSerializerMessage
    case _ => false
  }

  private def hasSimpleMessage = !messageQueue.isEmpty
  private def hasSerializerMessage = messageQueue.size >= 2

  private def nextMessage() = {
    assert(hasSimpleMessage)
    messageQueue dequeue
  }

  private def nextMessageTuple() = {
    assert(hasSerializerMessage)
    val meta = messageQueue.dequeue
    val data = messageQueue.dequeue
    (meta, data)
  }

  private def nextJavaMessage() = {
    assert(hasSimpleMessage)
    serializer.get.javaDeserialize(nextMessage())
  }

  private def nextSerializerMessage() = {
    assert(hasSerializerMessage)
    val (meta, data) = nextMessageTuple()
    serializer.get.deserialize(Some(meta), data)
  }


}

class StandardService(objectReceiveCallback: (Connection, Serializer, AnyRef) => Unit) 
  extends Service(objectReceiveCallback) {

  private def withState(conn: Connection)(f: ConnectionState => Unit) {
    if (conn.attachment eq null)
      throw new IllegalArgumentException("conn does not have attachment")
    if (!conn.attachment.isInstanceOf[ConnectionState])
      throw new IllegalArgumentException("Attachment is not a connection state")
    f(conn.attachment.asInstanceOf[ConnectionState])
  }

  override val receiveCallback = (conn: Connection, bytes: Array[Byte]) => {
    withState(conn) { state => state.receive(bytes) }
  }

  override val connectionCallback = (listener: Listener, conn: Connection) => {
    val state = new ConnectionState(conn, None, objectReceiveCallback)
    state.bootstrapServer()
    conn.attachment = Some(state)
  }

  override def attachmentFor(newConn: Connection, serializer: Serializer) = {
    val newConnState = new ConnectionState(newConn, Some(serializer), objectReceiveCallback)
    newConnState.bootstrapClient()
    Some(newConnState)
  }

  override def serviceProviderFor(mode: ServiceMode.Value) = mode match {
    case ServiceMode.NonBlocking => new NonBlockingServiceProvider
    case ServiceMode.Blocking    => new BlockingServiceProvider
  }

  override def send(conn: Connection)(f: Serializer => AnyRef) {
    withState(conn) { state => state.send(f) }
  }

}
