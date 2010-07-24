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

object ConnectionStatus {
  val WaitingForSerializer = 0x1
  val Handshaking          = 0x2
  val Established          = 0x3
  val Terminated           = 0x4
}

class HandshakeState(serializer: Serializer[_]) {
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

class DefaultMessageConnection(byteConn: ByteConnection, 
                               var serializer: Option[Serializer[Proxy]],
                               override val receiveCallback: MessageReceiveCallback,
                               isServer: Boolean)
  extends MessageConnection {

  import ConnectionStatus._

  // setup termination chain
  byteConn.beforeTerminate { isBottom => doTerminate(isBottom) }

  override def localNode  = byteConn.localNode
  override def remoteNode = byteConn.remoteNode

  override def doTerminateImpl(isBottom: Boolean) {
    Debug.info(this + ": doTerminateImpl(" + isBottom + ")")
    if (!isBottom && !sendQueue.isEmpty) {
      Debug.info(this + ": waiting 5 seconds for sendQueue to drain")
      terminateLock.wait(5000)
    }
    byteConn.doTerminate(isBottom)
    status = Terminated
  }

  override def mode             = byteConn.mode
  override def activeSerializer = serializer.get
  override def toString         = "<DefaultMessageConnection using: " + byteConn + ">"
    
  @volatile private var _status = 0
  def status = _status
  private def status_=(newStatus: Int) { _status = newStatus }

  private var handshakeState: Option[HandshakeState] = serializer.map(new HandshakeState(_))

  def isWaitingForSerializer = status == WaitingForSerializer
  def isHandshaking          = status == Handshaking
  def isEstablished          = status == Established

  /**
   * Messages which we have received, for processing
   */
  private val messageQueue = new Queue[Array[Byte]]
  /**
   * Messages which need to be sent out (in order), but could not have been at
   * the time send() was called (due to things like not finishing handshake
   * yet, etc)
   */
  private val sendQueue = new Queue[Serializer[Proxy] => AnyRef]
  private val primitiveSerializer = new PrimitiveSerializer

  // bootstrap in CTOR
  if (isServer)
    bootstrapServer()
  else
    bootstrapClient()

  assert(status != 0)

  private def bootstrapClient() {
    // clients start out with a serializer defined
    assert(serializer.isDefined)

    // send class name of serializer to remote side
    //Debug.info(this + ": sending serializer name: " + serializer.get.getClass.getName)
    byteConn.send(serializer.get.getClass.getName.getBytes)

    if (!handshakeState.get.isDone) {
      // if serializer requires handshake, place in handshake mode...
      status = Handshaking
      // ... and send the first message from this end
      sendNextMessage()
    } else 
      // otherwise, in established mode (ready to send messages)
      status = Established 
  }

  private def bootstrapServer() {
    // servers start out with no serializer defined
    assert(!serializer.isDefined)
    status = WaitingForSerializer
  }

  // assumes lock on terminateLock is held (except in the bootstrap phase)
  private def sendNextMessage() {
    assert(isHandshaking)
    handshakeState.get.nextHandshakeMessage() match {
      case Some(msg) =>
        //Debug.info(this + ": nextHandshakeMessage: " + msg.asInstanceOf[AnyRef])
        val meta = primitiveSerializer.serializeMetaData(msg.asInstanceOf[AnyRef])
        val data = primitiveSerializer.serialize(msg.asInstanceOf[AnyRef])
        byteConn.send(meta.get, data)
      case None =>
        // done
        status = Established
        if (!sendQueue.isEmpty) {
          sendQueue.foreach { f =>
            val msg = f(serializer.get)
            //Debug.info(this + ": serializing " + msg + " from sendQueue")
            val t = serialize(msg)
            byteConn.send(t._1, t._2)
          }
          sendQueue.clear()
          terminateLock.notifyAll()
        }
        Debug.info(this + ": handshake completed")
    }
  }

  def receive(bytes: Array[Byte]) {
    assert(status != 0)

    //Debug.info(this + ": received " + bytes.length + " bytes")
    messageQueue += bytes
    while (hasNextAction) {
      if (isWaitingForSerializer) {
        try {
          val clzName = new String(nextMessage())
          //Debug.info(this + ": going to create serializer of clz " + clzName)
          val _serializer = Class.forName(clzName).newInstance.asInstanceOf[Serializer[Proxy]]
          serializer = Some(_serializer)
          handshakeState = Some(new HandshakeState(_serializer))

          // same logic as in bootstrapClient()
          if (!handshakeState.get.isDone) {
            terminateLock.synchronized {
              if (terminateInitiated) return
              status = Handshaking
              sendNextMessage()
            }
          } else 
            terminateLock.synchronized {
              if (terminateInitiated) return
              status = Established 
            }
        } catch {
          case e: InstantiationException =>
            Debug.error(this + ": could not instantiate class: " + e.getMessage)
            Debug.doError { e.printStackTrace }
            terminateBottom()
          case e: ClassNotFoundException =>
            Debug.error(this + ": could not find class: " + e.getMessage)
            Debug.doError { e.printStackTrace }
            terminateBottom()
          case e: ClassCastException =>
            Debug.error(this + ": could not cast class to Serializer: " + e.getMessage)
            Debug.doError { e.printStackTrace }
            terminateBottom()
        }
      } else if (isHandshaking) {
        val msg = nextPrimitiveMessage()
        //Debug.info(this + ": receive() - nextPrimitiveMessage(): " + msg)
        handshakeState.get.handleNextMessage(msg)
        terminateLock.synchronized { 
          if (terminateInitiated) return
          sendNextMessage()
        }
      } else if (isEstablished) {
        val nextMsg = nextSerializerMessage()
        //Debug.info(this + ": calling receiveMessage with " + nextMsg)
        receiveMessage(serializer.get, nextMsg)
      } else {
        Debug.error(this + ": hasNextAction returned true but no action can be taken")
      }
    }
  }

  private val EmptyArray = new Array[Byte](0)

  private def serialize(msg: AnyRef) = serializer match {
    case Some(s) => (s.serializeMetaData(msg).getOrElse(EmptyArray), s.serialize(msg))
    case None =>
      throw new IllegalStateException("Cannot serialize message, no serializer agreed upon")
  }



  def send(msg: Serializer[Proxy] => AnyRef) {
    ensureAlive()
    if (isWaitingForSerializer || isHandshaking) {
      val repeat = withoutTermination {
        status match {
          case Terminated =>
            throw new IllegalStateException("Cannot send on terminated channel")
          case WaitingForSerializer | Handshaking =>
            //Debug.info(this + ": send() - queuing up msg")
            sendQueue += msg // queue it up
            false // no need to repeat
          case Established =>
            // we connected somewhere in between checking and grabbing
            // termination lock. we don't want to queue it up then. try again
            true
        }
      }
      if (repeat) send(msg)
    } else {
      // call send immediately
      val m = msg(serializer.get)
      //Debug.info(this + ": send() - serializing message: " + m)
      val t = serialize(m)
      byteConn.send(t._1, t._2)
    }
  }

  private def hasNextAction = status match {
    case WaitingForSerializer => hasSimpleMessage
    case Handshaking | Established => hasSerializerMessage
    case _ => false
  }

  private def hasSimpleMessage = messageQueue.size > 0
  private def hasSerializerMessage = messageQueue.size >= 2

  private def nextMessage() = {
    assert(hasSimpleMessage)
    messageQueue.dequeue()
  }

  private def nextMessageTuple() = {
    assert(hasSerializerMessage)
    val meta = messageQueue.dequeue()
    val data = messageQueue.dequeue()
    (meta, data)
  }

  private def nextPrimitiveMessage() = {
    assert(hasSerializerMessage)
    val (meta, data) = nextMessageTuple()
    primitiveSerializer.deserialize(Some(meta), data)
  }

  private def nextSerializerMessage() = {
    assert(hasSerializerMessage)
    val (meta, data) = nextMessageTuple()
    serializer.get.deserialize(Some(meta), data)
  }


}

class StandardService extends Service {

  override def serviceProviderFor(mode: ServiceMode.Value) = mode match {
    case ServiceMode.NonBlocking => new NonBlockingServiceProvider
    case ServiceMode.Blocking    => new BlockingServiceProvider
  }

  private val recvCall0 = (conn: ByteConnection, bytes: Array[Byte]) => {
    conn.attachment_!.asInstanceOf[DefaultMessageConnection].receive(bytes)
  }

  override def connect(node: Node, 
                       serializer: Serializer[Proxy], 
                       mode: ServiceMode.Value, 
                       recvCallback: MessageReceiveCallback): MessageConnection = {
    val byteConn = serviceProviderFor0(mode).connect(node, recvCall0)
    val msgConn = new DefaultMessageConnection(byteConn, Some(serializer), recvCallback, false)
    byteConn.attach(msgConn)
    msgConn
  }

  override def listen(port: Int, 
										  mode: ServiceMode.Value, 
											connCallback: ConnectionCallback[MessageConnection],
											recvCallback: MessageReceiveCallback): Listener = {
    val byteConnCallback = (listener: Listener, byteConn: ByteConnection) => {
      val msgConn = new DefaultMessageConnection(byteConn, None, recvCallback, true)
      byteConn.attach(msgConn)
			connCallback(listener, msgConn)	
    }
    serviceProviderFor0(mode).listen(port, byteConnCallback, recvCall0)
  }

}
