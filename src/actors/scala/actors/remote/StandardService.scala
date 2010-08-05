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

class DefaultMessageConnection(byteConn: ByteConnection, 
                               var serializer: Option[Serializer],
                               override val receiveCallback: MessageReceiveCallback,
                               isServer: Boolean,
                               config: Configuration)
  extends MessageConnection {

  import ConnectionStatus._

  // setup termination chain
  byteConn.beforeTerminate { isBottom => doTerminate(isBottom) }

  override def localNode  = byteConn.localNode
  override def remoteNode = byteConn.remoteNode

  override def isEphemeral = byteConn.isEphemeral

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
  private val sendQueue = new Queue[Serializer => ByteSequence]
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
    //Debug.info(this + ": sending serializer name: " + serializer.get.bootstrapClassName)
    byteConn.send(new ByteSequence(serializer.get.bootstrapClassName.getBytes))

    if (serializer.get.isHandshaking) {
      // if serializer requires handshake, place in handshake mode...
      status = Handshaking

      // ... and initialize it
      handleNextEvent(StartEvent(remoteNode))
    } else 
      // otherwise, in established mode (ready to send messages)
      status = Established 
  }

  private def bootstrapServer() {
    // servers start out with no serializer defined
    assert(!serializer.isDefined)
    status = WaitingForSerializer
  }

  private def handleNextEvent(evt: ReceivableEvent) {
    assert(isHandshaking)
    def sendIfNecessary(m: TriggerableEvent) {
      (m match {
        case SendEvent(msgs @ _*)             => msgs
        case SendWithSuccessEvent(msgs @ _*)  => msgs 
        case SendWithErrorEvent(_, msgs @ _*) => msgs
        case _ => Seq()
      }) foreach { msg =>
        //Debug.info(this + ": nextHandshakeMessage: " + msg.asInstanceOf[AnyRef])
        val data = primitiveSerializer.serialize(msg.asInstanceOf[AnyRef])
        Debug.info(this + ": sending in handshake: data: " + java.util.Arrays.toString(data))
        byteConn.send(new ByteSequence(data))
      }
    }
    serializer.get.handleNextEvent(evt).foreach { evt =>
      sendIfNecessary(evt)
      evt match {
        case SendEvent(_*) =>
        case SendWithSuccessEvent(_*) | Success =>
          // done
          status = Established
          if (!sendQueue.isEmpty) {
            sendQueue.foreach { f =>
              val msg = f(serializer.get)
              Debug.info(this + ": sending " + msg + " from sendQueue")
              byteConn.send(msg)
            }
            sendQueue.clear()
            terminateLock.notifyAll()
          }
          Debug.info(this + ": handshake completed")
        case SendWithErrorEvent(reason, _*) =>
          throw new IllegalHandshakeStateException(reason)
        case Error(reason) =>
          throw new IllegalHandshakeStateException(reason)
      }
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
          val _serializer = Class.forName(clzName, true, config.classLoader)
            .newInstance.asInstanceOf[Serializer]
          serializer = Some(_serializer)

          // same logic as in bootstrapClient()
          if (_serializer.isHandshaking) {
            terminateLock.synchronized {
              if (terminateInitiated) return
              status = Handshaking
              handleNextEvent(StartEvent(remoteNode))
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
        terminateLock.synchronized { 
          if (terminateInitiated) return
          handleNextEvent(RecvEvent(msg))
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

  def send(msg: Serializer => ByteSequence) {
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
      Debug.info(this + ": send() - serializing message: " + m)
      byteConn.send(m)
    }
  }

  @inline private def hasNextAction = status match {
    case WaitingForSerializer | Established | Handshaking => hasSimpleMessage
    case _ => false
  }

  @inline private def hasSimpleMessage = messageQueue.size > 0

  @inline private def nextMessage() = {
    assert(hasSimpleMessage)
    messageQueue.dequeue()
  }

  @inline private def nextPrimitiveMessage() =
    primitiveSerializer.deserialize(nextMessage())

  @inline private def nextSerializerMessage() =
		serializer.get.read(nextMessage())
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
                       config: Configuration,
                       recvCallback: MessageReceiveCallback): MessageConnection = {
    val byteConn = serviceProviderFor0(config.selectMode).connect(node, recvCall0)
    val msgConn = new DefaultMessageConnection(byteConn, Some(config.newSerializer()), recvCallback, false, config)
    byteConn.attach(msgConn)
    msgConn
  }

  override def listen(port: Int, 
                      config: Configuration,
                      connCallback: ConnectionCallback[MessageConnection], 
                      recvCallback: MessageReceiveCallback): Listener = {
    val byteConnCallback = (listener: Listener, byteConn: ByteConnection) => {
      val msgConn = new DefaultMessageConnection(byteConn, None, recvCallback, true, config)
      byteConn.attach(msgConn)
			connCallback(listener, msgConn)	
    }
    serviceProviderFor0(config.aliveMode).listen(port, byteConnCallback, recvCall0)
  }

}
