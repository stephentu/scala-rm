/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

import scala.collection.mutable.{ HashMap, HashSet }
import java.io._
import java.util.concurrent.{ ConcurrentHashMap, ConcurrentLinkedQueue, 
                              CountDownLatch, TimeUnit }

// TODO: make package private?
case object Terminate

/**
 * @version 1.0.0
 * @author Philipp Haller
 * @author Stephen Tu
 */
private[remote] object NetKernel {

  @volatile private var _service = new StandardService

  /**
   * Connection uniquely identified by its canonical node and its mode. That
   * means the serializer MUST agree across modes (I can have at most two
   * connections to a unique port, one in blocking, one in nonblocking, but
   * the serializer is the same for both). This constraint exists because a
   * listening endpoint is tied to a serializer instance
   */
  private val connections = new ConcurrentHashMap[(Node, ServiceMode.Value), MessageConnection]

  /**
   * Makes connection if one does not already exist, checking the serializer
   * consistency
   */
  @throws(classOf[InconsistentSerializerException])
  def getConnectionFor(node: Node, cfg: Configuration) = {
    // Check the serializers to see if they match
    def checkConn(testConn: MessageConnection) = {
      if (cfg.cachedSerializer != testConn.activeSerializer)
        throw new InconsistentSerializerException(testConn.activeSerializer, cfg.cachedSerializer)
      if (cfg.connectPolicy == ConnectPolicy.WaitEstablished)
        testConn.connectFuture.await(cfg.waitTimeout)
      else if (cfg.connectPolicy == ConnectPolicy.WaitHandshake ||
               cfg.connectPolicy == ConnectPolicy.WaitVerified)
        testConn.connectFuture chainWith testConn.handshakeFuture await (cfg.waitTimeout)
      testConn
    }
    val key = (node, cfg.selectMode)
    val testConn = connections.get(key)
    if (testConn ne null)
      checkConn(testConn)
    else {
      val newConn = connections.synchronized {
        val testConn0 = connections.get(key)
        if (testConn0 ne null) testConn0
        else {
          val conn = _service.connect(node, cfg, processMsgFunc)
          conn beforeTerminate { isBottom =>
            Debug.info(this + ": removing connection with key: " + key)
            connections.remove(key)
          }
          connections.put(key, conn)
          conn
        }
      }
      checkConn(newConn)
    }
  }

  private val listeners = new ConcurrentHashMap[Int, Listener]

  @throws(classOf[InconsistentSerializerException])
  @throws(classOf[InconsistentServiceException])
  def getListenerFor(port: Int, cfg: Configuration) = {
    def ensureListener(listener: Listener) = {
      if (listener.mode != cfg.aliveMode)
        throw new InconsistentServiceException(cfg.aliveMode, listener.mode)
      val listenerSerializer = listener.attachment_!.asInstanceOf[Configuration].cachedSerializer
      if (listenerSerializer != cfg.cachedSerializer)
        throw new InconsistentSerializerException(listenerSerializer, cfg.cachedSerializer)
      listener
    }
    val testListener = listeners.get(port)
    if (testListener ne null)
      ensureListener(testListener)
    else {
      listeners.synchronized {
        val testListener0 = listeners.get(port)
        if (testListener0 ne null)
          ensureListener(testListener0)
        else {
          val listener = _service.listen(port, cfg, msgConnCallback, processMsgFunc)
          listener.attach(cfg)
          val realPort = listener.port
          listener beforeTerminate { isBottom =>
            listeners.remove(realPort)
          }
          listeners.put(realPort, listener)
          listener
        }
      }
    }
  }

  def unlisten(port: Int) {
    Debug.info(this + ": unlisten() - port " + port)
    val listener = listeners.remove(port)
    if (listener eq null)
      Debug.info(this + ": unlisten() - no listener on port " + port)
    else
      listener.terminateTop()
  }

  def releaseResources() {
    listeners.synchronized {
      connections.synchronized {
        Debug.info(this + ": releaseResources()")
        import scala.collection.JavaConversions._
        connections.values.foreach(_.terminateTop())
        connections.clear()

        listeners.values.foreach(_.terminateTop())
        listeners.clear()

        _service.terminateTop()
        _service = new StandardService
      }
    }
  }

	final private val BufSize = 1024

	// TODO: don't expose baos, expose a locked version where reset() throws an
	// exception

  @inline private def makeFuture(from: Option[Reactor], blockingCond: Boolean): Option[Future] = {
    if (blockingCond)
      Some(new BlockingFuture)
    else
      from.map(f => new ErrorCallbackFuture((t: Throwable) => {
        if (t.isInstanceOf[Exception]) {
          val ex = t.asInstanceOf[Exception]
          if (f.exceptionHandler.isDefinedAt(ex))
            f.exceptionHandler(ex)
        } else {
          Debug.error("Got throwable error: " + t)
          Debug.doError { t.printStackTrace() }
        }
      }))
  }

  @inline private def makeSendFuture(from: Reactor, sendPolicy: SendPolicy.Value) = 
    makeFuture(from, sendPolicy == SendPolicy.WaitWritten)

  @inline private def makeLocateFuture(from: Reactor, connectPolicy: ConnectPolicy.Value) =
    makeFuture(from, connectPolicy == ConnectPolicy.WaitVerified)

  def asyncSend(conn: MessageConnection, toName: String, from: Option[Reactor], msg: AnyRef) {
    val fromName = from.map(f => RemoteActor.getOrCreateName(f).name)
    val config = conn.config
    val ftch = makeSendFuture(from, config.sendPolicy)
    conn.send(ftch) { serializer: Serializer =>
			val baos = new ExposingByteArrayOutputStream(BufSize)
      baos.writeZeros(4)
      serializer.writeAsyncSend(baos, fromName.orNull, toName, msg)
			new DiscardableByteSequence(baos.getUnderlyingByteArray, 4, baos.size - 4)
    }
    ftch.map(_.await(config.waitTimeout))
  }

  def syncSend(conn: MessageConnection, toName: String, from: Reactor, msg: AnyRef, session: String) {
    val fromName = RemoteActor.getOrCreateName(from).name
    val config = conn.config
    val ftch = makeSendFuture(Some(from), config.sendPolicy)
    conn.send(Some(ftch)) { serializer: Serializer =>
			val baos = new ExposingByteArrayOutputStream(BufSize)
      baos.writeZeros(4)
      val wireMsg = serializer.intercept(msg)
      serializer.writeSyncSend(baos, fromName, toName, wireMsg, session)
			new DiscardableByteSequence(baos.getUnderlyingByteArray, 4, baos.size - 4)
    }
    ftch.map(_.await(config.waitTimeout))
  }

  def syncReply(conn: MessageConnection, toName: String, msg: AnyRef, session: String) {
    val config = conn.config
    val ftch = makeSendFuture(None, config.sendPolicy)
    conn.send(ftch) { serializer: Serializer =>
			val baos = new ExposingByteArrayOutputStream(BufSize)
      baos.writeZeros(4)
      val wireMsg = serializer.intercept(msg)
      serializer.writeSyncReply(baos, toName, wireMsg, session)
			new DiscardableByteSequence(baos.getUnderlyingByteArray, 4, baos.size - 4)
    }
    ftch.map(_.await(config.waitTimeout))
  }

  def remoteApply(conn: MessageConnection, toName: String, from: Reactor, rfun: RemoteFunction) {
    val fromName = RemoteActor.getOrCreateName(from).name
    val config = conn.config
    val ftch = makeSendFuture(Some(from), config.sendPolicy)
    conn.send(Some(ftch)) { serializer: Serializer =>
			val baos = new ExposingByteArrayOutputStream(BufSize)
      baos.writeZeros(4)
      serializer.writeRemoteApply(baos, fromName, toName, rfun) 
			new DiscardableByteSequence(baos.getUnderlyingByteArray, 4, baos.size - 4)
    }
    ftch.map(_.await(config.waitTimeout))
  }

  private val requestFutures = new ConcurrentHashMap[String, ConcurrentLinkedQueue[Future]]

  def locateRequest(conn: MessageConnection, receiverName: String, from: Option[Reactor]) {
    val config = conn.config
    val ftch = makeLocateFuture(from, config.connectPolicy)
    ftch.foreach(f => {
      val queue0 = requestFutures.get(receiverName)
      val queue = 
        if (queue0 ne null) queue0
        else {
          val newQueue = new ConcurrentLinkedQueue[Future]
          val q = requestFutures.putIfAbsent(receiverName, newQueue)
          if (q ne null) q
          else newQueue
        }
      queue.offer(f)
    })
    conn.send(ftch) { serializer: Serializer =>
			val baos = new ExposingByteArrayOutputStream(BufSize)
      baos.writeZeros(4)
      serializer.writeLocateRequest(baos, receiverName) 
			new DiscardableByteSequence(baos.getUnderlyingByteArray, 4, baos.size - 4)
    }
    ftch.map(_.await(config.waitTimeout))
  }

  private val processMsgFunc = processMsg _

  private val msgConnCallback = (listener: Listener, msgConn: MessageConnection) => {
  }

  private final val NoSender = new Proxy {
    override def remoteNode    = throw new RuntimeException("NoSender")
    override def name          = throw new RuntimeException("NoSender")
    override def conn          = throw new RuntimeException("NoSender")
    override def numRetries    = throw new RuntimeException("NoSender") 

    override def terminateConn(usingConn: MessageConnection) { 
      throw new RuntimeException("NoSender")
    }

    override def handleMessage(m: ProxyCommand) {
      m match {
        case SendTo(out, m) =>
          out.send(m, null)
        case FinishSession(out, msg, session) =>
          // is this an active session?
          RemoteActor.finishChannel(session) match {
            case None =>
              Debug.info(this + ": lost session: " + session)
            // finishes request-reply cycle
            case Some(replyCh) =>
              Debug.info(this + ": finishing request-reply cycle for session: " + session + " on replyCh " + replyCh)
              replyCh ! msg
          }
        case _ =>
          throw new RuntimeException("NoSender cannot handle: " + m)
      }
    }

    override def send(msg: Any, sender: OutputChannel[Any]) {
      throw new RuntimeException("NoSender")
    }
    override def linkTo(to: AbstractActor) { 
      throw new RuntimeException("NoSender")
    }
    override def unlinkFrom(from: AbstractActor) {
      throw new RuntimeException("NoSender")
    }
    override def exit(from: AbstractActor, reason: AnyRef) {
      throw new RuntimeException("NoSender")
    }
    override def toString = "<NoSender>"
  }

  private def processMsg(conn: MessageConnection, serializer: Serializer, msg: AnyRef) {
    msg match {
      case LocateRequest(receiverName) =>
        val found = RemoteActor.getActor(Symbol(receiverName)) ne null
        conn.send(None) { serializer: Serializer => 
          val baos = new ExposingByteArrayOutputStream(BufSize)
          baos.writeZeros(4)
          serializer.writeLocateResponse(baos, receiverName, found) 
          new DiscardableByteSequence(baos.getUnderlyingByteArray, 4, baos.size - 4)
        }
      case LocateResponse(receiverName, found) =>
        val futures = requestFutures.get(receiverName)
        if (futures ne null) {
          var continue = false
          while (continue) {
            val ftch = futures.poll()
            if (ftch eq null)
              continue = false
            else {
              if (found)
                ftch.finishWithSuccess()
              else
                ftch.finishWithError(new RuntimeException("Actor " + receiverName + " was not found"))
            }
          }
        }
      case _ =>
        def mkProxy(senderName: String) = 
          new ConnectionProxy(conn.remoteNode, Symbol(senderName), conn)
        def removeOrphan(a: OutputChannel[Any]) {
          // orphaned actor sitting in hash maps
          Debug.info(this + ": found orphaned (terminated) actor: " + a)
          RemoteActor.unregister(a)
        }
        val (senderName, receiverName) = msg match {
          case AsyncSend(sender, receiver, _) => 
            (Option(sender), receiver) 
          case SyncSend(sender, receiver, _, _) => 
            (Some(sender), receiver)
          case SyncReply(receiver, _, _) => 
            (None, receiver)
          case RemoteApply(sender, receiver, _) => 
            (Some(sender), receiver)
        }
        val a = RemoteActor.getActor(Symbol(receiverName))
        if (a eq null)
          // message is lost
          Debug.info(this+": lost message: " + msg)
        else if (a.isInstanceOf[Actor] && 
                a.asInstanceOf[Actor].getState == Actor.State.Terminated)
          removeOrphan(a)
        else {
          val cmd = msg match {
            case AsyncSend(_, _, message) => 
              SendTo(a, message) 
            case SyncSend(_, _, message, session) => 
              StartSession(a, message, Symbol(session))
            case SyncReply(_, message, session) => 
              FinishSession(a, message, Symbol(session))
            case RemoteApply(_, _, rfun) => 
              LocalApply0(rfun, a.asInstanceOf[AbstractActor])
          }
          senderName.map(n => mkProxy(n)).getOrElse(NoSender).handleMessage(cmd)
        }
    }
  }

}
