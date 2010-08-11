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
import scala.util.Random

import java.io._
import java.util.concurrent.{ ConcurrentHashMap, CountDownLatch, 
                              LinkedBlockingQueue, TimeUnit }

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
      // sanity check
      assert(testConn ne null)

      // check serializers
      if (cfg.cachedSerializer != testConn.activeSerializer)
        throw new InconsistentSerializerException(testConn.activeSerializer, cfg.cachedSerializer)

      // blocking policies
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
        if (testConn0 ne null) // double check
          testConn0 // reuse existing
        else {
          // create new connection
          val conn = _service.connect(node, cfg, processMsgFunc)
          conn beforeTerminate { isBottom =>
            Debug.info(this + ": removing connection with key from NetKernel: " + key)
            connections.remove(key)
          }
          // attach a cache of the results of locate requests. Key is name of
          // remote actor. Value is (true if exists/false otherwise, last
          // timestamp checked)
          conn.attach(new ConcurrentHashMap[Symbol, (Boolean, Long)])
          // now available to other threads
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

  @inline private def makeFuture(from: Option[Reactor[Any]], blockingCond: Boolean): Option[RFuture] = {
    if (blockingCond)
      Some(new BlockingFuture)
    else
      from.filter(f => !f.isInstanceOf[ActorProxy]) // ActorProxy has no exception handler (no way for user to specify one)
          .map(f => new ErrorCallbackFuture((t: Throwable) => {
            if (t.isInstanceOf[Exception]) {
              val ex = t.asInstanceOf[Exception]
              if (f.exceptionHandler.isDefinedAt(ex))
                f.exceptionHandler(ex)
            } else {
              Debug.error("Got unhandlable error: " + t)
              Debug.doError { t.printStackTrace() }
            }
          }))
  }

  @inline private def makeSendFuture(from: Option[Reactor[Any]]) = 
    makeFuture(from, false)

  @inline private def makeLocateFuture(from: Option[Reactor[Any]], connectPolicy: ConnectPolicy.Value) = {
    //Debug.info("makeLocateFuture(): from %s, connectPolicy %s".format(from, connectPolicy))
    makeFuture(from, connectPolicy == ConnectPolicy.WaitVerified)
  }

  def asyncSend(conn: MessageConnection, toName: String, from: Option[Reactor[Any]], msg: AnyRef, config: Configuration) {
    //Debug.info("asyncSend(): to: %s, from: %s, msg: %s".format(toName, from, msg))
    val fromName = from.map(f => RemoteActor.getOrCreateName(f).name)
    val ftch = makeSendFuture(from)
    conn.send(ftch) { serializer: Serializer =>
			val baos = new ExposingByteArrayOutputStream(BufSize)
      baos.writeZeros(4)
      serializer.writeAsyncSend(baos, fromName.orNull, toName, msg)
			new DiscardableByteSequence(baos.getUnderlyingByteArray, 4, baos.size - 4)
    }
    //ftch.map(_.await(config.waitTimeout))
  }

  def syncSend(conn: MessageConnection, toName: String, from: Reactor[Any], msg: AnyRef, session: String, config: Configuration) {
    //Debug.info("syncSend(): to: %s, from: %s, msg: %s, session: %s".format(toName, from, msg, session))
    val fromName = RemoteActor.getOrCreateName(from).name
    val ftch = makeSendFuture(Some(from))
    conn.send(ftch) { serializer: Serializer =>
			val baos = new ExposingByteArrayOutputStream(BufSize)
      baos.writeZeros(4)
      serializer.writeSyncSend(baos, fromName, toName, msg, session)
			new DiscardableByteSequence(baos.getUnderlyingByteArray, 4, baos.size - 4)
    }
    //ftch.map(_.await(config.waitTimeout))
  }

  def syncReply(conn: MessageConnection, toName: String, msg: AnyRef, session: String, config: Configuration) {
    //Debug.info("syncSend(): to: %s, msg: %s, session: %s".format(toName, msg, session))
    val ftch = makeSendFuture(None) /** TODO: can we get this actor? */
    conn.send(ftch) { serializer: Serializer =>
			val baos = new ExposingByteArrayOutputStream(BufSize)
      baos.writeZeros(4)
      serializer.writeSyncReply(baos, toName, msg, session)
			new DiscardableByteSequence(baos.getUnderlyingByteArray, 4, baos.size - 4)
    }
    //ftch.map(_.await(config.waitTimeout))
  }

  def remoteApply(conn: MessageConnection, toName: String, from: Reactor[Any], rfun: RemoteFunction, config: Configuration) {
    //Debug.info("remoteApply(): to: %s, from: %s, rfun: %s".format(toName, from, rfun))
    val fromName = RemoteActor.getOrCreateName(from).name
    val ftch = makeSendFuture(Some(from))
    conn.send(ftch) { serializer: Serializer =>
			val baos = new ExposingByteArrayOutputStream(BufSize)
      baos.writeZeros(4)
      serializer.writeRemoteApply(baos, fromName, toName, rfun) 
			new DiscardableByteSequence(baos.getUnderlyingByteArray, 4, baos.size - 4)
    }
    //ftch.map(_.await(config.waitTimeout))
  }

  private val requestFutures = new ConcurrentHashMap[Long, RFuture]

  // TODO: i dont think randomness here is really necessary. thus could
  // probably just use a volatile long for performance
  private final val random = new Random

  def locateRequest(conn: MessageConnection, receiverName: String, from: Option[Reactor[Any]], proxyFtch: RFuture, config: Configuration) {
    //Debug.info("locateRequest(): receiverName: %s, from: %s".format(receiverName, from))

    val ftch = makeLocateFuture(from, config.connectPolicy)
    val requestId = random.nextLong()

    val callback = (t: Throwable) => {
      proxyFtch.finishWithError(t)
      ftch.foreach(_.finishWithError(t))
    }

    // map to session
    requestFutures.put(requestId, new CallbackFuture(() => {
      proxyFtch.finishSuccessfully()
      ftch.foreach(_.finishSuccessfully())
    }, callback))

    // send via network
    val connFtch = new ErrorCallbackFuture((t: Throwable) => {
      requestFutures.remove(requestId)
      callback(t)
    })

    conn.send(Some(connFtch)) { serializer: Serializer =>
			val baos = new ExposingByteArrayOutputStream(BufSize)
      baos.writeZeros(4)
      serializer.writeLocateRequest(baos, requestId, receiverName) 
			new DiscardableByteSequence(baos.getUnderlyingByteArray, 4, baos.size - 4)
    }

    //Debug.info("blocking on ftch: " + ftch)
    ftch.map(_.await(config.waitTimeout))
  }

  private val processMsgFunc = processMsg _

  private val msgConnCallback = (listener: Listener, msgConn: MessageConnection) => {}

  private final val NoSender = new Proxy {
    override def remoteNode = 
      throw new RuntimeException("NoSender")
    override def name = 
      throw new RuntimeException("NoSender")
    override def conn(a: Option[AbstractActor]) = 
      throw new RuntimeException("NoSender")
    override def config = 
      throw new RuntimeException("NoSender")
    override def numRetries =
      throw new RuntimeException("NoSender")

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
              Debug.error(this + ": lost session: " + session)
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

  // TODO: have N threads process these requests
  private object locateRequestProc extends Thread("LocateRequestProcThread") {
    val queue = new LinkedBlockingQueue[(MessageConnection, LocateRequest)]
    setDaemon(true)
    start()
    override def run() {
      Debug.info(this + ": started")
      while (true) {
        try {
          val (conn, r @ LocateRequest(sessionId, receiverName)) = queue.take()
          Debug.error(this + ": processing locate request" + r)
          val found = RemoteActor.getActor(Symbol(receiverName)) ne null
          conn.send(None) { serializer: Serializer => 
            val baos = new ExposingByteArrayOutputStream(BufSize)
            baos.writeZeros(4)
            serializer.writeLocateResponse(baos, sessionId, receiverName, found) 
            new DiscardableByteSequence(baos.getUnderlyingByteArray, 4, baos.size - 4)
          }
        } catch {
          case e: Exception =>
            Debug.error(this + ": caught exception: " + e.getMessage)
            Debug.doError { e.printStackTrace() }
        }
      }
    }
  }

  private def processMsg(conn: MessageConnection, serializer: Serializer, msg: AnyRef) {
    //Debug.info("processMsg: " + msg)
    msg match {
      case r @ LocateRequest(_, _) =>
        Debug.error(this + ": locate request: " + r)
        // offload to work queue so we don't block in receiving thread
        locateRequestProc.queue.offer((conn, r))
      case LocateResponse(sessionId, receiverName, found) =>
        Debug.error(this + ": locate response: " + msg)
        val future = requestFutures.remove(sessionId)
        if (future ne null)
          if (found) 
            future.finishSuccessfully()
          else 
            future.finishWithError(new NoSuchRemoteActorException(Symbol(receiverName)))
        else
          Debug.error("LocateResponse received with no associated request: " + msg)
      case _ =>
        @inline def mkProxy(senderName: String) = 
          new ConnectionProxy(conn.remoteNode, Symbol(senderName), conn)
        @inline def removeOrphan(a: OutputChannel[Any]) {
          // orphaned actor sitting in hash maps
          Debug.error(this + ": found orphaned (terminated) actor: " + a)
          RemoteActor.unregister(a)
        }
        // TODO: extractors are kind of expensive. move this logic into some
        // base trait here, and call a method instead
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
          Debug.error(this+": lost message")
        else if (a.isInstanceOf[Actor] && 
                a.asInstanceOf[Actor].getState == Actor.State.Terminated)
          removeOrphan(a)
        else {
          // TODO: see above, extractors are expensive
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
