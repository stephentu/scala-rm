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
import java.util.concurrent.{ ConcurrentHashMap, CountDownLatch, TimeUnit }


case object Terminate

/**
 * @version 1.0.0
 * @author Philipp Haller
 * @author Stephen Tu
 */
private[remote] object NetKernel {

  @volatile private var _service = new StandardService

  private val connections = new ConcurrentHashMap[(Node, Long, ServiceMode.Value), MessageConnection]

  def getConnectionFor(node: Node, cfg: Configuration) = {
    val key = (node, cfg.cachedSerializer.uniqueId, cfg.selectMode)
    val testConn = connections.get(key)
    if (testConn ne null)
      testConn
    else {
      connections.synchronized {
        val testConn0 = connections.get(key)
        if (testConn0 ne null)
          testConn0
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
    }
  }

  private val listeners = new ConcurrentHashMap[Int, Listener]

  def getListenerFor(port: Int, cfg: Configuration) = {
    def ensureListener(listener: Listener) = {
      if (listener.mode != cfg.aliveMode)
        throw InconsistentServiceException(cfg.aliveMode, listener.mode)
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

  def asyncSend(conn: MessageConnection, toName: String, fromName: Option[String], msg: AnyRef) {
    conn.send { serializer: Serializer =>
			val baos = new ByteArrayOutputStream(1024)
      val wireMsg  = serializer.intercept(msg)
      serializer.writeAsyncSend(baos, fromName.orNull, toName, wireMsg)
			baos.toByteArray
    }
  }

  def syncSend(conn: MessageConnection, toName: String, fromName: String, msg: AnyRef, session: String) {
    conn.send { serializer: Serializer =>
			val baos = new ByteArrayOutputStream(1024)
      val wireMsg  = serializer.intercept(msg)
      serializer.writeSyncSend(baos, fromName, toName, wireMsg, session)
			baos.toByteArray
    }
  }

  def syncReply(conn: MessageConnection, toName: String, msg: AnyRef, session: String) {
    conn.send { serializer: Serializer =>
			val baos = new ByteArrayOutputStream(1024)
      val wireMsg  = serializer.intercept(msg)
      serializer.writeSyncReply(baos, toName, wireMsg, session)
			baos.toByteArray
    }
  }

  def remoteApply(conn: MessageConnection, toName: String, fromName: String, rfun: RemoteFunction) {
    conn.send { serializer: Serializer =>
			val baos = new ByteArrayOutputStream(1024)
      serializer.writeRemoteApply(baos, fromName, toName, rfun) 
			baos.toByteArray
    }
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
