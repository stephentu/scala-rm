/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

import java.io._

/**
 *  @author Guy Oliver
 */
private[remote] class CustomObjectInputStream(in: InputStream, cl: ClassLoader)
extends ObjectInputStream(in) {
  override def resolveClass(cd: ObjectStreamClass): Class[_] =
    try {
      cl.loadClass(cd.getName())
    } catch {
      case cnf: ClassNotFoundException =>
        super.resolveClass(cd)
    }
  override def resolveProxyClass(interfaces: Array[String]): Class[_] =
    try {
      val ifaces = interfaces map { iface => cl.loadClass(iface) }
      java.lang.reflect.Proxy.getProxyClass(cl, ifaces: _*)
    } catch {
      case e: ClassNotFoundException =>
        super.resolveProxyClass(interfaces)
    }
}

private[remote] object JavaSerializer {
  final val LOCATE_REQ   = 0x0
  final val LOCATE_RESP  = 0x1
  final val ASYNC_SEND   = 0x2
  final val SYNC_SEND    = 0x3
  final val SYNC_REPLY   = 0x4
  final val REMOTE_APPLY = 0x5
}

/**
 * The default Java serializer.
 *
 * @author Philipp Haller
 */
class JavaSerializer(cl: ClassLoader) 
  extends Serializer
  with    IdResolvingSerializer {

  import JavaSerializer._

  /**
   * No argument constructor, using a regular class loader
   */
  def this() = this(null)

  override val uniqueId = 1679081588L

  private def newObjectInputStream(is: InputStream) = 
    if (cl eq null) new ObjectInputStream(is)
    else new CustomObjectInputStream(is, cl)

  override def writeLocateRequest(outputStream: OutputStream, sessionId: Long, receiverName: String) {
    val os = new DataOutputStream(outputStream)
    writeTag(os, LOCATE_REQ)
    writeLong(os, sessionId)
    writeString(os, receiverName)
  }

  override def writeLocateResponse(outputStream: OutputStream, sessionId: Long, receiverName: String, found: Boolean) {
    val os = new DataOutputStream(outputStream)
    writeTag(os, LOCATE_RESP)
    writeLong(os, sessionId)
    writeString(os, receiverName)
    writeBoolean(os, found)
  }

  // For performance reasons, all the write[...] methods do not use the
  // control messages (ie DefaultAsyncSendImpl). Instead, we just write the
  // data directly into the outputStream, and decode it on the other end.
  // Var-int encoding however has not been implemented, and should be done for
  // even better performance

  override def writeAsyncSend(outputStream: OutputStream, senderName: String, receiverName: String, message: AnyRef) {
    val os = new DataOutputStream(outputStream)
    writeTag(os, ASYNC_SEND)
    writeString(os, senderName)
    writeString(os, receiverName)
    writeObject(os, message)
  }

  override def writeSyncSend(outputStream: OutputStream, senderName: String, receiverName: String, message: AnyRef, session: String) {
    val os = new DataOutputStream(outputStream)
    writeTag(os, SYNC_SEND)
    writeString(os, senderName)
    writeString(os, receiverName)
    writeString(os, session)
    writeObject(os, message)
  }

  override def writeSyncReply(outputStream: OutputStream, receiverName: String, message: AnyRef, session: String) {
    val os = new DataOutputStream(outputStream)
    writeTag(os, SYNC_REPLY)
    writeString(os, receiverName)
    writeString(os, session)
    writeObject(os, message)
  }

  override def writeRemoteApply(outputStream: OutputStream, senderName: String, receiverName: String, rfun: RemoteFunction) {
    val os = new DataOutputStream(outputStream)
    writeTag(os, REMOTE_APPLY)
    writeString(os, senderName)
    writeString(os, receiverName)
    writeObject(os, rfun)
  }


  override def read(inputStream: InputStream) = {
    val is = new DataInputStream(inputStream)
    val tag = readTag(is) 
    tag match {
      case LOCATE_REQ =>
        val sessionId = readLong(is)
        val receiverName = readString(is)
        LocateRequest(sessionId, receiverName)
      case LOCATE_RESP =>
        val sessionId = readLong(is)
        val receiverName = readString(is)
        val found = readBoolean(is)
        LocateResponse(sessionId, receiverName, found)
      case ASYNC_SEND =>
        val senderName = readString(is)
        val receiverName = readString(is)
        val message = readObject(is)
        AsyncSend(senderName, receiverName, message)
      case SYNC_SEND =>
        val senderName = readString(is)
        val receiverName = readString(is)
        val session = readString(is)
        val message = readObject(is)
        SyncSend(senderName, receiverName, message, session)
      case SYNC_REPLY =>
        val receiverName = readString(is)
        val session = readString(is)
        val message = readObject(is)
        SyncReply(receiverName, message, session)
      case REMOTE_APPLY =>
        val senderName = readString(is)
        val receiverName = readString(is)
        // TODO: Don't bother serializing the function, just send a function
        // tag
        val function = readObject(is).asInstanceOf[RemoteFunction]
        RemoteApply(senderName, receiverName, function)
    }
  }

  protected def writeBoolean(os: DataOutputStream, b: Boolean) {
    os.writeByte(if (b) 1 else 0)
  }

  protected def readBoolean(is: DataInputStream): Boolean = 
    if (is.readByte() == 0) false else true

  protected def writeTag(os: DataOutputStream, t: Int) {
    os.writeByte(t)
  }

  protected def readTag(is: DataInputStream): Int = 
    is.readByte()

  // TODO: varint encoding
  protected def writeString(os: DataOutputStream, s: String) {
    if (s eq null)
      os.writeInt(0)
    else {
      os.writeInt(s.length)
      os.writeBytes(s)
    }
  }

  // TODO: varint decoding
  protected def readString(is: DataInputStream): String = {
    val len = is.readInt()
    if (len == 0) null
    else {
      val bytes = new Array[Byte](len)
      is.readFully(bytes, 0, len)
      new String(bytes)
    }
  }

  // TODO: varint encoding
  protected def writeLong(os: DataOutputStream, l: Long) {
    os.writeLong(l)
  }

  // TODO: varint decoding
  protected def readLong(is: DataInputStream): Long =
    is.readLong()

  protected def writeObject(os: DataOutputStream, o: AnyRef) {
    val out = new ObjectOutputStream(os)
    out.writeObject(o)
    out.flush()
  }

  protected def readObject(is: DataInputStream): AnyRef = {
    val in  = newObjectInputStream(is)
    in.readObject()
  }
}
