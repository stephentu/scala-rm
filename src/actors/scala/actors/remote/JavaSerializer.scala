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
  final val ASYNC_SEND   = 0x0
  final val SYNC_SEND    = 0x1
  final val SYNC_REPLY   = 0x2
  final val REMOTE_APPLY = 0x3
}

/**
 * The default Java serializer.
 *
 * @author Philipp Haller
 */
class JavaSerializer(cl: ClassLoader) 
  extends Serializer
  with    DefaultMessageCreator
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


  override def read(bytes: Array[Byte]) = {
    val bais = new ByteArrayInputStream(bytes)
    val is = new DataInputStream(bais)
    val tag = readTag(is) 
    tag match {
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

  private def writeTag(os: DataOutputStream, t: Int) {
    os.writeByte(t)
  }

  private def readTag(is: DataInputStream): Int = 
    is.readByte()

  // TODO: varint encoding
  private def writeString(os: DataOutputStream, s: String) {
    if (s eq null)
      os.writeInt(0)
    else {
      os.writeInt(s.length)
      os.writeBytes(s)
    }
  }

  // TODO: varint decoding
  private def readString(is: DataInputStream): String = {
    val len = is.readInt()
    if (len == 0) null
    else {
      val bytes = new Array[Byte](len)
      is.readFully(bytes, 0, len)
      new String(bytes)
    }
  }

  private def writeObject(os: DataOutputStream, o: AnyRef) {
    val out = new ObjectOutputStream(os)
    out.writeObject(o)
    out.flush()
  }

  private def readObject(is: DataInputStream): AnyRef = {
    val in  = newObjectInputStream(is)
    in.readObject()
  }
}
