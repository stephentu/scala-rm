/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

import java.io.{ByteArrayInputStream, ByteArrayOutputStream,
                ObjectInputStream, ObjectOutputStream, InputStream,
                ObjectStreamClass}

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

/**
 *  @author Philipp Haller
 */
class JavaSerializer(cl: ClassLoader) extends Serializer {

  case object SendID
  case object ExpectID
  case object Resolved

  val initialState = Some(SendID)

  def nextHandshakeMessage = {
    case SendID   => (ExpectID, Some(uniqueId))
    case Resolved => (Resolved, None)
  }

  def handleHandshakeMessage = {
    case (ExpectID, MyUniqueId) => Resolved
  }

  val MyUniqueId = uniqueId

  def uniqueId = 1679081588L

  def serializeMetaData(message: AnyRef): Option[Array[Byte]] = None

  def serialize(o: AnyRef): Array[Byte] = javaSerialize(o)

  def deserialize(metaData: Option[Array[Byte]], bytes: Array[Byte]): AnyRef =
    if (cl eq null) javaDeserialize(metaData, bytes)
    else {
      // use custom class loader in this case
      val bis = new ByteArrayInputStream(bytes)
      val in  = new CustomObjectInputStream(bis, cl)
      in.readObject()
    }

}
