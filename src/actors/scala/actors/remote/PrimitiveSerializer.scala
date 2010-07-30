/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

object PrimitiveSerializer {
  final val BYTE_TAG    = 1
  final val SHORT_TAG   = 2
  final val INT_TAG     = 3
  final val LONG_TAG    = 4
  final val FLOAT_TAG   = 5
  final val DOUBLE_TAG  = 6
  final val BOOLEAN_TAG = 7
  final val CHAR_TAG    = 8
  final val STRING_TAG  = 9 // string is an exception
  final val BYTES_TAG   = 10 // so is byte array
}

class NonPrimitiveClassException(clz: Class[_]) extends Exception

class PrimitiveSerializer 
  extends Serializer
  with    DefaultMessageCreator
  with    NonHandshakingSerializer {

  import PrimitiveSerializer._

  override def uniqueId = 282367820L  

  override def serializeMetaData(message: AnyRef) = { 
    if (message eq null) throw new NullPointerException
    val tag = (message: Any) match {
      case b: Byte        => BYTE_TAG
      case s: Short       => SHORT_TAG
      case i: Int         => INT_TAG
      case l: Long        => LONG_TAG
      case f: Float       => FLOAT_TAG
      case d: Double      => DOUBLE_TAG
      case b: Boolean     => BOOLEAN_TAG
      case c: Char        => CHAR_TAG
      case s: String      => STRING_TAG
      case b: Array[Byte] => BYTES_TAG
      case _              => throw new NonPrimitiveClassException(message.getClass)
    }
    val meta = new Array[Byte](1)
    meta(0) = tag.toByte
    Some(meta)
  }

  import BitUtils._

  override def serialize(message: AnyRef) = {
    if (message eq null) throw new NullPointerException
    (message: Any) match {
      case b: Byte    => 
        val bytes = new Array[Byte](1)
        bytes(0) = b
        bytes
      case s: Short       => shortToBytes(s)
      case i: Int         => intToBytes(i)
      case l: Long        => longToBytes(l)
      case f: Float       => floatToBytes(f)
      case d: Double      => doubleToBytes(d)
      case b: Boolean     =>
        val bytes = new Array[Byte](1)
        bytes(0) = if (b) 1 else 0 
        bytes
      case c: Char        => charToBytes(c)
      case s: String      => s.getBytes
      case b: Array[Byte] => b
      case _              => throw new NonPrimitiveClassException(message.getClass)
    }
  }

  override def deserialize(metaData: Option[Array[Byte]], data: Array[Byte]) = metaData match {
    case Some(metaBytes) =>
      val retVal = metaBytes(0).toInt match {
        case BYTE_TAG    => data(0)
        case SHORT_TAG   => bytesToShort(data)
        case INT_TAG     => bytesToInt(data)
        case LONG_TAG    => bytesToLong(data)
        case FLOAT_TAG   => bytesToFloat(data)
        case DOUBLE_TAG  => bytesToDouble(data)
        case BOOLEAN_TAG => if (data(0) == 0) false else true
        case CHAR_TAG    => bytesToChar(data)
        case STRING_TAG  => new String(data)
        case BYTES_TAG   => data
        case t           => throw new IllegalStateException("Bad tag found: " + t)
      }
      retVal.asInstanceOf[AnyRef]
    case None =>
      throw new IllegalStateException("Need metadata")
  }

}
