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

private[remote] object PrimitiveSerializer {
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

private[remote] class NonPrimitiveClassException(clz: Class[_]) extends Exception

private[remote] class PrimitiveSerializer {

	import PrimitiveSerializer._

	// TODO: varint encoding
	def serialize(message: Any, outputStream: OutputStream) {
		val dos = new DataOutputStream(outputStream)
		message match {
			case b: Byte        => dos.writeByte(BYTE_TAG); dos.writeByte(b) 
			case s: Short       => dos.writeByte(SHORT_TAG); dos.writeShort(s) 
			case i: Int         => dos.writeByte(INT_TAG); dos.writeInt(i) 
			case l: Long        => dos.writeByte(LONG_TAG); dos.writeLong(l) 
			case f: Float       => dos.writeByte(FLOAT_TAG); dos.writeFloat(f) 
			case d: Double      => dos.writeByte(DOUBLE_TAG); dos.writeDouble(d)
			case b: Boolean     => dos.writeByte(BOOLEAN_TAG); dos.writeBoolean(b)
			case c: Char        => dos.writeByte(CHAR_TAG); dos.writeChar(c) 
			case s: String      => dos.writeByte(STRING_TAG); dos.writeBytes(s) 
			case b: Array[Byte] => dos.writeByte(BYTES_TAG); dos.write(b)
		}
	}

	// TODO: varint decoding
	def deserialize(bytes: Array[Byte]) = {
		val is  = new ByteArrayInputStream(bytes)
		val dis = new DataInputStream(is)
		dis.readByte() match {
			case BYTE_TAG    => dis.readByte()
			case SHORT_TAG   => dis.readShort()
			case INT_TAG     => dis.readInt() 
			case LONG_TAG    => dis.readLong()
			case FLOAT_TAG   => dis.readFloat()
			case DOUBLE_TAG  => dis.readDouble() 
			case BOOLEAN_TAG => dis.readBoolean()
			case CHAR_TAG    => dis.readChar()
			case STRING_TAG  => 
				val buf = new Array[Byte](bytes.length - 1)
				dis.readFully(buf)
				new String(buf)
			case BYTES_TAG   => 
				val buf = new Array[Byte](bytes.length - 1)
				dis.readFully(buf)
				buf
			case t           => throw new IllegalStateException("Bad tag found: " + t)
		}
	}

  /**
   * Returns the TOTAL (including tag) number of bytes needed to serializer
   * message
   */
	def sizeOf(message: Any) = {
    val s0 = message match {
      case b: Byte        => 1
      case s: Short       => 2
      case i: Int         => 4
      case l: Long        => 8
      case f: Float       => 4
      case d: Double      => 8
      case b: Boolean     => 1
      case c: Char        => 2
      case s: String      => s.length
      case b: Array[Byte] => b.length
      case a: AnyRef      => throw new NonPrimitiveClassException(a.getClass)
    }
    1 + s0
  }

}
