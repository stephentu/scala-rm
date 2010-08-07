/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

private[remote] object BitUtils {
  private def newShortWord() = new Array[Byte](2)
  private def newWord() = new Array[Byte](4)
  private def newLongWord() = new Array[Byte](8)
  def shortToBytes(s: Short): Array[Byte] = {
    val bytes = newShortWord()
    bytes(0) = ((s >>> 8) & 0xff).toByte
    bytes(1) = (s & 0xff).toByte
    bytes
  }
  def bytesToShort(bytes: Array[Byte]): Short = {
    ((bytes(0).toInt & 0xFF) << 8 |
    (bytes(1).toInt & 0xFF)).toShort
  }
  def intToBytes(i: Int): Array[Byte] = {
    val bytes = newWord()
    bytes(0) = ((i >>> 24) & 0xff).toByte
    bytes(1) = ((i >>> 16) & 0xff).toByte
    bytes(2) = ((i >>> 8) & 0xff).toByte
    bytes(3) = (i & 0xff).toByte
    bytes
  }
  def bytesToInt(bytes: Array[Byte]): Int = {
    (bytes(0).toInt & 0xFF) << 24 | 
    (bytes(1).toInt & 0xFF) << 16 | 
    (bytes(2).toInt & 0xFF) << 8  |
    (bytes(3).toInt & 0xFF)
  }
  def longToBytes(l: Long): Array[Byte] = {
    val bytes = newLongWord()
    bytes(0) = ((l >>> 56) & 0xff).toByte
    bytes(1) = ((l >>> 48) & 0xff).toByte
    bytes(2) = ((l >>> 40) & 0xff).toByte
    bytes(3) = ((l >>> 32) & 0xff).toByte
    bytes(4) = ((l >>> 24) & 0xff).toByte
    bytes(5) = ((l >>> 16) & 0xff).toByte
    bytes(6) = ((l >>> 8) & 0xff).toByte
    bytes(7) = (l & 0xff).toByte
    bytes
  }
  def bytesToLong(bytes: Array[Byte]): Long = {
    (bytes(0).toLong & 0xFF) << 56 | 
    (bytes(1).toLong & 0xFF) << 48 | 
    (bytes(2).toLong & 0xFF) << 40 |
    (bytes(3).toLong & 0xFF) << 32 |
    (bytes(4).toLong & 0xFF) << 24 | 
    (bytes(5).toLong & 0xFF) << 16 | 
    (bytes(6).toLong & 0xFF) << 8  |
    (bytes(7).toLong & 0xFF)
  }
  def floatToBytes(f: Float): Array[Byte] = {
    val intRepr = java.lang.Float.floatToRawIntBits(f)
    intToBytes(intRepr)
  }
  def bytesToFloat(bytes: Array[Byte]): Float = {
    val intRepr = bytesToInt(bytes)
    java.lang.Float.intBitsToFloat(intRepr)
  }
  def doubleToBytes(d: Double): Array[Byte] = {
    val longRepr = java.lang.Double.doubleToRawLongBits(d)
    longToBytes(longRepr)
  }
  def bytesToDouble(bytes: Array[Byte]): Double = {
    val longRepr = bytesToLong(bytes)
    java.lang.Double.longBitsToDouble(longRepr)
  }
  def charToBytes(c: Char): Array[Byte] = shortToBytes(c.toShort)
  def bytesToChar(bytes: Array[Byte]): Char = bytesToShort(bytes).toChar
}
