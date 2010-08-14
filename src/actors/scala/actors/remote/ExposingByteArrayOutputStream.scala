/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

import java.io.ByteArrayOutputStream
import java.util.{Arrays => JArrays}
import java.lang.{Math => JMath}

/**
 * This class gives us access to the underlying buffer of
 * ByteArrayOutputStream, so that we can avoid a copy 
 */
private[remote] class ExposingByteArrayOutputStream(i: Int) extends ByteArrayOutputStream(i) {
  def this() = this(32)
  def getUnderlyingByteArray = this.buf

  ///**
  // * Does not check the newcount argument for sanity
  // */
  //@inline private def ensureLength(newcount: Int) {
  //  if (newcount > buf.length)
  //    buf = JArrays.copyOf(buf, JMath.max(buf.length << 1, newcount))
  //}

  // TODO: consider if writing non-synchronized versions of write() would
  // improve performance (since we don't use these
  // ExposingByteArrayOutputStream concurrently

  //override def write(b: Int) {
  //  val newcount = count + 1
  //  ensureLength(newcount)
  //  buf(count) = b.toByte
  //  count = newcount
  //}

  override def toString = "<ExposingByteArrayOutputStream size = %d".format(size)

  /**
   * Not thread-safe
   */
  def writeZeros(numBytes: Int) {
    if (numBytes < 0)
      throw new IllegalArgumentException("numBytes must be non negative")
    else if (numBytes > 0) {
      //val newcount = count + numBytes
      //ensureLength(newcount)
      //count = newcount
      var i = numBytes
      while (i > 0) {
        write(0)
        i -= 1
      }
    }
  }
}

/**
 * Represents an ByteArrayOutputStream with a pre-allocated field for a
 * header field to go.
 */
private[remote] class PreallocatedHeaderByteArrayOutputStream(prealloc: Int, bufsize: Int) 
  extends ByteArrayOutputStream(bufsize) {

  assert(prealloc >= 0 && bufsize >= prealloc)

  // preallocate the bytes
  count = prealloc

  def toDiscardableByteSeq =
  	new DiscardableByteSequence(buf, prealloc, count - prealloc)
}
