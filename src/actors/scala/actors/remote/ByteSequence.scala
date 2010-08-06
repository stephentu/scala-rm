/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote


/**
 * Explicit container for a (sub)-sequence of non-discardable bytes.
 * Non-discardable means that the bytes outside of the sequence, within the
 * array, are NOT to be modified
 */
class ByteSequence(val bytes: Array[Byte], 
                   val offset: Int, 
                   val length: Int) {
  def this(bytes: Array[Byte]) = this(bytes, 0, bytes.length)

  assert(bytes ne null)
  assert(offset >= 0 && offset < bytes.length)
  assert(length >= 0 && length <= bytes.length - offset)

  def isDiscardable = false
  
}

/**
 * A ByteSequence where the bytes outside of the sequence are un-important-
 * they can be modified for whatever optimizations by the user.
 */
class DiscardableByteSequence(bytes: Array[Byte], 
                              offset: Int, 
                              length: Int) extends ByteSequence(bytes, offset, length) {
  def this(bytes: Array[Byte]) = this(bytes, 0, bytes.length)
  override def isDiscardable = true
}

