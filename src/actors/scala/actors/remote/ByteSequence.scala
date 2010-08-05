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
 * Explicit container for a (sub)-sequence of bytes
 */
private[remote] case class ByteSequence(val bytes: Array[Byte], 
																			  val offset: Int, 
																				val length: Int) {
	def this(bytes: Array[Byte]) = this(bytes, 0, bytes.length)
	assert(bytes ne null)
	assert(offset >= 0 && offset < bytes.length)
	assert(length >= 0 && length <= bytes.length - offset)
}
