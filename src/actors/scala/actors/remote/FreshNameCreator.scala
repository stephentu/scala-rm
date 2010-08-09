/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

import scala.util.Random

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import java.lang.{Integer => JInteger}

/**
 * This utility offers methods for creating unique names. All methods here are
 * thread-safe.
 */
object FreshNameCreator {

  private final val counter  = new AtomicLong 
  private final val counters = new ConcurrentHashMap[String, JInteger]

  /**
   * Create a fresh name with the given prefix. It is guaranteed
   * that the returned name has never been returned by a previous
   * call to this function (unless the prefix starts with `ANON$`, which is
   * currently used by <code>newName</code>.
   * 
   * @param  prefix  Prefix to use when generating new names
   * 
   * @return A unique name containing <code>prefix</code> as the prefix
   */
  def newName(prefix: String): Symbol = {
    val test = counters.putIfAbsent(prefix, JInteger.valueOf(0))
    if (test eq null)
      mkSym(prefix, 0) // first ones
    else {
      var startId = counters.get(prefix)
      var testId = JInteger.valueOf(startId.intValue + 1)
      var continue = true
      while (continue) {
        if (counters.replace(prefix, startId, testId))
          continue = false 
        else {
          startId = JInteger.valueOf(startId.intValue + 1)
          testId  = JInteger.valueOf(testId.intValue + 1)
        }
      }
      mkSym(prefix, testId.intValue)
    }
  }

  @inline private def mkSym(prefix: String, id: Int) = 
    Symbol(prefix + "$" + id)

  /**
   * Returns a new name unique to this JVM instance, which is currently the
   * concatenation of `ANON$` with a incrementing counter 
   *
   * @return  Unique identifier
   */
  def newName(): Symbol = Symbol("ANON$" + counter.getAndIncrement())

  private final val random = new Random

  /**
   * Returns a new session ID that is unique to this host. Currently returns
   * a concatenation of [`$`, canonical localhost name, `$`, random 64-bit
   * number]
   */
  private[remote] def newSessionId(): Symbol =
    Symbol("$" + Node.localhost + "$" + random.nextLong)
}
