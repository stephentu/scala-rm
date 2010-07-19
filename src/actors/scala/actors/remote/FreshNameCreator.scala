/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import java.lang.{Integer => JInteger}

/**
 * Methods here are now threadsafe
 */
object FreshNameCreator {

  private val counter = new AtomicLong 
  private val counters = new ConcurrentHashMap[String, JInteger]

  /**
   * Create a fresh name with the given prefix. It is guaranteed
   * that the returned name has never been returned by a previous
   * call to this function (provided the prefix does not end in a digit).
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

  private def mkSym(prefix: String, id: Int) = Symbol(prefix + "$" + id)

  def newName(): Symbol = Symbol("$" + counter.getAndIncrement() + "$")
}
