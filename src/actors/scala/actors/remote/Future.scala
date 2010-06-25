/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

private[scala] trait Finishable {
  def finish(): Unit
  def finishWithError(ex: Throwable): Unit
}

/**
 * One time use
 */
private[scala] class Future extends Finishable {
  var alreadyDone          = false
  var exception: Throwable = _
  def await() {
    synchronized {
      while (!isFinished)       wait // TODO: thread interrupted ex
      if    (exception ne null) throw exception 
    }
  }
  private def finish0(ex: Throwable) {
    synchronized {
      alreadyDone = true
      exception   = ex
      notifyAll
    }
  }
  def isFinished =                   { alreadyDone   }
  def finish()                       { finish0(null) }
  def finishWithError(ex: Throwable) { finish0(ex)   }
}

private[scala] class CountFuture(count: Int) extends Future {
  if (count < 0)
    throw new IllegalArgumentException("Count must be non-negative")
  var curVal  = 0
  alreadyDone = curVal == count
  def addOne {
    synchronized {
      if (!alreadyDone) {
        curVal     += 1
        alreadyDone = curVal == count
        if (alreadyDone) finish()
      } else 
        throw new IllegalArgumentException("Already done")
    }
  }
}
