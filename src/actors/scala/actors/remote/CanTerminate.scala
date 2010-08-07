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
 * Can only be terminated once
 */
private[remote] trait CanTerminate {

  final def terminateTop()    { doTerminate(false) }
  final def terminateBottom() { doTerminate(true)  }

  @volatile private[this] var preTerminate: Option[Boolean => Unit] = None
  final def beforeTerminate(f: Boolean => Unit) {
    withoutTermination {
      if (!terminateCompleted) {
        if (preTerminate.isDefined)
          Debug.warning(this + ": beforeTerminate() - terminate sequence already registered - replacing")
        preTerminate = Some(f)
      } else Debug.info(this + ": beforeTerminate() - already terminated")
    }
  }

  @volatile private[this] var postTerminate: Option[Boolean => Unit] = None
  final def afterTerminate(f: Boolean => Unit) {
    withoutTermination {
      if (!terminateCompleted) {
        if (postTerminate.isDefined)
          Debug.warning(this + ": afterTerminate() - terminate sequence already registered - replacing")
        postTerminate = Some(f)
      } else Debug.info(this + ": afterTerminate() - already terminated")
    }
  }

  protected final def withoutTermination[T](f: => T) = terminateLock.synchronized { f }

  final def doTerminate(isBottom: Boolean) {
    if (!terminateInitiated) { // check first w/o acquiring lock
      terminateLock.synchronized {
        if (!terminateInitiated) { // now check again
          terminateInitiated = true
          preTerminate foreach { f => f(isBottom) }
          doTerminateImpl(isBottom)
          postTerminate foreach { f => f(isBottom) }
          terminateCompleted = true
        }
      }
    }
  }

  protected def ensureAlive() {
    if (terminateInitiated) throw newAlreadyTerminatedException()
  }

  protected def newAlreadyTerminatedException(): AlreadyTerminatedException = new AlreadyTerminatedException

  /**
   * Guaranteed to only execute once. TerminateLock is acquired when
   * doTerminateImpl() executes
   */
  protected def doTerminateImpl(isBottom: Boolean): Unit

  protected final val terminateLock = new Object

  @volatile protected var terminateInitiated = false
  @volatile protected var terminateCompleted = false

  /** Returns true if a termination has been initiated (but before any pre
   * handlers have run)  */
  final def hasTerminateStarted  = terminateInitiated
  /** Returns true if a termination has finished (after any post handlers have
   * run) */
  final def hasTerminateFinished = terminateCompleted
}

class AlreadyTerminatedException(s: String) extends RuntimeException(s) {
  def this() = this("Already terminated")
}

