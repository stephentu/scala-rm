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
trait CanTerminate {

  final def terminateTop()    { doTerminate(false) }
  final def terminateBottom() { doTerminate(true)  }

  @volatile protected var preTerminate: Option[() => Unit] = None
  final def beforeTerminate(f: => Unit) {
    withoutTermination {
      if (!terminateCompleted) {
        if (preTerminate.isDefined)
          Debug.warning(this + ": beforeTerminate() - terminate sequence already registered - replacing")
        preTerminate = Some(() => f)
      } else Debug.info(this + ": beforeTerminate() - already terminated")
    }
  }

  @volatile protected var postTerminate: Option[() => Unit] = None
  final def afterTerminate(f: => Unit) {
    withoutTermination {
      if (!terminateCompleted) {
        if (postTerminate.isDefined)
          Debug.warning(this + ": afterTerminate() - terminate sequence already registered - replacing")
        postTerminate = Some(() => f)
      } else Debug.info(this + ": afterTerminate() - already terminated")
    }
  }

  protected final def withoutTermination[T](f: => T) = terminateLock.synchronized { f }

  protected final def doTerminate(isBottom: Boolean) {
    terminateLock.synchronized {
      if (!terminateCompleted) {
        terminateInitiated = true
        preTerminate foreach { f => f() }
        doTerminateImpl(isBottom)
        postTerminate foreach { f => f() }
        terminateCompleted = true
      }
    }
  }

  protected def ensureAlive() {
    if (terminateInitiated) throw newAlreadyTerminatedException()
  }

  protected def newAlreadyTerminatedException(): Exception = new RuntimeException("Already terminated")

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
