/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

import scala.collection.mutable.ListBuffer

import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.ManagedBlocker

/**
 * A <code>FutureTimeoutException</code> is thrown whenever blocking on a
 * future results in a timeout (no result within the specified time). 
 *
 * @see ConnectPolicy
 */
class FutureTimeoutException extends Exception("Future timed out")


private[remote] trait RFuture {

  @throws(classOf[FutureTimeoutException])
  def await(timeout: Long): Unit

  def awaitWithOption(timeout: Long): Option[Throwable]

  def finishSuccessfully(): Unit

  def finishWithError(t: Throwable): Unit 

  def isFinished: Boolean

  /**
   * If the future has already failed when this method is called,
   * <code>e</code> will execute right away
   */
  def notifyOnError(e: Throwable => Unit): Unit

  /**
   * If the future has already succeeded when this method is called,
   * <code>e</code> will execute right away
   */
  def notifyOnSuccess(e: => Unit): Unit

  def blocker(timeout: Long): ManagedBlocker
}

/**
 * A simple blocking future. A <code>BlockingFuture</code> can only be 
 * finished once (either successfully or errorneously). Multiple calls to 
 * finish a future will result in an exception being thrown the second time. 
 *
 * This restriction is necessary to allow <code>await</code> and 
 * <code>awaitWithOption</code> to execute very quickly when the future 
 * is already finished
 */
private[remote] class BlockingFuture extends RFuture {

  // NOTE: Blocking methods are guarded by this

  private val successCallbacks = new ListBuffer[() => Unit]
  private val errorCallbacks   = new ListBuffer[Throwable => Unit]

  /**
   * Guard controls the state of this future. If guard is null, the future has
   * not been completed yet. If guard is None, then the future was successful.
   * Otherwise if guard is Some, then the future was errorneous. Letting an
   * Option variable take on null, while bad practice, allows us to have very
   * clean semantics for the state of this future, w/o creating another class
   * to represent the state
   */
  @volatile private var guard: Option[Throwable] = null 

  override def notifyOnError(e: Throwable => Unit) {
    val res = guard
    if (res ne null)
      res.foreach(ex => e(ex))
    else 
      synchronized {
        val res0 = guard
        if (res0 eq null)
          errorCallbacks += e
        else
          res.foreach(ex => e(ex))
      }
  }

  override def notifyOnSuccess(e: => Unit) {
    val res = guard
    if (res ne null) {
      if (res.isEmpty) e
    } else
      synchronized {
        val res0 = guard
        if (res0 eq null)
          successCallbacks += (() => e)
        else {
          if (res.isEmpty) e
        }
      }
  }

  override def await(timeout: Long) {
    val res = guard
    if (res ne null)
      // fast path - we're done
      res.foreach(e => throw e)
    else
      // slow path, we have to block on the future
      synchronized {
        if (guard eq null) // check again
          wait(timeout)
        val res0 = guard
        if (res0 ne null)
          res0.foreach(e => throw e)
        else
          throw new FutureTimeoutException
      }
  }

  override def awaitWithOption(timeout: Long) =
    try { 
      await(timeout) 
      None 
    } catch {
      case e: Throwable => Option(e)
    }

  override def finishSuccessfully() {
    synchronized {
      if (guard ne null)
        throw new IllegalStateException("Already set")
      else {
        guard = None
        notifyAll()
      }
    }
    // callbacks are NOT executed when holding the lock, to be safe
    successCallbacks foreach { e => e() }
    successCallbacks.clear()
  }

  override def finishWithError(t: Throwable) {
    require(t ne null)
    synchronized {
      if (guard ne null)
        throw new IllegalStateException("Already set")
      else {
        guard = Some(t)
        notifyAll()
      }
    }
    // callbacks are NOT executed when holding the lock, to be safe
    errorCallbacks foreach { e => e(t) }
    errorCallbacks.clear()
  }

  override def isFinished = 
    guard ne null

  /**
   * The returned `ManagedBlocker` is assumed to be only invoked by a single
   * thread (it is NOT threadsafe)
   */
  override def blocker(timeout: Long) = new ManagedBlocker {
    // we used completed to ensure that the block() method is invoked exactly
    // once (so that we can cause the necessary exceptions to be thrown)
    private var completed = false
    override def block() = {
      await(timeout)
      completed = true
      true
    }
    override def isReleasable = 
      completed
  }

}

/**
 * Lightweight RFuture where awaits are no-ops, 
 * and notification of success/failure is simply via callbacks 
 *
 * <code>CallbackFuture</code>s can be finished multiple times, with the
 * possibility of finishing successfully once, errorneously later, or vice
 * versa. This implementation simple calls the appropriate callback right away
 * and does not keep state.
 *
 * Note: <code>notifyOnError</code> and <code>notifyOnSuccess</code> are not
 * supported for this future
 */
private[remote] class CallbackFuture(successCallback: () => Unit,
                                     errorCallback: Throwable => Unit) extends RFuture {

  /**
   * No-op
   */
  override def await(timeout: Long) {}

  /**
   * Always returns <code>None</code>
   *
   * @return None
   */
  override def awaitWithOption(timeout: Long) = None

  override def finishSuccessfully() { successCallback() }

  override def finishWithError(t: Throwable) { errorCallback(t) }

  /** 
   * Always returns <code>true</code>
   *
   * @return true
   */
  override def isFinished = true 

  /**
   * Not supported for CallbackFuture
   */
  override def notifyOnError(e: Throwable => Unit) {
    throw new RuntimeException("Cannot notify on error")
  }

  /**
   * Not supported for CallbackFuture
   */
  override def notifyOnSuccess(e: => Unit) {
    throw new RuntimeException("Cannot notify on success")
  }

  override def blocker(timeout: Long) =
    NoOpBlocker
}

private[remote] class ErrorCallbackFuture(callback: Throwable => Unit) 
  extends CallbackFuture(() => (), callback) 

private[remote] class SuccessCallbackFuture(callback: () => Unit) 
  extends CallbackFuture(callback, (_: Throwable) => ()) 

/**
 * RFuture which represents an operation which has already completed
 * successfully
 */
private[remote] object NoOpFuture extends RFuture {
  override def await(timeout: Long) {}
  override def awaitWithOption(timeout: Long) = None
  override def finishSuccessfully() {
    throw new RuntimeException("finishSuccessfully should not be called") 
  }
  override def finishWithError(t: Throwable) {
    throw new RuntimeException("finishWithError should not be called") 
  }
  override def isFinished = true
  override def notifyOnError(e: Throwable => Unit) {}
  override def notifyOnSuccess(e: => Unit) { 
    // not a no-op, because this future's semantics are that it is already
    // successful
    e 
  }
  override def blocker(timeout: Long) = NoOpBlocker
}

private[remote] object NoOpBlocker extends ManagedBlocker {
  override def block()      = true
  override def isReleasable = true
}
