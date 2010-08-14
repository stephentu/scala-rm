import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.concurrent.SyncVar
import java.util.concurrent.CountDownLatch

object Test {

  private def withBlocking[T](f: Configuration => T) {
    val config = new DefaultConfiguration
    f(config)
  }

  private def withNonBlocking[T](f: Configuration => T) {
    val config = new DefaultNonBlockingConfiguration
    f(config)
  }

  trait AwaitFinishActor extends Actor {
    private[this] val guard = new CountDownLatch(1)
    protected def doAct(): Unit
    override final def act() {
      doAct()
      guard.countDown()
    }
    def await() { guard.await() }
  }

  class ExceptionHandlerActor extends Actor {
    private val guard = new SyncVar[Boolean]

    override def exceptionHandler: PartialFunction[Exception, Unit] = {
      case e: Exception =>
        println("Successfully caught: " + e.getMessage)
        Debug.doError { e.printStackTrace() }
        guard.set(true)
    }

    private def exceptError(f: => Unit) {
      try { f } catch {
        case e: Exception =>
          println("Successfully caught: " + e.getMessage)
          Debug.doError { e.printStackTrace() }
          guard.set(true)
      }
      guard.get(5000) match {
        case Some(true) =>
          println("Successfully caught error")
          guard.unset()
        case _ =>
          println("ERROR: Did not get exception")
          System.exit(1)
      }
    }

    override def act() {
      exceptError {
        withBlocking { config =>
          val handle = select(Node("no-such-domain", 8392), 'nosuchactor)(config)
          handle ! "HI"
        }
      }
      exceptError {
        withNonBlocking { config =>
          val handle = select(Node("no-such-domain", 8392), 'nosuchactor)(config)
          handle ! "HI"
        }
      }
    }
  }

  def main(args: Array[String]) {
    if (args.filter(_ == "--debug").size > 0)
      Debug.level = 3
    else
      Debug.level = 0

    (new ExceptionHandlerActor).start()
  }
}
