import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.concurrent.SyncVar
import java.util.concurrent.CountDownLatch

object Test {

  private def withBlockingConnectPolicy[T](c: ConnectPolicy.Value)(f: Configuration => T) {
    val config = new DefaultConfiguration { override val connectPolicy = c }
    f(config)
  }

  private def withNonBlockingConnectPolicy[T](c: ConnectPolicy.Value)(f: Configuration => T) {
    val config = new DefaultNonBlockingConfiguration { override val connectPolicy = c }
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

  class InActLoopExceptionActor extends AwaitFinishActor { 
    private def expectError(f: => Unit) {
      try { 
        f 
        println("ERROR: Should have caught exception")
        System.exit(1)
      } catch {
        case e: Exception =>
          println("Successfully silenced: " + e.getMessage)
          Debug.doError { e.printStackTrace() }
      }
    }

    override def doAct() {
      List(ConnectPolicy.WaitEstablished, 
           ConnectPolicy.WaitHandshake, 
           ConnectPolicy.WaitVerified) foreach { p =>
        expectError {
          withBlockingConnectPolicy(p) { config =>
            val handle = select(Node("no-such-domain", 8392), 'nosuchactor)(config)
            handle ! "HI"
          }
        }
        expectError {
          withNonBlockingConnectPolicy(p) { config =>
            val handle = select(Node("no-such-domain", 8392), 'nosuchactor)(config)
            handle ! "HI"
          }
        }
      }
    }
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
        withBlockingConnectPolicy(ConnectPolicy.NoWait) { config =>
          val handle = select(Node("no-such-domain", 8392), 'nosuchactor)(config)
          handle ! "HI"
        }
      }
      exceptError {
        withNonBlockingConnectPolicy(ConnectPolicy.NoWait) { config =>
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

    (new InActLoopExceptionActor).start().asInstanceOf[AwaitFinishActor].await()
    (new ExceptionHandlerActor).start()
  }
}
