import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.concurrent.SyncVar
import java.util.concurrent.CountDownLatch

object Test {
  def main(args: Array[String]) {
    if (args.filter(_ == "--debug").size > 0)
      Debug.level = 3
    else
      Debug.level = 0

    class TestActor extends Actor {
      private def expectError(desc: => String)(f: => Unit) {
        try { 
          println(desc)
          f 
          error("ERROR: Should have caught exception")
          System.exit(1)
        } catch {
          case e: Exception =>
            println("Successfully expected: " + e.getMessage)
            Debug.doError { e.printStackTrace() }
        }
      }
      private def expectErrorInHandler(desc: => String)(f: => Unit) {
        println(desc)
        f
        guard.get(5000) match {
          case Some(true) =>
            println("Successfully caught error")
            guard.unset()
          case _ =>
            error("ERROR: Did not get exception")
            System.exit(1)
        }
      }
      private val guard = new SyncVar[Boolean]
      override def exceptionHandler: PartialFunction[Exception, Unit] = {
        case e: Exception => 
          println("Successfully caught: " + e.getMessage)
          Debug.doError { e.printStackTrace() }
          guard.set(true)
      }
      trapExit = true
      override def act() {
        alive(9014)
        register('self, self)

        def makeConfig(select: ServiceMode.Value) = new DefaultConfiguration {
          override val selectMode        = select
          override val lookupValidPeriod = 0L /** Don't cache lookups */
        }

        expectErrorInHandler("Blocking") {
          val cfg    = makeConfig(ServiceMode.Blocking)
          val nobody = select(Node(9014), 'nobody)(cfg)
          nobody ! "HI"
        }

        expectErrorInHandler("NonBlocking") {
          val cfg    = makeConfig(ServiceMode.NonBlocking)
          val nobody = select(Node(9014), 'nobody)(cfg)
          nobody ! "HI"
        }
      }
    }

    val actor = new TestActor 
    actor.start()
  }
}
