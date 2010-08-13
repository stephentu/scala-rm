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
      private def expectError(f: => Unit) {
        try { 
          f 
          error("ERROR: Should have caught exception")
          System.exit(1)
        } catch {
          case e: Exception =>
            println("Successfully expected: " + e.getMessage)
            Debug.doError { e.printStackTrace() }
        }
      }
      private def expectErrorInHandler(f: => Unit) {
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
      override def act() {
        alive(9013)
        register('self, self)

        def makeConfig(policy: ConnectPolicy.Value) = new DefaultConfiguration {
          override def newSerializer() = new JavaSerializer {
            override val uniqueId = -1L /* intentionally make it clash */
          }
          override val connectPolicy = policy
        }

        expectError {
          val cfg    = makeConfig(ConnectPolicy.WaitHandshake)
          val mySelf = remoteActorFor(self)(cfg)
          mySelf ! "HI"
        }

        expectError {
          val cfg    = makeConfig(ConnectPolicy.WaitVerified)
          val mySelf = remoteActorFor(self)(cfg)
          mySelf ! "HI"
        }

        expectErrorInHandler {
          val cfg    = makeConfig(ConnectPolicy.NoWait)
          val mySelf = remoteActorFor(self)(cfg)
          mySelf ! "HI"
        }

        expectErrorInHandler {
          val cfg    = makeConfig(ConnectPolicy.WaitEstablished)
          val mySelf = remoteActorFor(self)(cfg)
          mySelf ! "HI"
        }
      }
    }

    val actor = new TestActor 
    actor.start()
  }
}
