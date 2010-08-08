import scala.actors._
import Actor._
import remote._
import RemoteActor._

import java.util.concurrent.CountDownLatch

case object STOP

object Test {
  def main(args: Array[String]) {
    if (args.filter(_ == "--debug").size > 0)
      Debug.level = 3
    else
      Debug.level = 0

    val blockingConfig    = new DefaultConfiguration
    val nonBlockingConfig = new DefaultNonBlockingConfiguration

    def error(a: Any) {
      println("ERROR: " + a)
      System.exit(1)
    }

    val latch = new CountDownLatch(1)
    val server = remoteActor(9005, 'server)({
      latch.countDown()
      loop {
        react {
          case STOP =>
            println("Server going down")
            exit()
          case e =>
            sender ! e
        }
      }
    })(blockingConfig)

    latch.await()

    val client = actor {
      val handle = select(Node(null, 9005), 'server)(nonBlockingConfig)
      handle ! "HELLO 1"
      receiveWithin(5000) {
        case "HELLO 1" =>
          println("Got HELLO 1")
        case e => error(e)
      }
      val ftch = handle !! "HELLO 2"
      ftch() match {
        case "HELLO 2" =>
          println("Got HELLO 2")

        case e => error(e)
      }
      handle !? (5000, "HELLO 3") match {
        case Some("HELLO 3") =>
          println("Got HELLO 3")
        case e => error(e)
      }
      handle ! STOP
    }
  }
}
