import scala.actors._
import Actor._
import remote._
import RemoteActor._

import java.util.concurrent.CountDownLatch

case object DO_CRITICAL
case object DONE

object Test {
  def main(args: Array[String]) {
    if (args.filter(_ == "--debug").size > 0)
      Debug.level = 3
    else
      Debug.level = 0

    implicit val config = new DefaultNonBlockingConfiguration

    val latch = new CountDownLatch(1)
    val master = remoteActor(9004, 'master) {
      latch.countDown()
      react {
        case DO_CRITICAL =>
          println("Master got DO_CRITICAL")
          unlink(sender.receiver)
          sender ! DONE
          exit()
      }
    }

    latch.await()

    val slave = actor {
      val handle = select(Node(null, 9004), 'master)
      link(handle)
      handle ! DO_CRITICAL
      react {
        case DONE =>
          println("Slave got DONE response")
          exit()
      }
    }
  }
}
