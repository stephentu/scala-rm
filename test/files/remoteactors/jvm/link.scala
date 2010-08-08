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

    implicit val config = new DefaultNonBlockingConfiguration

    val latch = new CountDownLatch(1)
    val master = remoteActor(9003, 'master) {
      latch.countDown()
      react {
        case STOP =>
          println("Master is stopping")
          exit()
      }
    }

    latch.await()

    val slave = actor {
      self.trapExit = true
      val handle = select(Node(null, 9003), 'master)
      link(handle)
      handle ! STOP
      react {
        case e: Exit =>
          println("Slave is stopping")
          exit()
      }
    }
  }
}
