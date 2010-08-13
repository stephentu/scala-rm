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

    val numActors = 5

    def mkSymbol(id: Int) = Symbol("actor%d".format(id))

    val guards = (for (i <- 1 to numActors) yield { new CountDownLatch(1) }).toArray
    val actors = for (i <- 0 until numActors) yield {
      if (i > 0)
        guards(i - 1).await()
      remoteActor(9016, mkSymbol(i)) {
        self.trapExit = true
        if (i > 0) {
          val prev = select(Node(9016), mkSymbol(i - 1))
          link(prev)
        }
        guards(i).countDown()
        reactWithin(30000) {
          case STOP =>
            println(mkSymbol(i) + " caught STOP")
            exit()
          case e: Exit =>
            println(mkSymbol(i) + " caught EXIT")
          case e =>
            println("ERROR: got unexpected message: " + e)
            System.exit(1)
        }
      }
    }

    guards(numActors - 1).await() // wait for last to finish

    Thread.sleep(5000) // wait 5 seconds for the link information to propogate

    actors(0).send(STOP, null) // kill the first, and watch the chain collaspe
  }
}

