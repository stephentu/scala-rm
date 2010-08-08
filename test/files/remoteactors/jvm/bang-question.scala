import scala.actors._
import Actor._
import remote._
import RemoteActor._

import java.util.concurrent.CountDownLatch

object Test {
  def main(args: Array[String]) {
    if (args.filter(_ == "--debug").size > 0)
      Debug.level = 3
    else
      Debug.level = 0

    val latch = new CountDownLatch(1)
    val server = remoteActor(9001, 'server) {
      latch.countDown()
      loop {
        react {
          case "STOP" => 
            println("SERVER STOPPING")
            exit()
          case e      => sender ! e
        }
      }
    }

    latch.await()

    val client = actor {
      val handle = select(Node(null, 9001), 'server)
      var I = 0
      loopWhile(I < 100) {
        handle !? (5000, I) match {
          case Some(i) if (i == I) =>
            println("GOT: " + I)
            I += 1
            if (I == 100) {
              println("CLIENT STOPPING")
              handle ! "STOP"
            }
          case i =>
            println("ERROR: " + i)
            System.exit(1)
        }
      }
    }
  }
}


