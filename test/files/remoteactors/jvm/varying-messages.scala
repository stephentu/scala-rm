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
    val server = remoteActor(9017, 'server) {
      latch.countDown()
      loop {
        react {
          case "STOP" => 
            println("SERVER STOPPING")
            exit()
          case e      => sender ! "ACK" 
        }
      }
    }

    latch.await()

    def makeMessage(s: Int) =
      new Array[Byte](s) 

    def makeActor(config: Configuration) = {
      val guard = new CountDownLatch(1)
      actor {
        val handle = select(Node(null, 9017), 'server)(config)
        for (i <- 1 to 100) {
          val msgSize = i * 8 * 16000
          handle !? (5 * 60 * 1000, makeMessage(msgSize)) match {
            case Some("ACK") => // success
            case _ =>
              println("ERROR: server did not ACK")
              System.exit(1)
          }
        }
        guard.countDown()
      }
      guard
    }

    val blocking = new DefaultConfiguration
    val guard1   = makeActor(blocking)
    guard1.await()
    println("Blocking Done")

    val nonblocking = new DefaultNonBlockingConfiguration
    val guard2      = makeActor(nonblocking)
    guard2.await()
    println("NonBlocking Done")

    server.send("STOP", null)
  }
}
