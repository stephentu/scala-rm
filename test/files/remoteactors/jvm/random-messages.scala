import scala.actors._
import Actor._
import remote._
import RemoteActor._

import java.util.concurrent.CountDownLatch

import scala.util.Random

object Test {
  def main(args: Array[String]) {
    if (args.filter(_ == "--debug").size > 0)
      Debug.level = 3
    else
      Debug.level = 0

    val latch = new CountDownLatch(1)
    val server = remoteActor(9018, 'server) {
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

    val random = new Random

    def makeNewMessage() = {
      val s = random.nextInt(8192000) + 1
      new Array[Byte](s) 
    }

    def makeActor(config: Configuration) = {
      val guard = new CountDownLatch(1)
      actor {
        val handle = select(Node(null, 9018), 'server)(config)
        for (i <- 1 to 100) {
          handle !? (5 * 60 * 1000, makeNewMessage()) match {
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

