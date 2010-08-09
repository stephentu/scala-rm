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

    def error(a: Any) {
      println("ERROR: " + a)
      System.exit(1)
    }

    val guard = new CountDownLatch(1) 
    val a = remoteActor(9011, 'a1) {
      guard.countDown()
      receiveWithin(5000) {
        case TIMEOUT => error(TIMEOUT)
        case e =>
          println("A1 got message: " + e)
          sender ! e
      }
    }

    guard.await()

    val receiver = actor {
      receiveWithin(5000) {
        case "MESSAGE" =>
          println("Successfully received response")
        case e => error(e)
      }
    }

    val handle = remoteActorAt(Node(null, 9011), 'a1)
    handle.send("MESSAGE", remoteActorFor(receiver))

  }
}
