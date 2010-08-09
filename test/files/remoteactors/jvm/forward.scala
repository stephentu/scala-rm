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

    setExplicitShutdown(true)

    val guard = new CountDownLatch(3) 

    val a1 = remoteActor(9008, 'a1) {
      guard.countDown()
      receiveWithin(5000) {
        case TIMEOUT => error(TIMEOUT)
        case e =>
          println("A1 got message: " + e)
          sender ! e
      }
    }

    val a2 = remoteActor(9009, 'a2) {
      guard.countDown()
      reactWithin(5000) {
        case TIMEOUT => error(TIMEOUT)
        case e =>
          println("A2 got message: " + e)
          val a1_handle = select(Node(9008), 'a1)
          a1_handle forward e
      }
    }

    val a3 = remoteActor(9010, 'a3) {
      guard.countDown()
      receiveWithin(5000) {
        case TIMEOUT => error(TIMEOUT)
        case e =>
        println("A3 got message: " + e)
        val a2_handle = select(Node(9009), 'a2)
        a2_handle forward e
      }
    }

    guard.await()

    actor {
      val a3_handle = select(Node(null, 9010), 'a3)
      a3_handle !? (5000, "FORWARD ME") match {
        case Some("FORWARD ME") =>
          println("Successfully received response")
          shutdown()
        case e => error(e)
      }
    }
  }
}
