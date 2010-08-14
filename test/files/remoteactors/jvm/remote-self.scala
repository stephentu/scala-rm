import scala.actors._
import Actor._
import remote._
import RemoteActor._

import java.util.concurrent.CountDownLatch

object Test {
  case class Command(proxy: AbstractActor, message: String)
  def main(args: Array[String]) {
    if (args.filter(_ == "--debug").size > 0)
      Debug.level = 3
    else
      Debug.level = 0

    def error(a: Any) {
      println("ERROR: " + a)
      System.exit(1)
    }

    implicit val config = new DefaultNonBlockingConfiguration {
      override val numRetries = 3
    }

    setExplicitShutdown(true)

    val guard = new CountDownLatch(1)

    remoteActor(9013, 'commandresponder) {
      guard.countDown()
      receiveWithin(5000) {
        case Command(proxy, message) =>
          println("Responder got message: " + message)
          proxy forward message
        case e => error(e)
      }
    }

    guard.await()

    actor {
      val remoteSelf = remoteActorFor(self)
      val responder = select(Node(null, 9013), 'commandresponder)
      responder ! Command(remoteSelf, "HELLO, WORLD")
      receiveWithin(5000) {
        case "HELLO, WORLD" =>
          sender ! "HELLO, UNIVERSE"
        case e => error(e)
      }
      receiveWithin(5000) {
        case "HELLO, UNIVERSE" =>
          println("SUCCESS")
          shutdown()
        case e => error(e)
      }
    }
  }
}
