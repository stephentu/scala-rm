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

    val latch = new CountDownLatch(1)
    val server = remoteActor(9006, 'server) {
      latch.countDown()
      loop {
        react {
          case STOP => 
            Debug.error("server caught STOP")
            println("Server DONE")
            exit()
          case e => 
            Debug.error("Server received: " + e)
            reply(e)
        }
      }
    }

    latch.await()

    Debug.error("Server alive")

    def error(a: Any) {
      println("ERROR: " + a)
      System.exit(1)
    }

    actor {
      val handle = select(Node("127.0.0.1", 9006), 'server)
      val M1 = "Client to server [0]"
      val M2 = "Client to server [1]"
      val M3 = "Client to server [2]"
      handle ! M1 
      react {
        case M1 =>
          sender.receiver !? (5000, M2) match {
            case Some(M2) =>
              val ftch = handle !! M3
              ftch() match {
                case M3 =>
                  println("Client DONE")
                  handle ! STOP
                case m =>
                  error(m)
              }
            case m => error(m)
          }
        case m => error(m)
      }
    }
  }
}
