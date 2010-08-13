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

    val guard = new CountDownLatch(1)

    def die(f: => String) {
      println(f)
      System.exit(1) 
    }

    remoteActor(9015, 'receiver) {
      guard.countDown()
      loop { 
        react {
          case STOP =>
            println("receiver done")
            exit()
          case m @ "Message 1" =>
            println(m + " received")
            sender.receiver ! m
          case m @ "Message 2" =>
            println(m + " received")
            sender.receiver !? m match {
              case "Message 2" => println("Got ack")
              case m => die("ERROR: was expecting Message 2, but got %s".format(m))
            }
            val ftch = sender.receiver !! "Message 3"
            ftch respond {                
              case "Message 3" => println("Got ack")
              case m => die("ERROR: was expecting Message 3, but got %s".format(m))
            }
        }
      }
    }

    guard.await()

    actor {
      val handle = select(Node(null, 9015), 'receiver)
      handle ! "Message 1"
      receive {
        case "Message 1" =>
          println("Got resp back")
      }
      handle ! "Message 2"
      receive {
        case m @ "Message 2" =>
          println("Got resp back")
          sender.send(m, remoteActorFor(self))
      }
      receive {
        case m @ "Message 3" =>
          println(m + " received")
          sender ! m
      }
      handle ! STOP
    }
  }
}
