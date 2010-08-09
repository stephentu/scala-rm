import scala.actors._
import Actor._
import remote._
import RemoteActor._

object Test {

  case object EXITING

  def error(a: Any) {
    println("ERROR: " + a)
    System.exit(1)
  }

  class StartActor extends Actor {
    override def act() {
      println("StartActor alive")
      receive {
        case "STOP" =>
          exit(EXITING)
        case e => error(e)
      }
    }
  }

  val startPort = 11928
  val startName = 'startActor

  def main(args: Array[String]) {
    if (args.filter(_ == "--debug").size > 0)
      Debug.level = 3
    else
      Debug.level = 0

    implicit val config = new DefaultNonBlockingConfiguration

    enableRemoteStart()

    Thread.sleep(5000) /* Let the remote start service start-up */

    actor {
      self.trapExit = true
      val handle = remoteStart[StartActor]("127.0.0.1", startPort, startName)
      link(handle)
      handle ! "STOP"
      receive {
        case Exit(_, EXITING) =>
          unlink(handle)
          println("Got successful exit")
        case e => error(e)
      }
    }
  }
}
