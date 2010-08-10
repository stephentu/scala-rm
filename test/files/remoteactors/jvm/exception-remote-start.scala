import scala.actors._
import Actor._
import remote._
import RemoteActor._

object Test {
  def main(args: Array[String]) {
    if (args.filter(_ == "--debug").size > 0)
      Debug.level = 3
    else
      Debug.level = 0

    enableRemoteStart(12783)

    Thread.sleep(5000) /* Let the remote start service start */

    actor {
      try {
        remoteStart(Node("localhost", 12783), "no.such.package.TestActor")
      } catch {
        case e: RemoteStartException =>
          println("PASS")
        case e: Exception =>
          println("WRONG TYPE OF EXCEPTION: " + e)
          e.printStackTrace()
      }
    }
  }
}
