import scala.actors._
import Actor._
import remote._
import RemoteActor._

object Test extends Actor {
  def act() {
    try {
      val a = select(Node("no-such-domain", 8392), 'nosuchactor) 
    } catch {
      case e: Exception =>
        println("Could not connect to no-such-domain")
    }
  }
  def main(args: Array[String]) {
    Test.start() 
  }
}
