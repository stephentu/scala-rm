import scala.actors._
import Actor._
import remote._
import RemoteActor._

case class AMessage(msg: String)

object A extends Actor {
  def act() {
    alive(9100)
    register('actorA, self)
    println("Actor A started...")
    react {
      case AMessage(m) => 
        println("received: " + m)
        sender ! AMessage("Right back at you!")
    }
  }
}

object Test {
  def main(args: Array[String]) {
    println("Starting actor A...")
    A.start
  }
}
