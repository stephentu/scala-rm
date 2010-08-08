import scala.actors._
import Actor._
import remote._
import RemoteActor._

case class AMessage(msg: String)

object A extends Actor {
  def act() {
    alive(9102)
    register('actorA, self)
    println("Actor A started...")
    react {
      case AMessage(m) => 
        println("received: " + m)
        sender ! AMessage("Right back at you!")
    }
  }
}

object Test1 {
  def main(args: Array[String]) {
    Debug.level = 0
    println("Starting actor A...")
    A.start
  }
}
