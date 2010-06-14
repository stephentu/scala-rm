import scala.actors._
import Actor._
import remote._
import RemoteActor._

case class AMessage(msg: String)

object B extends Actor {
  def act() {
    println("Actor B started...")
    val aActor = select(Node("127.0.0.1", 9100), 'actorA)
    aActor ! AMessage("Hello, world")
    react {
      case AMessage(m) =>
        println(m)
    }
  }
}

object Test {
  def main(args: Array[String]) {
    println("Starting actor B...")
    B.start
  }
}
