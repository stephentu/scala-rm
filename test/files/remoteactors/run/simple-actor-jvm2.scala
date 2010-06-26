import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync

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

object Test2 {
  def main(args: Array[String]) {
    FileSync.waitFor(0)
    println("Starting actor B...")
    B.start
  }
}
