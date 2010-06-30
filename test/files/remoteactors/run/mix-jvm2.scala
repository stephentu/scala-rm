import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

object TcpActor extends Actor {
  def act() {
    val nio = select(Node("127.0.0.1", 9500), 'nioActor)
    nio ! Message("HELLO", None)
    react {
      case resp @ Message(_, _) =>
        println("received resp: " + resp)
    }
  }
}

object Test2 {
  def main(args: Array[String]) {
    Debug.level = 0
    waitFor(0)
    TcpActor.start
  }
}
