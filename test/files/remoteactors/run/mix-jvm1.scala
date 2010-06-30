import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

case class Message(m: String, prev: Option[Message])

object NioActor extends Actor {
  def act() {
    alive(9500, serviceFactory = NioServiceFactory)
    register('nioActor, self)
    receive {
      case m @ Message(_, _) =>
        println("received m: " + m)
        sender ! Message("response", Some(m)) 
    }
    exit()
  }
}

object Test1 {
  def main(args: Array[String]) {
    Debug.level = 0
    NioActor.start
    writeFlag()
  }
}
