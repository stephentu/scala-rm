import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync

case class AMessage(msg: String)

object A extends Actor {
  def act() {
    alive(9106, ServiceMode.NonBlocking)
    register('actorA, self)
    println("Actor A started...")
    react {
      case AMessage(m) => 
        println("received: " + m)
        sender ! AMessage("Right back at you!")
        RemoteActor.releaseResourcesInActor()
    }
  }
}

object Test1 {
  def main(args: Array[String]) {
    Debug.level = 0
    println("Starting actor A...")
    A.start
    //FileSync.writeFlag()
  }
}
