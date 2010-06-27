import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

object Master extends Actor {
  trapExit = true
  def act() {
    //Debug.level = 3
    alive(9901)
    register('master, self)
    writeFlag()
    loop {
      react {
        case e: Exit =>
          println("* master: slave disconnected. Exiting too...")
          exit
        case x =>
          println("* master: message received: " + x)
          link(sender.asInstanceOf[AbstractActor])
          sender ! "Connected"
      }
    }
  }
}

object Test1 {
  def main(args: Array[String]) {
    Debug.level = 0
    Master.start
  }
}
