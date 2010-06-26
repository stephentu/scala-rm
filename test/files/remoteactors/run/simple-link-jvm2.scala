import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

object Test2 {
  def main(args: Array[String]) {
    waitFor(0)
    Debug.level = 0
    actor {
      self.trapExit = true
      val master = select(Node("localhost", 9100), 'first)
      link(master)
      master ! STOP
      react {
        case exit: Exit =>
          println("received exit notification")
          exit
      }
    }
  }
}

