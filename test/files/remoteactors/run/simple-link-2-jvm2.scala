import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

object Slave extends Actor {
  //Debug.level = 3
  def act() {
    val master = select(Node("localhost", 9900), 'master)
    master ! "Hello"

    loop {
      react {
        case x =>
          println("* slave: message received: " + x)
          println("* slave: exiting now...")
          exit
      }
    }
  }
}

object Test2 {
  def main(args: Array[String]) {
    waitFor(0)
    Slave.start
  }
}
