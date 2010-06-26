import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

class TestActor extends Actor {
  def act() {
    println("TestActor is now started")
    stopListeners
  }
}

object Test1 {
  def main(args: Array[String]) {
    startListeners
    writeFlag()
  }
}
