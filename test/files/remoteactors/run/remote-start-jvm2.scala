import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

import java.net.Socket

object B extends Actor {

  def act {
    println("Node B: Starting remote actor")
    remoteStart(
      Node("127.0.0.1", 9000),
      defaultSerializer,
      "TestActor",
      8000,
      'testActor,
      None,
      None)
  }

}

object Test2 {
  def main(args: Array[String]) {
    waitFor(0)
    B.start
  }
}
