import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._


object Test2 {
  def main(args: Array[String]) {
    Debug.level = 0
    waitFor(0)
    actor {
      val test1 = select(Node("127.0.0.1", 9100), 'test1)
      test1 !? Msg("test1 to test2 [0]") match {
        case Msg(m) =>
          println("received from test1.!?: " + m)
      }
      println("Test2 Done")
    }
  }
}

