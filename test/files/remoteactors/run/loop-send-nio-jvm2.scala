import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

object Test2 {
  def main(args: Array[String]) {
    Debug.level = 0
    //waitFor(0)
    def msg = "test2 -> test1"
    actor {
      val test1 = select(Node("127.0.0.1", 9111), 'test1, serviceMode = ServiceMode.NonBlocking)
      test1 ! msg
      var i = 1
      loopWhile(i <= 10001) {
        if (i <= 10000) {
          react {
            case "test1 -> test2" =>
              i += 1
              if ((i % 10) == 0) println("On i = " + i)
              sender ! msg
          }
        } else {
          println("Calling release resources in actor")
          releaseResourcesInActor()
          exit()
        }
      }
    }
  }
}
