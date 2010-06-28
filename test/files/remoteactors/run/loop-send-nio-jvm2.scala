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
      val test1 = select(Node("127.0.0.1", 9111), 'test1, serviceFactory = NioServiceFactory)
      test1 ! msg
      var i = 1
      loopWhile(i <= 10000) {
        react {
          case "test1 -> test2" =>
            i += 1
            if ((i % 1000) == 0) println("On i = " + i)
            sender ! msg
        }
      }
      println("Test2 is done!")
    }
  }
}
