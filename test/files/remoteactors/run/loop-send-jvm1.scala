import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

object Test1 {
  def main(args: Array[String]) {
    Debug.level = 0
    actor {
      alive(9112)
      register('test1, self)
      //writeFlag()
      var i = 1
      loopWhile(i <= 10001) {
        if (i <= 10000) {
          react {
            case "test2 -> test1" =>
              i += 1
              if ((i % 10) == 0) println("On i = " + i)
              sender ! "test1 -> test2"
          }
        } else {
          releaseResourcesInActor()
          exit()
        }
      }
      println("Test1 is done!")
    }
  }
}
