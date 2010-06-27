import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

case object STOP

object Test1 {
  def main(args: Array[String]) {
    Debug.level = 0
    actor {
      alive(9103)
      register('first, self)
      writeFlag()
      react {
        case STOP =>
          println("first is stopping")
          exit
      }
    }
  }
}
