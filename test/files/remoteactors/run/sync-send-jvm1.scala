import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

case class Msg(m: String)

object Test1 {
  def main(args: Array[String]) {
    Debug.level = 0
    actor {
      alive(9100)
      register('test1, self)
      writeFlag()
      receive {
        case Msg(m) =>
          println("received " + m)
          sender ! Msg("test1 to test2 [1]")
      }
      println("Test1 Done")
    }
  }
}
