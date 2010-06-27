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
      val test1 = select(Node("127.0.0.1", 9107), 'test1)
      test1 ! Msg("test2 to test1 [0]")
      react {
        case Msg(m) =>
          println("received " + m)
          sender.receiver !? Msg("test2 to test1 [1]") match {
            case Msg(m) =>
              println("received " + m)
              val ft = test1 !! Msg("test2 to test1 [2]")
              ft() match {
                case Msg(m) =>
                  println("received " + m)
                  println("Test2 Done")
              }
          }
      }
    }
  }
}
