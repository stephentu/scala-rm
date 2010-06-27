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
      var msgCount = 0
      while (msgCount < 3) {
        receive {
          case Msg(m) =>
            println("received [" + msgCount + "]: " + m)
            Debug.info("sender is: " + sender)
            reply( Msg("test1 to test2 [" + msgCount + "]") ) 
            msgCount += 1
        }
      }
      println("Test1 Done")
    }
  }
}
