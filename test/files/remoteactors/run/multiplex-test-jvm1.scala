import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

case class Query(msg: String)
case class Resp(thisServer: String, originServer: String)

object Test1 {

  def startEchoServer(sym: Symbol) = 
    actor {
      val idStr = "<server: >"
      alive(9501, ServiceMode.NonBlocking)
      register(sym, self)
      //println("echo server: started")
      var num = 0
      loopWhile(num <= 3) {
        if (num < 3) {
          react {
            case Query(msg) =>
              num += 1
              println(idStr + ": received message")
              sender ! Resp(sym.toString, msg) 
          }
        } else {
          releaseResourcesInActor()
        }
      }
    }

  def main(args: Array[String]) {
    Debug.level = 0
    (1 to 3).foreach(i => startEchoServer(Symbol("server" + i)))
    //writeFlag()
  }
}

