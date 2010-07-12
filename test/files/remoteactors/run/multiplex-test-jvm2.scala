import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

object Test2 {

  def startQueryClient(sym: Symbol) = 
    actor {
      val idStr = "<client: >"
      //println("query client: started")
      (1 to 3).map(i => Symbol("server" + i)).foreach(ssym => {
        val server = select(Node("127.0.0.1", 9501), ssym, serviceMode = ServiceMode.NonBlocking)
        server ! Query(sym.toString)
      })
      var num = 0
      loopWhile(num <= 3) {
        if (num < 3) {
          react {
            case r: Resp =>
              num += 1
              println(idStr + ": received message")
          }
        } else {
          releaseResourcesInActor()
        }
      }
    }

  def main(args: Array[String]) {
    Debug.level = 0
    //waitFor(0)
    (1 to 3).foreach(i => startQueryClient(Symbol("client" + i)))
  }
}

