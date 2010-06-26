import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

object StopClient extends Actor {
  def act() {
    val service = select(Node("127.0.0.1", 9100), 'lookupService, serviceFactory = NioServiceFactory)
    service ! StopService()
  }
}

object Test4 {
  def main(args: Array[String]) {
    println("Starting stop client...")
    waitForFiles(Array(0,1,2))
    StopClient.start
  }
}

