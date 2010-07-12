import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

object SecondClient extends Actor {
  def act() {
    val service = select(Node("127.0.0.1", 9100), 'lookupService, serviceMode = ServiceMode.NonBlocking)
    val requests = (1 to 10).map(_.toString)
    requests.foreach(req => service ! GetRequest(req))
    // signal completion
    //writeFlag()
    println("second client done")
    releaseResourcesInActor()
    exit()
  }
}

object Test3 {
  def main(args: Array[String]) {
    Debug.level = 0
    println("Starting second client...")
    //waitFor(0)
    SecondClient.start
  }
}
