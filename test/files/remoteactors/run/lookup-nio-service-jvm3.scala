import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

case class StopService()
case class GetRequest(request: String)
case class GetResponse(request: GetRequest, response: Option[String])

object SecondClient extends Actor {
  def act() {
    val service = select(Node("127.0.0.1", 9100), 'lookupService, serviceFactory = NioServiceFactory)
    val requests = (1 to 10).map(_.toString)
    requests.foreach(req => service ! GetRequest(req))
    // signal completion
    writeFlag()
    println("second client done")
    exit()
  }
}

object Test {
  def main(args: Array[String]) {
    println("Starting second client...")
    waitFor(0)
    SecondClient.start
  }
}
