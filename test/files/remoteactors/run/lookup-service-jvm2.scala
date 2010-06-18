import scala.actors._
import Actor._
import remote._
import RemoteActor._

import scala.tools.partest.FileSync._

case class StopService()
case class GetRequest(request: String)
case class GetResponse(request: GetRequest, response: Option[String])

object FirstClient extends Actor {
  def act() {
    val service = select(Node("127.0.0.1", 9100), 'lookupService)
    val requests = List("foo", "cat", "baz")
    requests.foreach(req => {
      service ! GetRequest(req)
      receive {
        case GetResponse(_, Some(resp)) =>
          println("GOT RESP: " + resp)
        case GetResponse(_, None)       =>
          println("GOT RESP, but no value")
      }
    })
    // signal completion
    writeFlag()
  }
}

object Test {
  def main(args: Array[String]) {
    println("Starting first client...")
    waitFor(0)
    FirstClient.start
  }
}
