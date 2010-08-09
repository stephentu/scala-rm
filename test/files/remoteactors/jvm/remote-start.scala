import scala.actors._
import Actor._
import remote._
import RemoteActor._

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.HashSet

case class Message(from: Int, message: Int)

object Test {
  // set to false because we use blocking ops
  System.setProperty("actors.enableForkJoin", "false")

  val startGuard = new CountDownLatch(1)
  val idGen      = new AtomicInteger(0)
  val numClients = 5
  val numMsgs    = 10

  class SendActor extends Actor {
    override def act() {
      val id     = idGen.getAndIncrement()
      println("SendActor started")
      startGuard.await()
      val master = select(Node("localhost", 9007), 'master)
      link(master)
      (1 to numMsgs).foreach(i => master ! Message(id, i)) 
      unlink(master)
    }
  }

  def error(a: Any) {
    println("ERROR: " + a)
    System.exit(1)
  }

  def main(args: Array[String]) {
    if (args.filter(_ == "--debug").size > 0)
      Debug.level = 3
    else
      Debug.level = 0

    enableRemoteStart()

    val master = remoteActor(9007, 'master) {
      (1 to numClients).foreach(_ => remoteStart[SendActor]("localhost"))
      while (idGen.get < numClients)
        Thread.sleep(500)
      println("MasterActor ready to go")
      startGuard.countDown()
      val counts   = (1 to numClients).map(_ => new HashSet[Int]).toArray
      var finished = 0
      while (finished < numClients) {
        receiveWithin(5000) {
          case Message(actorId, msgId) =>
            if (!counts(actorId).add(msgId))
              error("Message already seen: %d".format(msgId))
            if (counts(actorId).size == numMsgs) {
              println("Client finished")
              finished += 1
            }
          case e => error(e)
        }
      }
    }

  }
}
