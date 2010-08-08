import scala.actors._
import Actor._
import remote._
import RemoteActor._

import java.util.concurrent.CountDownLatch

object Test {
  case class Stop(callback: () => Unit)
  def main(args: Array[String]) {
    if (args.filter(_ == "--debug").size > 0)
      Debug.level = 3
    else
      Debug.level = 0

    setExplicitShutdown(true)

    val latch = new CountDownLatch(1)
    val server = remoteActor(9002, 'server) {
      latch.countDown()
      loop {
        react {
          case Stop(callback) => 
            Debug.error("server caught STOP")
            callback()
            exit()
          case e => 
            Debug.error("Server received: " + e)
            sender ! e
        }
      }
    }

    latch.await()

    Debug.error("Server alive")

    def error(a: Any) {
      println("ERROR: " + a)
      System.exit(1)
    }

    val numClients = 3
    val numMsgsPerClient = 100
    val guard = new CountDownLatch(numClients)

    val evalActor = actor {
      val counts = new Array[Int](numClients)
      loop {
        react {
          case (actorId: Int, msgId: Int, ftch: Future[_]) =>
            ftch() match {
              case (actorId0: Int, msgId0: Int) =>
                if (actorId != actorId0)
                  error("Actor IDs do not match")
                if (msgId != msgId0)
                  error("Message IDs do not match")
                counts(actorId - 1) += 1
                if (counts(actorId - 1) == numMsgsPerClient) {
                  Debug.info("Actor: " + actorId + " is finished")
                  println("An actor is finished")
                  guard.countDown()
                }
              case e => error(e)
            }
          case Stop(callback) =>
            Debug.error("Eval actor caught STOP")
            callback()
            exit()
          case e =>
            error(e)
        }
      }
    }

    def makeActor(id: Int) = new Actor {
      override def act() {
        val handle = select(Node(null, 9002), 'server)
        val ftchs = (1 to numMsgsPerClient).map(i => handle !! ((id, i)))
        ftchs.zipWithIndex.foreach { case (ftch, idx) => evalActor ! ((id, idx + 1, ftch)) }
      }
    }

    (1 to numClients).foreach(i => makeActor(i).start())

    guard.await()

    Debug.info("All clients terminated - sending STOP messages")
    val stopGuard = new CountDownLatch(2)
    evalActor.send(Stop(() => stopGuard.countDown()), null)
    server.send(Stop(() => stopGuard.countDown()), null)

    stopGuard.await()

    shutdown()
  }
}
