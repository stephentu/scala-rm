import scala.actors._
import Actor._
import remote._
import RemoteActor._

import java.util.concurrent.CountDownLatch

object Test {

  final val M1 = "MESSAGE TO ACTOR1 WITH NAME1"
  final val M2 = "MESSAGE TO ACTOR2 WITH NAME1"

  val guard1   = new CountDownLatch(1)
  val oneDone  = new CountDownLatch(1)
  val guard2   = new CountDownLatch(1)

  class TestActor1(implicit config: Configuration) extends Actor {
    override def act() {
      alive(9012)
      register('name1, this)
      guard1.countDown()
      println("TestActor1 ready")
      receiveWithin(5000) {
        case M1 =>
          println("passed first test")
        case e => error(e)
      }
      unregister(this)
      oneDone.countDown()
    }
  }

  class TestActor2(implicit config: Configuration) extends Actor {
    override def act() {
      alive(9012)
      register('name1, this)
      guard2.countDown()
      println("TestActor2 ready")
      receiveWithin(5000) {
        case M2 =>
          println("passed second test")
        case e => error(e)
      }
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

    implicit val config = new DefaultNonBlockingConfiguration

    val testActor1 = (new TestActor1).start()
    guard1.await()
    remoteActorAt(Node(9012), 'name1) ! M1

    oneDone.await()

    val testActor2 = (new TestActor2).start()
    guard2.await()
    remoteActorAt(Node(9012), 'name1) ! M2
  }
}
