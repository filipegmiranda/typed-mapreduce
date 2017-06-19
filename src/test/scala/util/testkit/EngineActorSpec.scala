package util.testkit

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

class EngineActorSpec extends TestKit(ActorSystem("MapReduceActorsSystem"))
  with ImplicitSender
  with FlatSpecLike
  with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

}
