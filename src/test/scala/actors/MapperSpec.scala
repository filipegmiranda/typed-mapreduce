package actors

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import engine.executors.{MapperActor, MapperResult}
import org.scalatest.{BeforeAndAfter, FlatSpecLike}
import util.testkit.{EngineActorSpec, EngineTestData, WordCountMapReduceTest}

class MapperSpec extends EngineActorSpec {

  " A Mapper Actor  " should " map an input Key Value Pair to the expected result after running a mapper algorithm over the data Phrases " in {
    val mapper = system.actorOf(Props[MapperActor], "mapper")
    val mapperAlg = new WordCountMapReduceTest
    val phrases = EngineTestData.phrases
    val jobName = "fake-job"
    val mapperTaskDesc = engine.executors.MapperInputDescription(jobName, mapperAlg, phrases, 1)
    mapper ! mapperTaskDesc
    expectMsg(MapperResult(jobName, EngineTestData.mapperResult))
  }

}
