package actors

import akka.actor.Props
import engine.executors._
import org.scalatest.tagobjects.Slow
import util.testkit._

import scala.concurrent.duration._

class MapReduceCoordinatorSpec extends EngineActorSpec {

  "An Actor MapReducerCoordinator " should
    "Send back Job completion notification the resulting should be mapped and reduced correctly " taggedAs Slow in {
    val input = Seq((InMemoryDataSetInput(), classOf[WordCountMapReduceTest]))
    val reducer = classOf[WordCountMapReduceTest]
    val fakeJobName = "actor-job-test"
    val outputDataSet = InMemoryDataSetOutput()
    val jobId = s"$fakeJobName-${java.util.UUID.randomUUID}"
    val mapReduceCoordinator = system.actorOf(Props(new MapReduceCoordinatorActor(input, reducer, outputDataSet, fakeJobName, jobId)), fakeJobName)
    mapReduceCoordinator ! engine.api.Start
    expectMsgAnyClassOf(60 seconds, classOf[JobCompleted])
    assert(EngineTestData.reducedWordCountOutput === outputDataSet.records)
  }

}
