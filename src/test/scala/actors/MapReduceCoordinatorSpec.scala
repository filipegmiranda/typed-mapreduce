package actors

import akka.actor.Props
import engine.dataset.OutputDataSet
import engine.executors._
import org.scalatest.tagobjects.Slow
import util.testkit.{EngineActorSpec, InMemoryDataSetInput, InMemoryDataSetOutput, WordCountMapReduceTest}

import scala.concurrent.duration._

class MapReduceCoordinatorSpec extends EngineActorSpec {

  "An Actor MapReducerCoordinator " should "Send back Job completion notification " taggedAs Slow in {
    val input = Seq(InMemoryDataSetInput())
    val mapReduce = new WordCountMapReduceTest
    val mapper = mapReduce
    val reducer = new WordCountMapReduceTest
    val fakeJobName = "actor-job-test"
    val outputDataSet: OutputDataSet[_, _] = InMemoryDataSetOutput()
    val jobId = s"$fakeJobName-${java.util.UUID.randomUUID}"
    val mapReduceCoordinator = system.actorOf(Props(new MapReduceCoordinatorActor(input, mapper, reducer, outputDataSet, fakeJobName, jobId)), fakeJobName)
    mapReduceCoordinator ! engine.api.Start
    expectMsgAnyClassOf(60 seconds, classOf[JobCompleted])
  }

}
