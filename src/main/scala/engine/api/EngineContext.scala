package engine.api

import akka.actor.{ActorSystem, Props}
import akka.pattern._
import akka.util.Timeout
import engine.util.EngineLogger

import scala.concurrent.Future

/**
  * Entry and management class to control the engine, creating new Jobs, adding, removing them, and logging information about the environment
  */
object EngineContext extends EngineLogger {

  private val akkaSysName = "TypedMapReduceSystem"

  private val actorSystem = ActorSystem(akkaSysName)

  def submit(j: Job): Future[Any] = {
    val input = j.inputsAndMappersClass
    val reducer = j.reducerClass
    val outputDataSet = j.getOutput.get
    val jobId = Job.newJobId(j)
    val mapReduceCoordinator = actorSystem.actorOf(Props(new engine.executors.MapReduceCoordinatorActor(input, reducer, outputDataSet, j.getName, jobId)), j.getName)
    implicit val timeout = Timeout(j.getTimeout._1, j.getTimeout.unit)
    val f = mapReduceCoordinator ? Start
    f
  }

  def newJob(jobName: String): Job = {
    val j = Job(jobName)
    logger.debug(s"created fresh Job by name $jobName in EngineContext")
    j
  }

}

