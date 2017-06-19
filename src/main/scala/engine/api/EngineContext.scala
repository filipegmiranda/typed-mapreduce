package engine.api

import akka.actor.{ActorRef, ActorSystem, Props}
import engine.api.MapReduce.{MapReduceUpB, ReducerT}
import engine.util.EngineLogger
import akka.pattern._
import akka.util.Timeout
import scala.concurrent.Future

/**
  * Entry and management class to control the engine, creating new Jobs, adding, removing them, and logging information about the environment
  */
object EngineContext extends EngineLogger {

  private val akkaSysName = "TypedMapReduceSystem"

  private val actorSystem = ActorSystem(akkaSysName)

  def submit(j: Job): Future[Any] = {
    val input = j.getInputDataSetPaths
    val mapper = j.getSingleMapperClass.newInstance
    val reducer = if (mapper.isInstanceOf[MapReduceUpB]) mapper.asInstanceOf[ReducerT] else j.getReducer.get.newInstance
    val outputDataSet = j.getOutput.get
    val jobId = Job.newJobId(j)
    val mapReduceCoordinator = actorSystem.actorOf(Props(new engine.executors.MapReduceCoordinatorActor(input, mapper, reducer, outputDataSet, j.getName, jobId)), j.getName)
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

