package engine.executors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.routing.RoundRobinPool
import engine.api.MapReduce.{MapperT, ReducerT}
import engine.api.Start
import engine.dataset.{InputDataSet, OutputDataSet}

class MapReduceCoordinatorActor(inputDataSet: Seq[(InputDataSet[_], Class[_ <: MapperT])],
                                reducerClass: Class[_ <: ReducerT],
                                outputDataSet: OutputDataSet[_, _],
                                jobName: String,
                                jobId: String)
  extends Actor with ActorLogging {

  private val reducersRouter = context.actorOf(RoundRobinPool(Runtime.getRuntime.availableProcessors, Some(resizer)).props(Props[ReducerActor]), s"reducer-router-$jobId")

  private val sorter = context.actorOf(Props[SorterActor], s"sorter-$jobId")

  private val writer = context.actorOf(Props[WriterActor], s"writer-$jobId")

  private var nrReduceChunks = 0

  private var startTime: Long = _

  private var jobStarterSender: ActorRef = _

  override def receive: Receive = {
    case Start =>
      startTime = System.currentTimeMillis
      log.info(s"starting new Job $jobId after request message Start")
      jobStarterSender = sender()
      inputDataSet.foreach { i =>
        val mapperCoordinator = context.actorOf(Props(new MapperCoordinatorActor(jobId, sorter)), s"mapper-coordinator-of-job-$jobId-${System.currentTimeMillis}")
        mapperCoordinator ! MappperWithInput(i._1, i._2.newInstance())
      }
    case Sorted(jName, r) =>
      val partitions: Map[Any, Seq[(Any, Any)]] = r.groupBy(_._1)
      val reducer = reducerClass.newInstance
      partitions.foreach { p =>
        nrReduceChunks += 1
        reducersRouter ! ReducerDescription(jName, reducer, p._2)
      }
    case ReductionResult(jName, r) =>
      print(r)
      writer ! OutputDescription(jName, outputDataSet, r)
    case WrittenSucceeded(jName) =>
      nrReduceChunks -= 1
      if (nrReduceChunks == 0) {
        self ! Finish(jName, None)
      }
    case WrittenFailed(jName) =>
      log.error(s"written failed in job $jName ... closing resources if any")
      closeOutputResources()
      self ! Finish(jName, Some(new RuntimeException(s"Failed with a problem while writing to OutputDataSet, check logs for Job $jName")))
    case Finish(jName, e) =>
      log.info(s"Job  $jName normally Finished... closing resources if any")
      closeOutputResources()
      jobStarterSender ! JobCompleted(jName, jobId, System.currentTimeMillis - startTime, e)
      context stop self
  }

  private def closeOutputResources(): Unit = {
    outputDataSet match {
      case o: AutoCloseable => o.close()
      case _ => log.debug(s"tried closing outputDataSet resources resources of job $jobId, but it is AutoCloseable")
    }
  }

}
