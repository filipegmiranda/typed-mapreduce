package engine.executors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.routing.{DefaultResizer, RoundRobinPool}
import engine.api.Start
import engine.api.MapReduce.{MapperT, ReducerT}
import engine.dataset.{InputDataSet, OutputDataSet}

class MapReduceCoordinatorActor(inputDataSet: Seq[InputDataSet[_]],
                                mapper: MapperT,
                                reducer: ReducerT,
                                outputDataSet: OutputDataSet[_, _],
                                jobName: String,
                                jobId: String)
  extends Actor with ActorLogging {

  private val chunkSize = 10000

  private val mappersRouter = context.actorOf(RoundRobinPool(Runtime.getRuntime.availableProcessors, Some(resizer)).props(Props[MapperActor]), s"mapper-router-$jobId")

  private val reducersRouter = context.actorOf(RoundRobinPool(Runtime.getRuntime.availableProcessors, Some(resizer)).props(Props[ReducerActor]), s"reducer-router-$jobId")

  private val sorter = context.actorOf(Props[SorterActor], s"sorter-$jobId")

  private val writer = context.actorOf(Props[WriterActor], s"writer-$jobId")

  private var nrMappedChunks = 0

  private var nrReduceChunks = 0

  private var startTime: Long = _

  private var jobStarterSender: ActorRef = _

  override def receive: Receive = {
    case Start =>
      startTime = System.currentTimeMillis
      log.info(s"starting new Job $jobId after request message Start")
      val input = inputDataSet.head
      val initial = 0
      var nrChunk = 0
      jobStarterSender = sender()
      while (input.hasNext) {
        val chunkIterator = input.readSlice(initial, chunkSize).toList
        nrMappedChunks += 1
        nrChunk += 1
        mappersRouter ! MapperInputDescription(jobId, mapper, chunkIterator, nrChunk)
      }
      closeInputResources() // TODO Define in Supervision Strategy the closing of resource in case of failure
    case MapperResult(jName, r) =>
      nrMappedChunks -= 1
      sorter ! PreSort(jName, r)
      if (nrMappedChunks == 0) sorter ! Sort(jName)
    case Sorted(jName, r) =>
      val partitions: Map[Any, Seq[(Any, Any)]] = r.groupBy(_._1)
      partitions.foreach { p =>
        nrReduceChunks += 1
        reducersRouter ! ReducerDescription(jName, reducer, p._2)
      }
    case ReductionResult(jName, r) =>
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

  private def closeInputResources(): Unit = {
    inputDataSet.foreach {
      case i: AutoCloseable => i.close()
      case _ => log.debug(s"tried closing all inputDataSets resources of job $jobId, but none is AutoCloseable")
    }
  }

  private def resizer = DefaultResizer(upperBound = Runtime.getRuntime.availableProcessors * 10, rampupRate = 0.5, backoffRate = 0.3)

}
