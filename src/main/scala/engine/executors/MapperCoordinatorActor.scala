package engine.executors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.routing.RoundRobinPool
import engine.dataset.InputDataSet

class MapperCoordinatorActor(jobId: String, sorter: ActorRef) extends Actor with ActorLogging {

  private val chunkSize = 10000

  private val mappersRouter = context.actorOf(RoundRobinPool(Runtime.getRuntime.availableProcessors, Some(resizer)).props(Props[MapperActor]), s"mapper-router-$jobId")

  private var nrMappedChunks = 0

  override def receive: Receive = {
    case MappperWithInput(i, m) =>
      val input = i
      val initial = 0
      var nrChunk = 0
      while (input.hasNext) {
        val chunkIterator = input.readSlice(initial, chunkSize).toList
        nrMappedChunks += 1
        nrChunk += 1
        mappersRouter ! MapperInputDescription(jobId, m, chunkIterator, nrChunk)
      }
      closeInputResources(input)
    case MapperResult(jName, r) =>
      nrMappedChunks -= 1
      sorter ! PreSort(jName, r)
      if (nrMappedChunks == 0) sorter ! Sort(jName)
    case s: Sorted =>
      context.parent ! s
  }

  private def closeInputResources(input: InputDataSet[_]): Unit = {
    input match {
      case i: AutoCloseable => i.close()
      case _ => log.debug(s"tried closing all inputDataSets resources of job $jobId, but none is AutoCloseable")
    }
  }

}
