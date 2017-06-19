package engine.executors

import akka.actor.{Actor, ActorLogging}

class MapperActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case MapperInputDescription(jobName, mapper, chunkIterator, nrChunk) =>
      log.debug(s"received message to process chunk applying mapper $mapper - with nrChunk $nrChunk in Job $jobName")
      val result = chunkIterator.flatMap { record =>
        val (k, v) = record match {
          case rec: (_, _) => (rec._1, rec._2)
          case _ => (None, record)
        }
        mapper.runMap(k, v)
      }
      val r = result.toList
      log.debug(s"finished mapper task result size ${r.size} for chunk number $nrChunk om Job $jobName")
      sender ! MapperResult(jobName, r)
  }
}
