package engine.executors

import akka.actor.{Actor, ActorLogging}

class SorterActor extends Actor with ActorLogging {

  private val items = scala.collection.mutable.ListBuffer[(Any, Any)]()

  override def receive: Receive = {
    case PreSort(jobName, a) =>
      log.debug(s"caching for preSorter in Mapper for job $jobName items size to cache ${a.size}")
      items.append(a: _*)
    case Sort(jobName) =>
      log.debug(s"sorting <key , values> phase initiated for job $jobName - items in size ${items.size}")
      //TODO check sorting
      sender ! Sorted(jobName, items)
  }
}

