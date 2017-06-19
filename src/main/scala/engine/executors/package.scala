package engine

import engine.api.MapReduce.{MapperT, ReducerT}
import engine.dataset.OutputDataSet

package object executors {

  /**
    * All Messages exchanged bt Actors in this package below
    */

  case class OutputDescription(jobName: String, outputDataSet: OutputDataSet[_, _], value: Any)

  case class MapperInputDescription(jobName: String, m: MapperT, chunkInput: TraversableOnce[_], nrChunk: Int)

  case class ReducerDescription(jobName: String, r: ReducerT, chunk: Seq[(Any, Any)])

  case class MapperResult(jobName: String, a: Seq[(Any, Any)])

  case class Sorted(jobName: String, s: Seq[(Any, Any)])

  case class ReductionResult(jobName: String, r: Any)

  case class Finish(jobName: String, ex: Option[Exception])

  sealed abstract class WrittenStatus(jobName: String)

  case class WrittenSucceeded(jobName: String) extends WrittenStatus(jobName)

  case class WrittenFailed(jobName: String) extends WrittenStatus(jobName)

  sealed abstract class SortingAction(jobName: String)

  case class PreSort(jobName: String, a: Seq[(Any, Any)]) extends SortingAction(jobName)

  case class Sort(jobName: String) extends SortingAction(jobName)

  case class JobCompleted(jobName: String, jobId:String, totalTime: Long, exception: Option[Exception])
}
