package engine.api

import java.nio.file.Path

import engine.api.MapReduce.{MapReduceUpB, MapperT, ReducerT}
import engine.dataset.{FilePathInputDataSet, InputDataSet, OutputDataSet}
import engine.util.EngineLogger

import scala.concurrent.duration.Duration

trait Job extends EngineLogger {
  private var output: Option[OutputDataSet[_, _]] = None

  private val mappers = collection.mutable.Buffer[(Path, Class[_ <: MapperT])]()

  private var reducer: Class[_ <: ReducerT] = _

  protected val name: String

  protected var timeout: Duration = _

  def withTimeout(timeout: Duration = Duration.Inf): Job = {
    this.timeout = timeout
    this
  }

  def getTimeout: Duration = this.timeout

  def addReducer[A <: ReducerT](clazz: Class[A]): Job = {
    reducer = clazz
    logger.debug(s"added reducer $clazz")
    this
  }

  def addMapper[A <: MapperT](inputPath: Path, clazz: Class[A]): Job = {
    mappers +=  inputPath -> clazz
    logger.debug(s"added mapper $clazz")
    this
  }

  def inputsAndMappersClass: Seq[(InputDataSet[_], Class[_ <: MapperT])] = mappers.map(i => (new FilePathInputDataSet(i._1), i._2))

  def addSingleMapReduce[A <: MapReduceUpB](inputPath: Path, clazz: Class[A]): Job = {
    logger.debug(s"adding single MapReduce class implementation $clazz")
    require(reducer == null, "By calling the addSingleMapReduce, the Mapper also is a reducer, and the Job API's today only supports a Reducer, therefore if already defined, can't be changed")
    addMapper(inputPath, clazz)
    addReducer(clazz)
    this
  }

  def reducerClass: Class[_ <: ReducerT] = reducer


  def addOutput(outputDataSet: OutputDataSet[_, _]): Job = {
    this.output = Some(outputDataSet)
    this
  }

  def getOutput: Option[OutputDataSet[_, _]] = output

  def getName: String = name

  override def hashCode(): Int = name.hashCode

  override def equals(other: scala.Any): Boolean = this.name == other.asInstanceOf[Job].name

}

object Job {
  def apply(jName: String): Job = new Job {
    require(jName != null && jName.nonEmpty, "jobName should be given, and it can't be an empty String")
    override val name: String = jName
  }

  def newJobId(j: Job): String = {
    s"${j.name}-${java.util.UUID.randomUUID}"
  }
}

sealed trait JobAction

case object Start extends JobAction