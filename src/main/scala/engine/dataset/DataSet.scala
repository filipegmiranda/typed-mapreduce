package engine.dataset

import java.io.FileOutputStream
import java.nio.file.{Files, Path}
import engine.util.EngineLogger

trait InputDataSet[Record] extends EngineLogger {
  def readNext: Option[Record]

  def hasNext: Boolean

  def readSlice(from: Int, to: Int): Iterator[_]
}

trait OutputDataSet[RecordIn, RecordOut] extends EngineLogger {
  type RecIn = RecordIn

  final def convertAndWrite(rec: Any): Unit = {
    write {
      convert(rec.asInstanceOf[RecIn])
    }
  }

  def convert(rec: RecordIn): RecordOut

  def write(rec: RecordOut): Unit
}

class FilePathInputDataSet(path: Path) extends InputDataSet[String]
  with AutoCloseable {

  logger.info(s"FilePathInputDataSet created from $path")
  if (!Files.exists(path)) {
    logger.warn(s"The path does not exist - $path")
  }
  if (!Files.isReadable(path)) {
    logger.warn(s"The path is not readable - $path")
  }

  private lazy val source = io.Source.fromFile(path.toFile)
  private lazy val lines = source.getLines

  override def readNext: Option[String] = if (lines.nonEmpty) Some(lines.next) else None

  override def close(): Unit = source.close

  override def hasNext: Boolean = lines.hasNext

  override def readSlice(from: Int, to: Int): Iterator[_] = lines.slice(from, to)
}

class FilePathFromPairsToOutTypedDataSet(path: Path,
                                         createIfNotExists: Boolean = true,
                                         converter: Option[Converter[(_, _), String]] = Some(PairToStringCommaSeparated))
  extends OutputDataSet[(_, _), String]
    with AutoCloseable {

  logger.info(s"FilePathOutDataSet created from $path")
  if (!Files.exists(path) && createIfNotExists) {
    Files.createDirectories(path)
    logger.debug(s"directories created for path - $path")
  }

  lazy val outputStream = new FileOutputStream(path.toFile)

  override def write(rec: String): Unit = {
    outputStream.write(rec.getBytes)
  }

  override def close(): Unit = outputStream.close()

  override def convert(rec: (_, _)): String = {
    converter.get.convert(rec)
  }
}

trait Converter[In, Out] {
  def convert(in: In): Out
}

case object PairToStringCommaSeparated extends Converter[(_, _), String] {
  override def convert(in: (_, _)): String = {
    s"${in._1.toString}, ${in._2.toString}"
  }
}
