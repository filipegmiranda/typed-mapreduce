package util.testkit

import engine.dataset.InputDataSet
import engine.util.EngineLogger

import scala.annotation.tailrec

case class InMemoryDataSetInput(input: Stream[String] = EngineTestData.phrases,
                                multiplyEachBy: Int = 100000) extends InputDataSet[String] with EngineLogger {

  val lines: Iterator[String] = {
    input.flatMap { p =>
      @tailrec
      def loop(n: Int, s: Stream[String]): Stream[String] = if (n == 0) s else loop(n - 1, Stream.cons(p, s))

      loop(multiplyEachBy, Stream.empty)
    }
  }.iterator

  logger.debug("InMemoryDataSetInput created with words in memory")

  override def readNext: Option[String] = if (lines.nonEmpty) Some(lines.next) else None

  override def hasNext: Boolean = lines.hasNext

  override def readSlice(from: Int, to: Int): Iterator[_] = lines.slice(from, to)
}