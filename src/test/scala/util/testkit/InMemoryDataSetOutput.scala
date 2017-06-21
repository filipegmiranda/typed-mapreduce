package util.testkit

import engine.dataset.OutputDataSet


case class InMemoryDataSetOutput() extends OutputDataSet[(String, Int), (String, Int)] {

  import scala.collection.mutable.Map

  val records: Map[String, Int] = Map[String, Int]()

  override def write(rec: (String, Int)): Unit = {
    records += rec
  }

  override def convert(rec: (String, Int)): (String, Int) = identity(rec)
}
