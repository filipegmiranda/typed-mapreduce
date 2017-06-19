package util.testkit

import engine.dataset.OutputDataSet


case class InMemoryDataSetOutput() extends OutputDataSet[(String, Double), (String, Double)] {

  import scala.collection.mutable.Map

  val records: Map[String, Double] = Map[String, Double]()

  override def write(rec: (String, Double)): Unit = {
    records += rec
  }

  override def convert(rec: (String, Double)): (String, Double) = identity(rec)
}
