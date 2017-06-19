package util.testkit

import engine.api.MapReduce

/**
  * Used for testing the MapReduce Interface Unit
  */
class WordCountMapReduceTest extends MapReduce[None.type, String, String, Int, String, Int, (String, Int)] {
  override def map(k: None.type, v: String): Iterable[(String, Int)] = {
    v.split(" ").map((_, 1))
  }

  override def reduce(key: String, list: Iterable[Int]): (String, Int) = {
    (key, list.fold(0)(_ + _))
  }
}
