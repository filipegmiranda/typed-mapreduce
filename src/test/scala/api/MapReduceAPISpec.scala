package api

import engine.api.MapReduce
import org.scalatest.FlatSpec
import util.testkit.WordCountMapReduceTest

/**
  * Tests that a MapReduce Instance are correct
  */
class MapReduceSpec extends FlatSpec {

  import util.testkit.EngineTestData._

  val m: MapReduce[None.type, String, String, Int, String, Int, (String, Int)] = new WordCountMapReduceTest

  "A typed MapReduce (WordCountMapReduce) interface " should " map the input according to the implementation and type parameter " in {
    val pair  = phrases.flatMap { line =>
      m.map(None, line)
    }
    assert(pair.head === ("if", 1))
    assert(pair.drop(5).head === ("not", 1))
    val firsLineDropped = pair.drop(10)
    assert(firsLineDropped.head === ("if", 1))
    assert(firsLineDropped.head !== ("if", 2))
  }

  "A typed MapReduce (WordCount)" should " reduce the output of map correctly, according to the implementation " in {
    val pairs = phrases.flatMap(m.map(None,_))
    val reduced: Map[String, Stream[Int]] = pairs.groupBy(_._1).map(kv => kv._1 -> kv._2.map(_._2))
    reduced take 5 foreach { kv =>
      val keyPairAfterReduce: (String, Int) = m.reduce(kv._1, kv._2)
      // For the key that's the word, the count should be correct, once compared to the in memory expected result
      assert(keyPairAfterReduce._2 === reducedWordCountOutput(keyPairAfterReduce._1))
    }
  }

}



