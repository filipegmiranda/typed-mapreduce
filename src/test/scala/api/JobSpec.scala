package api


import org.scalatest.FlatSpec


/**
  * Used to test the Job trait Unit
  */
class JobSpec extends FlatSpec {

//  import EngineTest.WordCountMapReduce
//
//  val context = new EngineContext("test-app")
//
//  val path = Paths.get(System.getProperty("user.home"))
//  val input = new FilePathInputDataSet(path)
//  val output = new FilePathOutputDataSet(path)
//
//  "A new Job created by a programmer using the Engine " should "add an Input Data Set and Mapper plus Reducer correctly" in {
//    val job = context.newJob("test-job")
//    job.addInputAndMapper(input, classOf[WordCountMapReduce])
//    assert(job.name === "test-job")
//    assert(job.getStatus === Idle)
//    assert(job.getInputAndMappers.head === (input, classOf[WordCountMapReduce]))
//  }
//
//  "A Job when created firstly " should "have no input defined, but once added should keep the input " in {
//    val job = context.newJob("test-job")
//    assert(job.getInputAndMappers === Seq.empty)
//    val path = Paths.get(System.getProperty("user.home"))
//    job.addInputAndMapper(input, classOf[WordCountMapReduce])
//    assert(job.getInputAndMappers.head._1 === input)
//  }
//
//  "A Job when created firstly " should "have no output defined, but once added should keep the output " in {
//    val job = context.newJob("test-job")
//    assert(job.getOutput === Option.empty)
//    job.addOutput(output)
//    assert(job.getOutput === Some(output))
//  }

}
