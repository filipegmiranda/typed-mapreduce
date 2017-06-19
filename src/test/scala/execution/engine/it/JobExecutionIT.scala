package execution.engine.it

import engine.api.EngineContext


/**
  * Integration Test for End to End test
  */
class JobExecutionIT {

  // This Integration Test, reads 2 distinct file, having them as Inputs for the Job, and applies a MapReduce Implementation, writing to an output

  // define output

  val context = EngineContext
  val pathMovies = "test-files/movies.txt"
  val pathRatings = "test-files/ratings.txt"
  val j = context.newJob("avg-ratings")
  //val output =
  j.addInputPathsFromStrs(pathMovies, pathRatings)
  j.addSingleMapReduce(null)
  j.addOutput(util.testkit.InMemoryDataSetOutput())

  context.submit(j)

}





