package execution.engine.it

import java.nio.file.Paths

import engine.api.EngineContext


/**
  * Integration Test for End to End test
  */
class JobExecutionIT {

  // This Integration Test, reads 2 distinct file, having them as Inputs for the Job, and applies a MapReduce Implementation, writing to an output

  //val moviesMapper = ///new Mapper[] {}

  //val moviesRatings = ///new Mapper

  //val reducer = //new Reducer

  val context = EngineContext
  val pathMovies = Paths.get("../test-files/movies.txt")
  val pathRatings = Paths.get("../test-files/ratings.txt")
  val j = context.newJob("avg-movie-ratings")
  j.addMapper(pathMovies, null)
  j.addMapper(pathRatings, null)
  j.addReducer(null)
  j.addOutput(util.testkit.InMemoryDataSetOutput())

  val fResult = context.submit(j)
}





