package execution.engine.it

import java.nio.file.Paths

import engine.api.{EngineContext, Mapper, Reducer}
import org.scalatest.FlatSpec

import scala.concurrent.duration._


/**
  * Integration Test for End to End test
  */
class JobExecutionIT extends FlatSpec {

  // This Integration Test, reads 2 distinct file, having them as Inputs for the Job, and applies a MapReduce Implementation, writing to an output

  val moviesMapper = classOf[MoviesMapper]
  val moviesRatings = classOf[MovieRatingMapper]
  val reducer = classOf[MovieRatingReducer]

  val context = EngineContext
  val pathMovies = Paths.get("../test-files/movies.txt")
  val pathRatings = Paths.get("../test-files/ratings.txt")
  val j = context.newJob("avg-movie-ratings")
  j.addMapper(pathMovies, moviesMapper)
  j.addMapper(pathRatings, moviesRatings)
  j.addReducer(reducer)
  j.addOutput(util.testkit.InMemoryDataSetOutput())
  j.withTimeout(10 seconds)

  "Reduce Side Join " should " work " in {
    val fResult = context.submit(j)

  }

}


class MoviesMapper extends Mapper[None.type, String, String, String] {
  override def map(k: None.type, v: String): Iterable[(String, String)] = {
    val key = v.substring(v.indexOf(":"), v.indexOf(",")).trim
    val value = s"movie\t${v.substring(v.lastIndexOf(":") + 1, v.lastIndexOf("}")).trim}"
    Seq(key -> value)
  }
}

class MovieRatingMapper extends Mapper[None.type, String, String, String] {
  override def map(k: None.type, v: String): Iterable[(String, String)] = {
    val key = v.substring(v.indexOf(":", v.indexOf("movie_id:")) + 1, v.lastIndexOf(",")).trim
    val value = s"rating\t${v.substring(v.lastIndexOf(":") + 1, v.lastIndexOf("}")).trim}"
    Seq(key -> value)
  }
}

class MovieRatingReducer extends Reducer[String, String, (Int, String)] {
  override def reduce(key: String, list: Iterable[String]): (Int, String) = {
    val taggedMovieName = list.filter(_.startsWith("movie")).head
    val movieName = taggedMovieName.substring(taggedMovieName.indexOf("\t") + 1)
    val rantings = list.filter(_.startsWith("rating"))
    val count = rantings.size
    val avgRating = rantings.map(r => r.substring(r.indexOf("rating\t") + 1).toDouble).sum / count
    key.toInt -> s"{movieId: $key, movieName: $movieName, avgRating: $avgRating}"
  }
}




