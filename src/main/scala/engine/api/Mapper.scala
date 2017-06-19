package engine.api

/**
  * Mapper trait where a Map operation may be defined
  *
  * @tparam VIN the input value
  * @tparam KOUT the output key after the map transformation
  * @tparam VOUT the output value after the map transformation
  */
trait Mapper[KIN, VIN, KOUT, VOUT] {
  type K = KIN
  type V = VIN

  final def runMap(k: Any, v: Any): Iterable[(KOUT, VOUT)] = {
    map(k.asInstanceOf[K], v.asInstanceOf[V])
  }

  def map(k: KIN, v: VIN): Iterable[(KOUT, VOUT)]
}

/**
  * reducer trait for defining reducers
  *
  * @tparam K the input key of the pair
  * @tparam V the input value of the pair
  * @tparam R the result of the the reduce operation
  */
trait Reducer[K,V,R] {
  type KIN = K
  type VIN = V
  type ROUT = R

  final def runReduce(k: Any, list: Iterable[Any]): ROUT = {
    reduce(k.asInstanceOf[K], list.asInstanceOf[Iterable[V]])
  }

  def reduce(key: K, list: Iterable[V]): R
}

/**
  * MapReduce class that abstracts the Mapper and Reducer in one single class, defined in this assignment to allow users
  * of the api to define simple Jobs with one Single Map Operation and one Reduce Operation over a dataset
  *
  * Perhaps it is even better doing with Spark, and its RDDs, or dataframes, rather than MapReduce, Spark API is also a lot more concise
  *
  */
abstract class MapReduce[KIN, VIN, KOUT, VOUT, RKIN, RVIN, R] extends Mapper[KIN, VIN, KOUT, VOUT] with Reducer[RKIN,RVIN, R]

object MapReduce {

  /**
    * Aliases to simplify type parameters
    */
  type MapReduceUpB = engine.api.Mapper[_,_,_,_] with engine.api.Reducer[_,_,_]
  type MapperT = engine.api.Mapper[_,_,_,_]
  type ReducerT = engine.api.Reducer[_,_,_]
}