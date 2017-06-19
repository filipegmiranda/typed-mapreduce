package engine.api

/**
  * This represents a MapReduce Interface, that maps a String to a collection of Key Value Pairs (no key as input, only value)
  */
trait MapReduceFileLineLine[KOUT, VOUT, RKIN, RVIN, R] extends MapReduce[None.type , String, KOUT, VOUT, RKIN, RVIN, R] {
  override final def map(k: None.type, v: String): Iterable[(KOUT, VOUT)] = map(v)

  def map(line: String): Iterable[(KOUT, VOUT)]
}
