package util.testkit

object TestUtils {

  def measure(block: => Unit): Long = {
    val initial = System.currentTimeMillis()
    block
    System.currentTimeMillis() - initial
  }

}
