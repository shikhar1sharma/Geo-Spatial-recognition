package cse512

/**
  * Created by shanky on 11/20/17.
  */
class OrdisOrdering extends Ordering[(Cell, Long)] with Serializable {
  override def compare(x: (Cell, Long), y: (Cell, Long)): Int = {
    x._2.compareTo(y._2)
  }
}
