package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val rectangePoints = queryRectangle.split(",").map(_.toDouble)
    val minX = math.min(rectangePoints(0), rectangePoints(2))
    val maxX = math.max(rectangePoints(0), rectangePoints(2))
    val minY = math.min(rectangePoints(1), rectangePoints(3))
    val maxY = math.max(rectangePoints(1), rectangePoints(3))
    val point = pointString.split(",").map(_.toDouble)
    if ((point(0) >= minX && point(0) <= maxX) && (point(1) >= minY && point(1) <= maxY))
      true
    else
      false
  }

  // YOU NEED TO CHANGE THIS PART

}
