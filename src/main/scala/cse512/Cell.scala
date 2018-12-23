package cse512

/**
  * Created by shanky on 11/20/17.
  */
class Cell(x: Int, y: Int, z: Int) extends Serializable {
  private val coordinateStep = 0.01
  private val minX = -74.50 / coordinateStep
  private val maxX = -73.70 / coordinateStep
  private val minY = 40.50 / coordinateStep
  private val maxY = 40.90 / coordinateStep
  private val minZ = 1
  private val maxZ = 31

  def getX: Int = this.x

  def getY: Int = this.y

  def getZ: Int = this.z

  private var neighbours = 0


  def setNeighbours(): Unit = {
    for (i <- this.x - 1 to this.x + 1) {
      for (j <- this.y - 1 to this.y + 1) {
        for (k <- this.z - 1 to this.z + 1) {
          if (i >= minX && i <= maxX
            && j >= minY && j <= maxY
            && k >= minZ && k <= maxZ) {
            this.neighbours += 1
          }
        }
      }
    }
  }

  def getNeighbours: Int = this.neighbours

  override def toString: String = this.x + "," + this.y + "," + this.z

  override def equals(obj: scala.Any): Boolean = {
    val that = obj.asInstanceOf[Cell]
    this.x == that.getX && this.y == that.getY && this.z == that.getZ
  }

  override def hashCode: Int = {
    val hashStr = this.x + "," + this.y + "," + this.z
    hashStr.hashCode
  }

}
