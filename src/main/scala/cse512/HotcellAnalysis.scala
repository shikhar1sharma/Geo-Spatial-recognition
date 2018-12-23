package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)


  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    //pickupInfo.show()
    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.createOrReplaceTempView("PickUpLocation")

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    val attr = spark.sql("select x, y, z, count(*) as attr from PickUpLocation group by x,y,z").persist()

    attr.createOrReplaceTempView("AttributeList")

    val summations = spark.sql("select sum(attr),sum(attr*attr) from AttributeList").first()
    val xBar: Double = summations.get(0).toString.toDouble / numCells
    val sValue: Double = math.sqrt((summations.get(1).toString.toDouble / numCells) - (xBar * xBar))


    spark.udf.register("validNeighbour", (x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int) => {
      if (x2 > maxX || x2 < minX || y2 < minY || y2 > maxY || z2 > maxZ || z2 < minZ) false
      else if ((x1 - 1 == x2 || x1 == x2 || x1 + 1 == x2) && (y1 - 1 == y2 || y1 == y2 || y2 == y1 + 1)
        && (z1 == z2 || z2 == z1 - 1 || z2 == z1 + 1)) true
      else false
    })

    def nCount(x: Long, y: Long, z: Long): Int = {
      var ans = 0
      for (i <- x - 1 to x + 1) {
        for (j <- y - 1 to y + 1) {
          for (k <- z - 1 to z + 1) {
            if (i >= minX && i <= maxX
              && j >= minY && j <= maxY
              && k >= minZ && k <= maxZ) {
              ans += 1
            }
          }
        }
      }
      ans
    }

    spark.udf.register("countValidNeighbourCount", (x: Long, y: Long, z: Long) => nCount(x, y, z))
    val neighBourAttr = spark.sql("select first.x as x, first.y as y, first.z as z, sum(second.attr) as nrcount, countValidNeighbourCount(first.x,first.y,first.z) as ncount from AttributeList first cross join AttributeList second where validNeighbour(first.x,first.y,first.z,second.x,second.y,second.z) group by first.x,first.y,first.z").persist()
    neighBourAttr.createOrReplaceTempView("NeighboursWith")


    def giveGetisOrd(nrCount: Long, ncount: Long): Double = {
      var numerator: Double = nrCount.toDouble - xBar.toDouble * ncount
      var partDenom: Double = math.sqrt(((numCells.toDouble * ncount) - (ncount.toDouble * ncount)) / (numCells.toDouble - 1)) * sValue
      numerator / partDenom
    }

    spark.udf.register("getisord", (nrcount: Long, ncount: Long) => giveGetisOrd(nrcount, ncount))
    val getisOrd = spark.sql("select x, y, z, getisord(nrcount, ncount) as zscore from NeighboursWith order by zscore desc").persist()
    getisOrd.createOrReplaceTempView("finaldata")
    val finalData = spark.sql("select x,y,z from finaldata")
    finalData
  }
}
