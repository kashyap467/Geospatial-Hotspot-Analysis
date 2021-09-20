package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  val totalCellCount = numCells.longValue()
  pickupInfo.createOrReplaceTempView("pickupInfo")

  // Filter out cells that aren't within given boundaries
  val boundaryCondition = "x >= " + minX + " and x <= " + maxX + " and y >= " + minY + " and y <= " + maxY + " and z >= " + minZ + " and z <= " + maxZ
  val boundedPickupInfo = spark.sql("select x, y, z from pickupInfo where " + boundaryCondition)
  boundedPickupInfo.createOrReplaceTempView("boundedPickupInfo")

  // Get pickup count of each cell
  val pickupsPerCell = spark.sql("select x, y, z, count(*) as xj from boundedPickupInfo group by x, y, z order by count(x) desc")
  pickupsPerCell.createOrReplaceTempView("pickupsPerCell")

  // Get neighbor count (wj) and total pickup count in neighbourhood (xj) of each cell
  val xCondition = "(p1.x = p2.x or p1.x = p2.x + 1 or p1.x = p2.x - 1)"
  val yCondition = "(p1.y = p2.y or p1.y = p2.y + 1 or p1.y = p2.y - 1)"
  val zCondition = "(p1.z = p2.z or p1.z = p2.z + 1 or p1.z = p2.z - 1)"
  val xj_wj_val = spark.sql("select p1.x, p1.y, p1.z, sum(p2.xj) as total_xj_sum, count(p2.x) as total_wj_sum from pickupsPerCell p1, pickupsPerCell p2 where " + xCondition + " and " + yCondition + " and " + zCondition + " group by p1.x, p1.y, p1.z, p1.xj order by total_xj_sum desc")
  xj_wj_val.createOrReplaceTempView("xj_wj_val")

  // Get Mean and Standard Deviation values using xj of all cells
  val mean = "sum(xj)/" + totalCellCount
  val stDv = "sqrt(sum(xj*xj)/" + totalCellCount + " - ((" + mean +")*" + mean + "))"
  val mean_std_val = spark.sql("select " + mean + " as meanX, count(xj), " + stDv + " as stdDev from pickupsPerCell")
  mean_std_val.createOrReplaceTempView("mean_std_val")

  // Calculate numerator value in G-Score for each cell
  val prod_mean_wj = "(select meanX from mean_std_val)*total_wj_sum"
  val numerator = spark.sql("select x,y,z, total_xj_sum - " + prod_mean_wj + " as numerVal from xj_wj_val")
  numerator.createOrReplaceTempView("numerator")

  // Calculate denominator value in G-Score for each cell
  val std = "(select stdDev from mean_std_val)"
  val prod_n_wjSqr = "(" + totalCellCount + " * total_wj_sum)"
  val wj_wholeSqr = "(total_wj_sum * total_wj_sum)"
  val n_minus_1 = "(" + totalCellCount + "-1)"
  val denominator = spark.sql("select x,y,z," + std + " * sqrt((" + prod_n_wjSqr + " - " + wj_wholeSqr + ") / " + n_minus_1 + ") as denomVal from xj_wj_val")
  denominator.createOrReplaceTempView("denominator")

  // Calculate G-Score for all cells and Return top 50 hot cells based on G-Score
  val sameCellCondition = "n.x = d.x and n.y = d.y and n.z = d.z"
  val g_score = "n.numerVal/d.denomVal"
  val hotCells = spark.sql("select n.x,n.y,n.z from numerator n, denominator d where " + sameCellCondition + " order by " + g_score + " desc limit 50")
  var newHeader = Seq("x", "y", "z")
  val hotCellsDf = hotCells.toDF(newHeader:_*)

  return hotCellsDf.coalesce(1)
}
}
