package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    // Get rectangle co-ordinates x1, x2, y1, y2
    val rectangle = queryRectangle.split(",").map(_.toDouble)
    val rt_x1 = rectangle(0)
    val rt_y1 = rectangle(1)
    val rt_x2 = rectangle(2)
    val rt_y2 = rectangle(3)

    // Get point c-ordinates x, y
    val point = pointString.split(",").map(_.toDouble)
    val pt_x = point(0)
    val pt_y = point(1)

    var min_x: Double = 0
    var max_x: Double = 0
    var min_y: Double = 0
    var max_y: Double = 0

    // Get all four vertices of the rectangle
    if (rt_x1 < rt_x2) {
      min_x = rt_x1 ; max_x = rt_x2
    } else {
      min_x = rt_x2 ; max_x = rt_x1
    }

    if (rt_y1 < rt_y2) {
      min_y = rt_y1 ; max_y = rt_y2
    } else {
      min_y = rt_y2 ; max_y = rt_y1
    }

    // Check if point is lying outside the rectangle
    // if yes - return false, else - return true
    if (pt_x < min_x | pt_x > max_x | pt_y < min_y | pt_y > max_y) {
      return false
    }
    return true
  }

  // YOU NEED TO CHANGE THIS PART IF YOU WANT TO ADD ADDITIONAL METHODS

}
